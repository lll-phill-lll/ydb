#include "dia.h"

#include "common.h"

#include <algorithm>
#include <map>
#include <unordered_map>

namespace NPchUpdate {

    namespace {

        // LLVM bitstream.
        constexpr uint64_t END_BLOCK = 0;
        constexpr uint64_t ENTER_SUBBLOCK = 1;
        constexpr uint64_t DEFINE_ABBREV = 2;
        constexpr uint64_t UNABBREV_RECORD = 3;
        constexpr uint64_t FIRST_ABBREV = 4;

        constexpr uint64_t BLOCKINFO_BLOCK = 0;
        constexpr uint64_t BLOCKINFO_SETBID = 1;

        // clang/Frontend/SerializedDiagnostics.h
        constexpr uint64_t BLOCK_DIAG = 9;
        constexpr uint64_t RECORD_DIAG = 2;
        constexpr uint64_t RECORD_DIAG_FLAG = 4;
        constexpr uint64_t RECORD_CATEGORY = 5;
        constexpr uint64_t RECORD_FILENAME = 6;

        struct TAbbrevOp {
            enum EKind {
                Literal,
                Fixed,
                Vbr,
                Array,
                Char6,
                Blob,
            };

            EKind Kind = Literal;
            // Literal: the value; Fixed, Vbr: the width.
            uint64_t Value = 0;
        };

        using TAbbrev = std::vector<TAbbrevOp>;

        struct TRecord {
            uint64_t Code = 0;
            std::vector<uint64_t> Values;
            bool HasBlob = false;
            std::string_view Blob;

            // A string field at `index` and on: the blob, else one value per char.
            std::string Text(size_t index) const {
                if (HasBlob) {
                    return std::string(Blob);
                }
                std::string text;
                for (size_t i = index; i < Values.size(); ++i) {
                    text.push_back(static_cast<char>(Values[i]));
                }
                return text;
            }

            uint64_t Value(size_t index) const {
                return index < Values.size() ? Values[index] : 0;
            }
        };

        struct TBlockScope {
            uint64_t Id = 0;
            unsigned Width = 2;
            std::vector<TAbbrev> Abbrevs;
            // BLOCKINFO only: the block SETBID named, -1 before one.
            int64_t InfoTarget = -1;
        };

        class TBitReader {
        public:
            explicit TBitReader(std::string_view data)
                : Data_(data)
                , Size_(static_cast<uint64_t>(data.size()) * 8)
            {
            }

            bool AtEnd() const {
                return Pos_ >= Size_;
            }

            uint64_t Read(unsigned width) {
                if (width > 64) {
                    Fail(Format("serialized diagnostics: field width %u", width));
                }
                if (width > Size_ - Pos_) {
                    Fail("serialized diagnostics: truncated");
                }
                uint64_t value = 0;
                unsigned done = 0;
                while (done < width) {
                    unsigned offset = Pos_ & 7;
                    unsigned take = std::min(8 - offset, width - done);
                    uint64_t byte = static_cast<unsigned char>(Data_[Pos_ >> 3]);
                    value |= ((byte >> offset) & ((1u << take) - 1)) << done;
                    done += take;
                    Pos_ += take;
                }
                return value;
            }

            uint64_t ReadVbr(unsigned width) {
                if (width < 2 || width > 32) {
                    Fail(Format("serialized diagnostics: VBR width %u", width));
                }
                uint64_t high = uint64_t(1) << (width - 1);
                uint64_t value = 0;
                for (unsigned shift = 0;; shift += width - 1) {
                    if (shift >= 64) {
                        Fail("serialized diagnostics: VBR value too long");
                    }
                    uint64_t chunk = Read(width);
                    value |= (chunk & (high - 1)) << shift;
                    if (!(chunk & high)) {
                        return value;
                    }
                }
            }

            void Align32() {
                uint64_t aligned = (Pos_ + 31) & ~uint64_t(31);
                Pos_ = std::min(aligned, Size_);
            }

            void SkipWords(uint64_t words) {
                if (words > (Size_ - Pos_) / 32) {
                    Fail("serialized diagnostics: block out of bounds");
                }
                Pos_ += words * 32;
            }

            // At a byte boundary.
            std::string_view Bytes(uint64_t size) {
                if (size > (Size_ - Pos_) / 8) {
                    Fail("serialized diagnostics: blob out of bounds");
                }
                std::string_view bytes = Data_.substr(Pos_ / 8, size);
                Pos_ += size * 8;
                return bytes;
            }

        private:
            std::string_view Data_;
            uint64_t Size_;
            uint64_t Pos_ = 0;
        };

        char DecodeChar6(uint64_t value) {
            static const char ALPHABET[] = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789._";
            return ALPHABET[value & 63];
        }

        class TDiaReader {
        public:
            explicit TDiaReader(std::string_view data)
                : Bits_(data)
            {
            }

            std::vector<TDiagnostic> Read() {
                std::vector<TDiagnostic> result;
                TBlockScope top;
                while (!Bits_.AtEnd()) {
                    if (Bits_.Read(top.Width) != ENTER_SUBBLOCK) {
                        Fail("serialized diagnostics: no block at the top level");
                    }
                    uint64_t id = Bits_.ReadVbr(8);
                    if (id == BLOCKINFO_BLOCK) {
                        TBlockScope scope = EnterBlock(id);
                        ReadBlockInfo(scope);
                    } else if (id == BLOCK_DIAG) {
                        TBlockScope scope = EnterBlock(id);
                        result.emplace_back();
                        ReadDiag(scope, &result.back());
                    } else {
                        SkipBlock();
                    }
                }
                return result;
            }

        private:
            enum class EEntry {
                EndBlock,
                SubBlock,
                Record,
            };

            // The next entry of the block but abbrev definitions; for SubBlock
            // `id` is the block id read, for Record `record` is filled.
            EEntry Next(TBlockScope& scope, uint64_t* id, TRecord* record) {
                while (true) {
                    uint64_t abbrev = Bits_.Read(scope.Width);
                    if (abbrev == END_BLOCK) {
                        Bits_.Align32();
                        return EEntry::EndBlock;
                    }
                    if (abbrev == ENTER_SUBBLOCK) {
                        *id = Bits_.ReadVbr(8);
                        return EEntry::SubBlock;
                    }
                    if (abbrev == DEFINE_ABBREV) {
                        TAbbrev definition = ReadAbbrev();
                        if (scope.Id == BLOCKINFO_BLOCK) {
                            if (scope.InfoTarget < 0) {
                                Fail("serialized diagnostics: abbrev in BLOCKINFO before SETBID");
                            }
                            BlockInfo_[scope.InfoTarget].push_back(std::move(definition));
                        } else {
                            scope.Abbrevs.push_back(std::move(definition));
                        }
                        continue;
                    }
                    record->Values.clear();
                    record->HasBlob = false;
                    record->Blob = {};
                    if (abbrev == UNABBREV_RECORD) {
                        record->Code = Bits_.ReadVbr(6);
                        uint64_t count = Bits_.ReadVbr(6);
                        for (uint64_t i = 0; i < count; ++i) {
                            record->Values.push_back(Bits_.ReadVbr(6));
                        }
                        return EEntry::Record;
                    }
                    if (abbrev - FIRST_ABBREV >= scope.Abbrevs.size()) {
                        Fail(Format("serialized diagnostics: unknown abbrev %llu", static_cast<unsigned long long>(abbrev)));
                    }
                    ReadAbbreviated(scope.Abbrevs[abbrev - FIRST_ABBREV], record);
                    return EEntry::Record;
                }
            }

            TAbbrev ReadAbbrev() {
                TAbbrev abbrev;
                uint64_t count = Bits_.ReadVbr(5);
                for (uint64_t i = 0; i < count; ++i) {
                    TAbbrevOp op;
                    if (Bits_.Read(1)) {
                        op.Kind = TAbbrevOp::Literal;
                        op.Value = Bits_.ReadVbr(8);
                    } else {
                        uint64_t encoding = Bits_.Read(3);
                        switch (encoding) {
                            case 1:
                                op.Kind = TAbbrevOp::Fixed;
                                op.Value = Bits_.ReadVbr(5);
                                break;
                            case 2:
                                op.Kind = TAbbrevOp::Vbr;
                                op.Value = Bits_.ReadVbr(5);
                                break;
                            case 3:
                                op.Kind = TAbbrevOp::Array;
                                break;
                            case 4:
                                op.Kind = TAbbrevOp::Char6;
                                break;
                            case 5:
                                op.Kind = TAbbrevOp::Blob;
                                break;
                            default:
                                Fail(Format("serialized diagnostics: abbrev encoding %llu", static_cast<unsigned long long>(encoding)));
                        }
                        if ((op.Kind == TAbbrevOp::Fixed && op.Value > 64) ||
                            (op.Kind == TAbbrevOp::Vbr && op.Value > 32))
                        {
                            Fail("serialized diagnostics: abbrev operand width");
                        }
                    }
                    abbrev.push_back(op);
                }
                return abbrev;
            }

            uint64_t ReadScalar(const TAbbrevOp& op) {
                switch (op.Kind) {
                    case TAbbrevOp::Literal:
                        return op.Value;
                    case TAbbrevOp::Fixed:
                        return Bits_.Read(op.Value);
                    case TAbbrevOp::Vbr:
                        return op.Value ? Bits_.ReadVbr(op.Value) : 0;
                    case TAbbrevOp::Char6:
                        return static_cast<unsigned char>(DecodeChar6(Bits_.Read(6)));
                    default:
                        Fail("serialized diagnostics: array or blob as a scalar");
                }
            }

            void ReadAbbreviated(const TAbbrev& abbrev, TRecord* record) {
                if (abbrev.empty()) {
                    Fail("serialized diagnostics: empty abbrev");
                }
                record->Code = ReadScalar(abbrev[0]);
                for (size_t i = 1; i < abbrev.size(); ++i) {
                    const TAbbrevOp& op = abbrev[i];
                    if (op.Kind == TAbbrevOp::Array) {
                        if (i + 2 != abbrev.size()) {
                            Fail("serialized diagnostics: array not last but one in an abbrev");
                        }
                        uint64_t count = Bits_.ReadVbr(6);
                        for (uint64_t j = 0; j < count; ++j) {
                            record->Values.push_back(ReadScalar(abbrev[i + 1]));
                        }
                        return;
                    }
                    if (op.Kind == TAbbrevOp::Blob) {
                        uint64_t size = Bits_.ReadVbr(6);
                        Bits_.Align32();
                        record->HasBlob = true;
                        record->Blob = Bits_.Bytes(size);
                        Bits_.Align32();
                        continue;
                    }
                    record->Values.push_back(ReadScalar(op));
                }
            }

            // After ENTER_SUBBLOCK and the block id.
            TBlockScope EnterBlock(uint64_t id) {
                TBlockScope scope;
                scope.Id = id;
                scope.Width = Bits_.ReadVbr(4);
                if (scope.Width < 1 || scope.Width > 32) {
                    Fail(Format("serialized diagnostics: abbrev width %u", scope.Width));
                }
                Bits_.Align32();
                Bits_.Read(32);
                auto info = BlockInfo_.find(id);
                if (info != BlockInfo_.end()) {
                    scope.Abbrevs = info->second;
                }
                return scope;
            }

            void SkipBlock() {
                Bits_.ReadVbr(4);
                Bits_.Align32();
                Bits_.SkipWords(Bits_.Read(32));
            }

            void ReadBlockInfo(TBlockScope& scope) {
                TRecord record;
                uint64_t id = 0;
                while (true) {
                    EEntry entry = Next(scope, &id, &record);
                    if (entry == EEntry::EndBlock) {
                        return;
                    }
                    if (entry == EEntry::SubBlock) {
                        SkipBlock();
                    } else if (record.Code == BLOCKINFO_SETBID) {
                        if (record.Values.empty()) {
                            Fail("serialized diagnostics: SETBID without a block id");
                        }
                        scope.InfoTarget = record.Values[0];
                    }
                }
            }

            // Notes are DIAG blocks in the DIAG block of their diagnostic.
            void ReadDiag(TBlockScope& scope, TDiagnostic* diag) {
                TRecord record;
                uint64_t id = 0;
                while (true) {
                    EEntry entry = Next(scope, &id, &record);
                    if (entry == EEntry::EndBlock) {
                        return;
                    }
                    if (entry == EEntry::SubBlock) {
                        if (id == BLOCK_DIAG) {
                            TBlockScope inner = EnterBlock(id);
                            diag->Notes.emplace_back();
                            ReadDiag(inner, &diag->Notes.back());
                        } else {
                            SkipBlock();
                        }
                        continue;
                    }
                    switch (record.Code) {
                        case RECORD_DIAG:
                            // Level, file, line, column, offset, category, flag, size, message.
                            diag->Level = static_cast<int>(record.Value(0));
                            diag->File = Lookup(Files_, record.Value(1), "file");
                            diag->Line = static_cast<uint32_t>(record.Value(2));
                            diag->Column = static_cast<uint32_t>(record.Value(3));
                            diag->Category = Lookup(Categories_, record.Value(5), "category");
                            diag->Flag = Lookup(Flags_, record.Value(6), "flag");
                            diag->Message = record.Text(8);
                            break;
                        case RECORD_FILENAME:
                            // Id, size, modification time, name size, name.
                            Files_[record.Value(0)] = record.Text(4);
                            break;
                        case RECORD_CATEGORY:
                            // Id, name size, name.
                            Categories_[record.Value(0)] = record.Text(2);
                            break;
                        case RECORD_DIAG_FLAG:
                            // Id, name size, name.
                            Flags_[record.Value(0)] = record.Text(2);
                            break;
                        default:
                            break;
                    }
                }
            }

            // Id 0 is none; the others are defined before their first use.
            static std::string Lookup(const std::unordered_map<uint64_t, std::string>& table, uint64_t id, const char* what) {
                if (!id) {
                    return {};
                }
                auto it = table.find(id);
                if (it == table.end()) {
                    Fail(Format("serialized diagnostics: unknown %s id %llu", what, static_cast<unsigned long long>(id)));
                }
                return it->second;
            }

            TBitReader Bits_;
            std::map<uint64_t, std::vector<TAbbrev>> BlockInfo_;
            std::unordered_map<uint64_t, std::string> Files_;
            std::unordered_map<uint64_t, std::string> Categories_;
            std::unordered_map<uint64_t, std::string> Flags_;
        };

    } // namespace

    std::vector<TDiagnostic> ReadSerializedDiagnostics(std::string_view data) {
        if (!StartsWith(data, "DIAG")) {
            Fail("serialized diagnostics: no DIAG magic");
        }
        return TDiaReader(data.substr(4)).Read();
    }

} // namespace NPchUpdate
