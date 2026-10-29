#include "elf.h"

#include "common.h"

#include <algorithm>
#include <cstring>

#include <elf.h>

namespace NPchUpdate {

    namespace {

        constexpr size_t AR_HEADER_SIZE = 60;

        [[noreturn]] void Malformed(const std::string& path, const char* what) {
            Fail(Format("%s: malformed file: %s", path.c_str(), what));
        }

        bool IsElf(std::string_view data) {
            return data.size() >= SELFMAG && memcmp(data.data(), ELFMAG, SELFMAG) == 0;
        }

        bool IsArchive(std::string_view data) {
            return StartsWith(data, "!<arch>\n") || StartsWith(data, "!<thin>\n");
        }

        // Whether [offset, offset + size) lies in a range of `limit` bytes.
        bool InBounds(uint64_t offset, uint64_t size, uint64_t limit) {
            return offset <= limit && size <= limit - offset;
        }

        class TElfFile {
        public:
            TElfFile(std::string_view data, const std::string& path)
                : Data_(data)
                , Path_(path)
            {
                if (!IsElf(data) || data.size() < sizeof(Elf64_Ehdr)) {
                    Malformed(path, "not an ELF file");
                }
                memcpy(&Header_, data.data(), sizeof(Header_));
                if (Header_.e_ident[EI_CLASS] != ELFCLASS64 || Header_.e_ident[EI_DATA] != ELFDATA2LSB ||
                    Header_.e_machine != EM_X86_64)
                {
                    Fail(Format("%s: not an ELF64 little endian x86-64 file", path.c_str()));
                }
                if (Header_.e_shoff == 0) {
                    return;
                }
                if (Header_.e_shentsize != sizeof(Elf64_Shdr)) {
                    Malformed(path, "section header size");
                }
                SectionCount_ = Header_.e_shnum;
                if (SectionCount_ == 0) {
                    // Extended numbering: the count is in sh_size of section 0.
                    if (!InBounds(Header_.e_shoff, sizeof(Elf64_Shdr), data.size())) {
                        Malformed(path, "section headers out of bounds");
                    }
                    Elf64_Shdr first;
                    memcpy(&first, data.data() + Header_.e_shoff, sizeof(first));
                    SectionCount_ = first.sh_size;
                }
                if (SectionCount_ > data.size() / sizeof(Elf64_Shdr) ||
                    !InBounds(Header_.e_shoff, SectionCount_ * sizeof(Elf64_Shdr), data.size()))
                {
                    Malformed(path, "section headers out of bounds");
                }
            }

            uint16_t Type() const {
                return Header_.e_type;
            }

            const std::string& Path() const {
                return Path_;
            }

            size_t SectionCount() const {
                return SectionCount_;
            }

            Elf64_Shdr Section(size_t index) const {
                if (index >= SectionCount_) {
                    Malformed(Path_, "section index out of bounds");
                }
                Elf64_Shdr section;
                memcpy(&section, Data_.data() + Header_.e_shoff + index * sizeof(Elf64_Shdr), sizeof(section));
                return section;
            }

            std::string_view SectionData(const Elf64_Shdr& section) const {
                if (section.sh_type == SHT_NOBITS) {
                    return {};
                }
                if (!InBounds(section.sh_offset, section.sh_size, Data_.size())) {
                    Malformed(Path_, "section data out of bounds");
                }
                return Data_.substr(section.sh_offset, section.sh_size);
            }

            // The first section of the type.
            bool FindSection(uint32_t type, Elf64_Shdr* out) const {
                for (size_t i = 0; i < SectionCount_; ++i) {
                    Elf64_Shdr section = Section(i);
                    if (section.sh_type == type) {
                        *out = section;
                        return true;
                    }
                }
                return false;
            }

        private:
            std::string_view Data_;
            const std::string& Path_;
            Elf64_Ehdr Header_;
            size_t SectionCount_ = 0;
        };

        class TSymbolTable {
        public:
            TSymbolTable(const TElfFile& elf, const Elf64_Shdr& section)
                : Path_(elf.Path())
            {
                if (section.sh_entsize != sizeof(Elf64_Sym)) {
                    Malformed(Path_, "symbol table entry size");
                }
                Symbols_ = elf.SectionData(section);
                if (Symbols_.size() % sizeof(Elf64_Sym) != 0) {
                    Malformed(Path_, "symbol table size");
                }
                Elf64_Shdr strings = elf.Section(section.sh_link);
                if (strings.sh_type != SHT_STRTAB) {
                    Malformed(Path_, "symbol table without a string table");
                }
                Strings_ = elf.SectionData(strings);
            }

            size_t Size() const {
                return Symbols_.size() / sizeof(Elf64_Sym);
            }

            Elf64_Sym Symbol(size_t index) const {
                Elf64_Sym symbol;
                memcpy(&symbol, Symbols_.data() + index * sizeof(Elf64_Sym), sizeof(symbol));
                return symbol;
            }

            std::string_view Name(const Elf64_Sym& symbol) const {
                if (symbol.st_name >= Strings_.size()) {
                    Malformed(Path_, "symbol name out of bounds");
                }
                const char* begin = Strings_.data() + symbol.st_name;
                const void* end = memchr(begin, '\0', Strings_.size() - symbol.st_name);
                if (!end) {
                    Malformed(Path_, "unterminated symbol name");
                }
                return {begin, static_cast<size_t>(static_cast<const char*>(end) - begin)};
            }

        private:
            const std::string& Path_;
            std::string_view Symbols_;
            std::string_view Strings_;
        };

        bool IsDefinedGlobal(const Elf64_Sym& symbol) {
            if (symbol.st_shndx == SHN_UNDEF) {
                return false;
            }
            unsigned bind = ELF64_ST_BIND(symbol.st_info);
            unsigned type = ELF64_ST_TYPE(symbol.st_info);
            return (bind == STB_GLOBAL || bind == STB_WEAK || bind == STB_GNU_UNIQUE) &&
                type != STT_SECTION && type != STT_FILE;
        }

        // The defined globals of the symbol table of the type (SHT_SYMTAB, SHT_DYNSYM).
        void AddDefined(const TElfFile& elf, uint32_t type, std::vector<std::string_view>* out) {
            Elf64_Shdr section;
            if (!elf.FindSection(type, &section)) {
                return;
            }
            TSymbolTable table(elf, section);
            for (size_t i = 1; i < table.Size(); ++i) {
                Elf64_Sym symbol = table.Symbol(i);
                if (IsDefinedGlobal(symbol)) {
                    std::string_view name = table.Name(symbol);
                    if (!name.empty()) {
                        out->push_back(name);
                    }
                }
            }
        }

        std::vector<std::string> Unique(std::vector<std::string_view>& names) {
            std::sort(names.begin(), names.end());
            names.erase(std::unique(names.begin(), names.end()), names.end());
            return std::vector<std::string>(names.begin(), names.end());
        }

        uint64_t ReadBigEndian(const char* data, size_t size) {
            uint64_t value = 0;
            for (size_t i = 0; i < size; ++i) {
                value = (value << 8) | static_cast<unsigned char>(data[i]);
            }
            return value;
        }

        // The names of the symbol index of GNU ar: a count, the member offsets
        // and the names, all numbers big endian of `width` bytes.
        void AddIndex(std::string_view index, size_t width, const std::string& path, std::vector<std::string_view>* out) {
            if (index.size() < width) {
                Malformed(path, "archive symbol index");
            }
            uint64_t count = ReadBigEndian(index.data(), width);
            if (count > (index.size() - width) / width) {
                Malformed(path, "archive symbol index count");
            }
            std::string_view names = index.substr(width + count * width);
            out->reserve(out->size() + count);
            for (uint64_t i = 0; i < count; ++i) {
                size_t end = names.find('\0');
                if (end == std::string_view::npos) {
                    Malformed(path, "archive symbol index names");
                }
                if (end) {
                    out->push_back(names.substr(0, end));
                }
                names.remove_prefix(end + 1);
            }
        }

        uint64_t ParseDecimal(std::string_view field, const std::string& path) {
            uint64_t value = 0;
            size_t i = 0;
            while (i < field.size() && field[i] == ' ') {
                ++i;
            }
            size_t digits = 0;
            for (; i < field.size() && field[i] >= '0' && field[i] <= '9'; ++i) {
                value = value * 10 + (field[i] - '0');
                ++digits;
            }
            for (; i < field.size(); ++i) {
                if (field[i] != ' ') {
                    Malformed(path, "archive member header number");
                }
            }
            if (!digits) {
                Malformed(path, "archive member header number");
            }
            return value;
        }

        // The name of a member: "name/" or "/<offset of the long name table>".
        std::string_view MemberName(std::string_view field, std::string_view longNames, const std::string& path) {
            while (!field.empty() && field.back() == ' ') {
                field.remove_suffix(1);
            }
            if (field.size() > 1 && field[0] == '/' && field[1] >= '0' && field[1] <= '9') {
                uint64_t offset = ParseDecimal(field.substr(1), path);
                if (offset >= longNames.size()) {
                    Malformed(path, "archive long member name");
                }
                std::string_view name = longNames.substr(offset);
                size_t end = name.find('\n');
                if (end == std::string_view::npos) {
                    Malformed(path, "archive long member name");
                }
                field = name.substr(0, end);
            }
            if (!field.empty() && field.back() == '/') {
                field.remove_suffix(1);
            }
            return field;
        }

        std::vector<std::string> ArchiveDefined(std::string_view data, const std::string& path) {
            bool thin = StartsWith(data, "!<thin>\n");
            std::vector<std::string_view> names;
            // Of members out of a thin archive: the names are kept by `external`.
            std::vector<std::vector<std::string>> external;
            std::string_view longNames;
            size_t pos = 8;
            while (pos < data.size()) {
                if (data.size() - pos < AR_HEADER_SIZE) {
                    Malformed(path, "truncated archive member header");
                }
                std::string_view header = data.substr(pos, AR_HEADER_SIZE);
                if (header.substr(58, 2) != "`\n") {
                    Malformed(path, "archive member header magic");
                }
                std::string_view rawName = header.substr(0, 16);
                while (!rawName.empty() && rawName.back() == ' ') {
                    rawName.remove_suffix(1);
                }
                uint64_t size = ParseDecimal(header.substr(48, 10), path);
                pos += AR_HEADER_SIZE;
                bool special = rawName == "/" || rawName == "/SYM64/" || rawName == "//";
                std::string_view member;
                if (!thin || special) {
                    if (!InBounds(pos, size, data.size())) {
                        Malformed(path, "archive member out of bounds");
                    }
                    member = data.substr(pos, size);
                    pos += size + (size & 1);
                }
                if (rawName == "/") {
                    AddIndex(member, 4, path, &names);
                    return Unique(names);
                }
                if (rawName == "/SYM64/") {
                    AddIndex(member, 8, path, &names);
                    return Unique(names);
                }
                if (rawName == "//") {
                    longNames = member;
                    continue;
                }
                if (thin) {
                    std::string memberPath = JoinPath(DirName(path), MemberName(rawName, longNames, path));
                    TMappedFile file(memberPath);
                    if (IsElf(file.Data())) {
                        std::vector<std::string_view> own;
                        AddDefined(TElfFile(file.Data(), memberPath), SHT_SYMTAB, &own);
                        external.emplace_back(own.begin(), own.end());
                    }
                } else if (IsElf(member)) {
                    AddDefined(TElfFile(member, path), SHT_SYMTAB, &names);
                }
            }
            for (const auto& list : external) {
                names.insert(names.end(), list.begin(), list.end());
            }
            return Unique(names);
        }

    } // namespace

    TSharedObjectSymbols ReadSharedObjectSymbols(const std::string& path) {
        TMappedFile file(path);
        TElfFile elf(file.Data(), path);
        TSharedObjectSymbols result;
        Elf64_Shdr section;
        if (!elf.FindSection(SHT_DYNSYM, &section)) {
            return result;
        }
        TSymbolTable table(elf, section);
        std::string_view versions;
        Elf64_Shdr versionSection;
        if (elf.FindSection(SHT_GNU_versym, &versionSection)) {
            versions = elf.SectionData(versionSection);
            if (versions.size() / sizeof(Elf64_Half) < table.Size()) {
                Malformed(path, ".gnu.version shorter than .dynsym");
            }
        }
        std::vector<std::string_view> defined;
        std::vector<std::string_view> unresolved;
        for (size_t i = 1; i < table.Size(); ++i) {
            Elf64_Sym symbol = table.Symbol(i);
            if (IsDefinedGlobal(symbol)) {
                std::string_view name = table.Name(symbol);
                if (!name.empty()) {
                    defined.push_back(name);
                }
                continue;
            }
            if (symbol.st_shndx != SHN_UNDEF || ELF64_ST_BIND(symbol.st_info) != STB_GLOBAL) {
                continue;
            }
            if (!versions.empty()) {
                Elf64_Half version;
                memcpy(&version, versions.data() + i * sizeof(Elf64_Half), sizeof(version));
                // 0 (local, as GNU ld writes) or 1 (global): no version
                // needed of a library, so nothing on the link line defined it.
                if ((version & 0x7fff) > VER_NDX_GLOBAL) {
                    continue;
                }
            }
            std::string_view name = table.Name(symbol);
            if (!name.empty()) {
                unresolved.push_back(name);
            }
        }
        result.Defined = Unique(defined);
        result.Unresolved = Unique(unresolved);
        return result;
    }

    std::vector<std::string> ReadArchiveDefined(const std::string& path) {
        TMappedFile file(path);
        if (!IsArchive(file.Data())) {
            Malformed(path, "not an archive");
        }
        return ArchiveDefined(file.Data(), path);
    }

    std::vector<std::string> ReadObjectDefined(const std::string& path) {
        TMappedFile file(path);
        std::vector<std::string_view> names;
        AddDefined(TElfFile(file.Data(), path), SHT_SYMTAB, &names);
        return Unique(names);
    }

    std::vector<std::string> ReadDefinedSymbols(const std::string& path) {
        TMappedFile file(path);
        std::string_view data = file.Data();
        if (IsArchive(data)) {
            return ArchiveDefined(data, path);
        }
        if (!IsElf(data)) {
            Fail(Format("%s: neither an archive nor an ELF file", path.c_str()));
        }
        TElfFile elf(data, path);
        std::vector<std::string_view> names;
        if (elf.Type() == ET_DYN) {
            AddDefined(elf, SHT_DYNSYM, &names);
        } else if (elf.Type() == ET_REL) {
            AddDefined(elf, SHT_SYMTAB, &names);
        } else {
            Fail(Format("%s: an ELF file neither a shared object nor a relocatable object", path.c_str()));
        }
        return Unique(names);
    }

} // namespace NPchUpdate
