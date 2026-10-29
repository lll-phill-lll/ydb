#include "trace.h"

#include <rapidjson/error/en.h>
#include <rapidjson/memorystream.h>
#include <rapidjson/reader.h>

#include <algorithm>
#include <cmath>
#include <limits>
#include <tuple>
#include <unordered_map>

namespace NPchUpdate {

    namespace {
        constexpr uint64_t TRACE_SUMMARY_FORMAT = 1;

        // The depths of the trace: { "traceEvents": [ { ..., "args": { "detail": ... } } ] }.
        constexpr int ROOT_DEPTH = 1;
        constexpr int EVENTS_DEPTH = 2;
        constexpr int EVENT_DEPTH = 3;
        constexpr int ARGS_DEPTH = 4;

        // A Source parsed: a header, from `Ts` for `Dur`.
        struct TOccurrence {
            uint32_t Header = 0;
            int64_t Ts = 0;
            int64_t Dur = 0;
        };

        // The events of a trace as load_trace() of build_time_report.py has them.
        class TTraceHandler: public rapidjson::BaseReaderHandler<rapidjson::UTF8<>, TTraceHandler> {
        public:
            bool StartObject() {
                ++Depth_;
                if (Depth_ == EVENT_DEPTH && InEvents_) {
                    Event_.Ph.clear();
                    Event_.Name.clear();
                    Event_.Detail.clear();
                    Event_.Ts = 0;
                    Event_.Dur = 0;
                    InEvent_ = true;
                } else if (Depth_ == ARGS_DEPTH && InEvent_ && Key_ == EKey::Args) {
                    InArgs_ = true;
                }
                return true;
            }

            bool EndObject(rapidjson::SizeType) {
                if (Depth_ == EVENT_DEPTH && InEvent_) {
                    AddEvent();
                    InEvent_ = false;
                } else if (Depth_ == ARGS_DEPTH) {
                    InArgs_ = false;
                }
                --Depth_;
                Key_ = EKey::None;
                return true;
            }

            bool StartArray() {
                ++Depth_;
                if (Depth_ == EVENTS_DEPTH && Key_ == EKey::TraceEvents) {
                    // As json.load: the last "traceEvents" wins.
                    Reset();
                    Found = true;
                    InEvents_ = true;
                }
                return true;
            }

            bool EndArray(rapidjson::SizeType) {
                if (Depth_ == EVENTS_DEPTH) {
                    InEvents_ = false;
                }
                --Depth_;
                Key_ = EKey::None;
                return true;
            }

            bool Key(const char* str, rapidjson::SizeType length, bool) {
                std::string_view key(str, length);
                Key_ = EKey::None;
                if (Depth_ == ROOT_DEPTH) {
                    if (key == "traceEvents") {
                        Key_ = EKey::TraceEvents;
                    }
                } else if (Depth_ == EVENT_DEPTH && InEvent_) {
                    if (key == "ph") {
                        Key_ = EKey::Ph;
                    } else if (key == "name") {
                        Key_ = EKey::Name;
                    } else if (key == "ts") {
                        Key_ = EKey::Ts;
                    } else if (key == "dur") {
                        Key_ = EKey::Dur;
                    } else if (key == "args") {
                        Key_ = EKey::Args;
                    }
                } else if (Depth_ == ARGS_DEPTH && InArgs_) {
                    if (key == "detail") {
                        Key_ = EKey::Detail;
                    }
                }
                return true;
            }

            bool String(const char* str, rapidjson::SizeType length, bool) {
                if (Depth_ == EVENT_DEPTH && InEvent_) {
                    if (Key_ == EKey::Ph) {
                        Event_.Ph.assign(str, length);
                    } else if (Key_ == EKey::Name) {
                        Event_.Name.assign(str, length);
                    }
                } else if (Depth_ == ARGS_DEPTH && InArgs_ && Key_ == EKey::Detail) {
                    Event_.Detail.assign(str, length);
                }
                Key_ = EKey::None;
                return true;
            }

            bool Int(int value) {
                return Number(value);
            }

            bool Uint(unsigned value) {
                return Number(value);
            }

            bool Int64(int64_t value) {
                return Number(value);
            }

            bool Uint64(uint64_t value) {
                return Number(int64_t(std::min<uint64_t>(value, std::numeric_limits<int64_t>::max())));
            }

            bool Double(double value) {
                if (!std::isfinite(value)) {
                    return Number(0);
                }
                return Number(std::llround(std::clamp(value, -9.0e18, 9.0e18)));
            }

            bool Default() {
                Key_ = EKey::None;
                return true;
            }

        private:
            bool Number(int64_t value) {
                if (Depth_ == EVENT_DEPTH && InEvent_) {
                    if (Key_ == EKey::Ts) {
                        Event_.Ts = value;
                    } else if (Key_ == EKey::Dur) {
                        Event_.Dur = value;
                    }
                }
                Key_ = EKey::None;
                return true;
            }

            void Reset() {
                Names.clear();
                Occurrences.clear();
                Execute = 0;
                TotalExecute = 0;
                HasTotalExecute = false;
                TotalFrontend = 0;
                Unbalanced = 0;
                Index_.clear();
                Begins_.clear();
                OrphanEnds_ = 0;
            }

            void AddSource(const std::string& detail, int64_t ts, int64_t dur) {
                auto [it, added] = Index_.emplace(detail, uint32_t(Names.size()));
                if (added) {
                    Names.push_back(detail);
                }
                Occurrences.push_back({it->second, ts, dur});
            }

            void AddEvent() {
                const TEvent& e = Event_;
                if (e.Name == "Source" && e.Ph == "b") {
                    Begins_.emplace_back(e.Ts, e.Detail);
                } else if (e.Name == "Source" && e.Ph == "e") {
                    // An end closes the latest begin.
                    if (Begins_.empty()) {
                        ++OrphanEnds_;
                    } else {
                        AddSource(Begins_.back().second, Begins_.back().first, e.Ts - Begins_.back().first);
                        Begins_.pop_back();
                    }
                } else if (e.Ph == "X") {
                    if (StartsWith(e.Name, "Total ")) {
                        std::string_view total = std::string_view(e.Name).substr(6);
                        if (total == "ExecuteCompiler") {
                            TotalExecute = e.Dur;
                            HasTotalExecute = true;
                        } else if (total == "Frontend") {
                            TotalFrontend = e.Dur;
                        }
                    } else if (e.Name == "ExecuteCompiler") {
                        Execute = std::max(Execute, e.Dur);
                    } else if (e.Name == "Source") {
                        AddSource(e.Detail, e.Ts, e.Dur);
                    }
                }
                Unbalanced = int64_t(Begins_.size()) + OrphanEnds_;
            }

            enum class EKey {
                None,
                TraceEvents,
                Ph,
                Name,
                Ts,
                Dur,
                Args,
                Detail,
            };

            struct TEvent {
                std::string Ph;
                std::string Name;
                std::string Detail;
                int64_t Ts = 0;
                int64_t Dur = 0;
            };

        public:
            bool Found = false;
            std::vector<std::string> Names;
            std::vector<TOccurrence> Occurrences;
            int64_t Execute = 0;
            int64_t TotalExecute = 0;
            bool HasTotalExecute = false;
            int64_t TotalFrontend = 0;
            int64_t Unbalanced = 0;

        private:
            int Depth_ = 0;
            EKey Key_ = EKey::None;
            bool InEvents_ = false;
            bool InEvent_ = false;
            bool InArgs_ = false;
            TEvent Event_;
            std::unordered_map<std::string, uint32_t> Index_;
            // (ts, detail) of the Source begins not ended yet.
            std::vector<std::pair<int64_t, std::string>> Begins_;
            int64_t OrphanEnds_ = 0;
        };
    } // namespace

    void TTraceSummary::Save(TBinWriter& out) const {
        out.U64(TRACE_SUMMARY_FORMAT);
        out.StrList(Names);
        for (size_t i = 0; i < Names.size(); ++i) {
            out.I64(Parse[i]);
            out.I64(Own[i]);
        }
        out.U64(Edges.size());
        for (const auto& [from, to] : Edges) {
            out.U64(from);
            out.U64(to);
        }
        out.I64(First);
        out.I64(Outer);
        out.I64(Execute);
        out.I64(Frontend);
        out.I64(Unbalanced);
    }

    TTraceSummary TTraceSummary::Load(TBinReader& in) {
        uint64_t format = in.U64();
        if (format != TRACE_SUMMARY_FORMAT) {
            Fail(Format("a trace summary of format %llu, not %llu", (unsigned long long)format,
                (unsigned long long)TRACE_SUMMARY_FORMAT));
        }
        TTraceSummary out;
        out.Names = in.StrList();
        out.Parse.resize(out.Names.size());
        out.Own.resize(out.Names.size());
        for (size_t i = 0; i < out.Names.size(); ++i) {
            out.Parse[i] = in.I64();
            out.Own[i] = in.I64();
        }
        uint64_t edges = in.U64();
        for (uint64_t i = 0; i < edges; ++i) {
            uint64_t from = in.U64();
            uint64_t to = in.U64();
            if (from >= out.Names.size() || to >= out.Names.size()) {
                Fail("a bad trace summary");
            }
            out.Edges.emplace_back(uint32_t(from), uint32_t(to));
        }
        out.First = in.I64();
        out.Outer = in.I64();
        out.Execute = in.I64();
        out.Frontend = in.I64();
        out.Unbalanced = in.I64();
        return out;
    }

    bool SummarizeTrace(std::string_view json, TTraceSummary* out, std::string* error) {
        TTraceHandler handler;
        rapidjson::MemoryStream stream(json.data(), json.size());
        rapidjson::Reader reader;
        rapidjson::ParseResult result = reader.Parse<rapidjson::kParseDefaultFlags>(stream, handler);
        if (result.IsError()) {
            *error = Format("%s at %zu", rapidjson::GetParseError_En(result.Code()), result.Offset());
            return false;
        }
        if (!handler.Found) {
            *error = "no traceEvents";
            return false;
        }

        // As summarize_trace() of pch_update.py: the Source events in the
        // order of the nesting, the outer before the inner.
        std::vector<TOccurrence>& occurrences = handler.Occurrences;
        std::stable_sort(occurrences.begin(), occurrences.end(), [](const TOccurrence& a, const TOccurrence& b) {
            return std::make_tuple(a.Ts, -a.Dur) < std::make_tuple(b.Ts, -b.Dur);
        });

        TTraceSummary summary;
        summary.Names = std::move(handler.Names);
        summary.Parse.assign(summary.Names.size(), 0);
        summary.Own.assign(summary.Names.size(), 0);
        // (end, header) of the open headers.
        std::vector<std::pair<int64_t, uint32_t>> stack;
        for (const TOccurrence& o : occurrences) {
            while (!stack.empty() && o.Ts >= stack.back().first) {
                stack.pop_back();
            }
            summary.Parse[o.Header] += o.Dur;
            summary.Own[o.Header] += o.Dur;
            if (stack.empty()) {
                summary.Outer += o.Dur;
                if (summary.First < 0) {
                    summary.First = o.Header;
                }
            } else {
                uint32_t top = stack.back().second;
                summary.Own[top] -= o.Dur;
                if (top != o.Header) {
                    summary.Edges.emplace_back(top, o.Header);
                }
            }
            stack.emplace_back(o.Ts + o.Dur, o.Header);
        }
        std::sort(summary.Edges.begin(), summary.Edges.end());
        summary.Edges.erase(std::unique(summary.Edges.begin(), summary.Edges.end()), summary.Edges.end());

        summary.Execute = handler.HasTotalExecute ? handler.TotalExecute : handler.Execute;
        summary.Frontend = handler.TotalFrontend;
        summary.Unbalanced = handler.Unbalanced;
        *out = std::move(summary);
        return true;
    }

} // namespace NPchUpdate
