#pragma once

#include "common.h"

#include <cstdint>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace NPchUpdate {

    // A clang -ftime-trace of one compile: its headers with their parse time
    // (a header parsed twice: summed) and own time (without the nested
    // headers), the includes seen, the header parsed first, ExecuteCompiler,
    // Frontend, the parse of the outermost headers.
    struct TTraceSummary {
        // As the trace names them (the "detail" of Source).
        std::vector<std::string> Names;
        std::vector<int64_t> Parse;
        std::vector<int64_t> Own;
        // (including, included), indices of Names, sorted, unique.
        std::vector<std::pair<uint32_t, uint32_t>> Edges;
        int64_t First = -1;
        int64_t Outer = 0;
        int64_t Execute = 0;
        int64_t Frontend = 0;
        // Source begins without an end and ends without a begin.
        int64_t Unbalanced = 0;

        void Save(TBinWriter& out) const;
        static TTraceSummary Load(TBinReader& in);
    };

    // False with `error` set for a file that is no trace.
    bool SummarizeTrace(std::string_view json, TTraceSummary* out, std::string* error);

} // namespace NPchUpdate
