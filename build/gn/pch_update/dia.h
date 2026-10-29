#pragma once

#include <cstdint>
#include <string>
#include <string_view>
#include <vector>

namespace NPchUpdate {

    // A diagnostic of clang's --serialize-diagnostics file (LLVM bitstream).
    struct TDiagnostic {
        enum ELevel {
            Ignored = 0,
            Note = 1,
            Warning = 2,
            Error = 3,
            Fatal = 4,
            Remark = 5,
        };

        int Level = Ignored;
        // As the compiler names it; empty for no location.
        std::string File;
        uint32_t Line = 0;
        uint32_t Column = 0;
        std::string Category;
        std::string Flag;
        std::string Message;
        std::vector<TDiagnostic> Notes;
    };

    // The top-level diagnostics of a .dia file in order, each with its notes;
    // throws TError for a file that is not one.
    std::vector<TDiagnostic> ReadSerializedDiagnostics(std::string_view data);

} // namespace NPchUpdate
