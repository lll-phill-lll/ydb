#pragma once

#include <string>
#include <vector>

namespace NPchUpdate {

    // The dynamic symbols of a shared object (ELF64 little endian).
    struct TSharedObjectSymbols {
        // Defined, GLOBAL or WEAK binding, any type but SECTION/FILE.
        std::vector<std::string> Defined;
        // Undefined, GLOBAL binding, not resolved at the link: no version
        // (index 1 of .gnu.version, or no .gnu.version at all). The WEAK
        // undefined ones (__gmon_start__, _ITM_*) are left out.
        std::vector<std::string> Unresolved;
    };

    TSharedObjectSymbols ReadSharedObjectSymbols(const std::string& path);

    // The global symbols a static archive defines: of its symbol index
    // ("/" of GNU ar, "/SYM64/"), else of the .symtab of each member.
    std::vector<std::string> ReadArchiveDefined(const std::string& path);

    // The global symbols (GLOBAL or WEAK, defined) of the .symtab of an
    // object file.
    std::vector<std::string> ReadObjectDefined(const std::string& path);

    // Of a path: an archive, a shared object or an object, by its content.
    std::vector<std::string> ReadDefinedSymbols(const std::string& path);

} // namespace NPchUpdate
