#pragma once

#include <cstdint>
#include <memory>
#include <string>
#include <string_view>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

namespace NPchUpdate {

    class TNinjaManifest;
    class TWorkDir;

    inline constexpr std::string_view LIBCXX = "contrib/libs/cxxsupp/libcxx/include/";
    inline constexpr std::string_view GEN = "<out>/gen/";
    inline constexpr std::string_view UTIL = "//util:util";
    inline constexpr std::string_view ENUM_OBJS = "_enum_objs";
    inline constexpr std::string_view PCH_DIR = "build/gn/pch";
    inline constexpr std::string_view GLOBAL_DIR = "build/gn/pch/global";
    inline constexpr std::string_view PER_TARGET_DIR = "build/gn/pch/per_target";
    inline constexpr std::string_view GROUPS_GNI = "build/gn/pch/global/groups.gni";
    inline constexpr std::string_view TARGETS_GNI = "build/gn/pch/per_target/targets.gni";
    inline constexpr std::string_view ENUM_EXCLUDE_GNI = "build/gn/pch/global/enum_serialization.gni";
    inline constexpr std::string_view PCH_FLAGS_GNI = "build/gn/pch/global/pch_flags.gni";
    inline constexpr std::string_view WRITTEN = "written by build/gn/pch_update";

    // Paths of the baseline as the traces and depfiles name them: relative to
    // the source root for sources, <out>/... for the build dir, <sysroot>/...
    class TPaths {
    public:
        TPaths() = default;
        TPaths(const std::string& buildDir, const std::string& sourceRoot, const std::string& sysroot);

        std::string Normalize(std::string_view path) const;
        // The file a normalized name is.
        std::string Real(std::string_view name) const;

        std::string BuildDir;
        std::string SourceRoot;
        std::string Sysroot;
        // The source root as the build names it too, e.g. through a symlink.
        std::vector<std::string> SourceRoots;
    };

    // The options of a run.
    struct TOptions {
        std::string Baseline;
        bool PerTarget = false;
        std::vector<std::string> Targets = {"all", "tests"};
        double Q = 0.0;
        int MaxGroupFiles = 500;
        double MaxMissingTraces = 0.01;
        int Jobs = 0;
        std::string MainRef = "origin/main";
        std::string WorkDir;
        std::string Ninja = "ninja";
        // Roots tried at once by a step of the search of the root breaking a file.
        int BisectWays = 4;
    };

    // What the steps learn of the baseline, the tree and the history.
    struct TFacts {
        std::string Root;
        std::string BuildDir;
        TPaths Paths;
        std::string Compiler;
        std::string CompilerDir;
        std::unique_ptr<TNinjaManifest> Manifest;
        // label -> the outputs gn writes for it, and back.
        std::unordered_map<std::string, std::string> LabelOfOutput;
        std::unordered_map<std::string, std::vector<std::string>> OutputsOfLabel;
        // gn desc: label -> type, the labels it depends on.
        std::unordered_map<std::string, std::string> TypeOf;
        std::unordered_map<std::string, std::vector<std::string>> DepsOf;
        // Durations of .ninja_log, by output.
        std::unordered_map<std::string, int64_t> DurationMs;

        // CheckBaseline: (object, its trace) of the C++ files built with one,
        // by object; ReadTraces takes them.
        std::vector<std::pair<std::string, std::string>> Traced;

        // The C++ files of the baseline with a trace, sorted by object.
        std::vector<std::string> Objs;
        std::unordered_map<std::string, int32_t> FileOf;
        std::vector<std::string> Traces;
        std::vector<int64_t> NinjaMs;
        std::vector<int64_t> CompileUs;
        std::vector<int64_t> HeaderUs;
        std::vector<int32_t> FirstHeader;
        // The edge of the manifest compiling each file.
        std::vector<int64_t> EdgeOf;

        // The headers: the files parsing each one, the parse there without
        // and with the nested headers; the includes seen.
        std::vector<std::string> HeaderNames;
        std::unordered_map<std::string, int32_t> HeaderId;
        std::vector<std::vector<int32_t>> HeaderFiles;
        std::vector<std::vector<int64_t>> HeaderOwn;
        std::vector<std::vector<int64_t>> HeaderParse;
        std::unordered_set<uint64_t> HeaderEdges;

        // .ninja_deps of the files, normalized names.
        std::vector<std::vector<int32_t>> DepsOfFile;
        std::vector<std::string> DepNames;

        // History: per commit its files (with the generated files made of
        // them), the C++ files it rebuilds.
        std::vector<std::string> Commits;
        std::vector<std::vector<std::string>> CommitFiles;
        std::vector<std::vector<int32_t>> Rebuilt;
        // Generated file (normalized) -> us its command ends at with
        // unlimited parallelism.
        std::unordered_map<std::string, int64_t> ReadyOf;

        // Flag classes, the one of the most compile first.
        std::vector<std::vector<int32_t>> Classes;
        std::vector<std::vector<std::string>> ClassFlags;
        std::vector<int32_t> ClassOf;
        // Per file: the target whose PCH it takes, its -D/-U, include dirs
        // (option, dir), source, the compile as the checks run it.
        std::vector<int32_t> TargetOf;
        // Per file: its own target (of a <target>_enum_objs file: that one).
        std::vector<int32_t> LabelOf;
        std::vector<std::string> TargetNames;
        std::unordered_map<std::string, int32_t> TargetId;
        std::vector<std::vector<std::string>> DefinesOf;
        std::vector<std::vector<std::pair<std::string, std::string>>> IncludeDirsOf;
        std::vector<std::string> SourceOf;
        std::vector<std::vector<std::string>> ArgvOf;

        // Headers compiling alone; empty: not known, all taken as compiling.
        std::vector<char> SelfContained;
        // Of each header compiling alone: the symbols its static
        // initializers need of the libraries.
        std::vector<std::vector<std::string>> NeedsOfHeader;

        int32_t Target(const std::string& label);
    };

    std::string Display(std::string_view name);
    // How a PCH includes a header of the traces.
    std::string Spelling(const TFacts& facts, std::string_view name);

} // namespace NPchUpdate
