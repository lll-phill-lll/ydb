#pragma once

#include "common.h"
#include "dia.h"
#include "trace.h"

#include <map>
#include <memory>
#include <string>
#include <utility>
#include <vector>

namespace NPchUpdate {

    // Of what a job writes: older results are not taken (part of each command).
    inline constexpr int JOBS_VERSION = 1;

    // The work dir of the tool: a ninja manifest of the jobs of its runs,
    // ninja runs them. A job runs again only when its command (its kind and
    // arguments) or an input of it (its depfile, its inputs) changed; the
    // jobs of earlier runs stay in the manifest for that.
    class TWorkDir {
    public:
        struct TJob {
            std::string Kind;
            // All the job depends on but its input files: its identity.
            std::vector<std::string> Args;
            // Absolute paths, or results of other jobs.
            std::vector<std::string> Inputs;
            // "heavy": the jobs of much memory (a PCH), fewer at once.
            std::string Pool;
            // The job writes the files it read as a depfile.
            bool Depfile = true;
            // The job leaves its result as it was if it is the same: the jobs
            // taking it do not run again.
            bool Restat = false;
        };

        TWorkDir(const std::string& dir, const std::string& ninja, int jobs);
        ~TWorkDir();
        TWorkDir(const TWorkDir&) = delete;
        TWorkDir& operator=(const TWorkDir&) = delete;

        // The path of the result of `job`.
        std::string Add(const TJob& job);
        // Runs the jobs of `results` not done yet; fails if ninja does.
        void Build(const std::vector<std::string>& results, const std::string& what);
        // Saves the jobs of the runs; drops the ones no run used for a while.
        void Save();

        const std::string& Dir() const {
            return Dir_;
        }

        int Jobs() const {
            return Jobs_;
        }

        // A dir for the side files of a kind of job (the PCH of the checks).
        std::string SideDir(const std::string& kind) const;

    private:
        std::string ResultPath(const std::string& kind, const std::string& key) const;

        struct TEdge {
            std::string Kind;
            std::string Text;
            uint64_t LastRun = 0;
        };

        std::string Dir_;
        std::string Ninja_;
        int Jobs_ = 1;
        int LockFd_ = -1;
        uint64_t Run_ = 0;
        std::string Tool_;
        std::map<std::string, TEdge> Edges_;
        bool Dirty_ = false;
        size_t Builds_ = 0;
    };

    // The results of the jobs.
    struct TCompileResult {
        int ExitCode = 0;
        // Errors and fatal errors, each with its notes.
        std::vector<TDiagnostic> Errors;
        // The end of stderr, for a failure with no diagnostic (a crash).
        std::string StderrTail;
        // The files the compile read, absolute.
        std::vector<std::string> Deps;

        void Save(TBinWriter& out) const;
        void Load(TBinReader& in);
        // The first error as "file:line: message", or the end of stderr.
        std::string FirstError() const;
    };

    // A header compiled alone, its object linked alone.
    struct THeaderResult {
        TCompileResult Compile;
        // (including, included) headers its trace shows, as named there.
        std::vector<std::pair<std::string, std::string>> Edges;
        int LinkExit = -1;
        std::vector<std::string> Unresolved;
    };

    struct TPchResult {
        TCompileResult Compile;
        std::string Pch;
        // The arguments of the job: a job taking the PCH makes it again if gone.
        std::vector<std::string> JobArgs;
    };

    // The empty file with a PCH, compiled and linked alone.
    struct TLinkResult {
        TCompileResult Compile;
        int LinkExit = -1;
        std::string LinkError;
        std::vector<std::string> Unresolved;
    };

    // -E: of each file as the line markers name it, the digest of what it
    // gives (its lines but the blank ones, the files it includes).
    struct TPreprocessResult {
        TCompileResult Compile;
        std::map<std::string, std::string> Digests;
        std::vector<std::pair<std::string, std::string>> Edges;
    };

    // The #define of a source before its last #include as its preprocessor
    // sees them: (name, -D argument).
    struct TDefinesResult {
        int ExitCode = 0;
        std::vector<std::pair<std::string, std::string>> Defines;
    };

    // Results are written only if changed: a job of restat leaves them be.
    void SaveResult(const std::string& path, const TCompileResult& result);
    void SaveResult(const std::string& path, const THeaderResult& result);
    void SaveResult(const std::string& path, const TPchResult& result);
    void SaveResult(const std::string& path, const TLinkResult& result);
    void SaveResult(const std::string& path, const TPreprocessResult& result);
    void SaveResult(const std::string& path, const TDefinesResult& result);
    void SaveSymbolsResult(const std::string& path, const std::vector<std::string>& symbols);

    // A trace summarized, or the error of reading it.
    struct TTraceResult {
        bool Ok = false;
        std::string Error;
        TTraceSummary Summary;
    };
    void SaveResult(const std::string& path, const TTraceResult& result);
    TTraceResult LoadTraceResult(const std::string& path);

    THeaderResult LoadHeaderResult(const std::string& path);
    TPchResult LoadPchResult(const std::string& path);
    TCompileResult LoadCompileResult(const std::string& path);
    TLinkResult LoadLinkResult(const std::string& path);
    TPreprocessResult LoadPreprocessResult(const std::string& path);
    TDefinesResult LoadDefinesResult(const std::string& path);
    std::vector<std::string> LoadSymbolsResult(const std::string& path);

    // The gn job (baseline.cpp): gn gen of the args of a build dir in a dir
    // of its own, gn desc of it.
    void GnJob(const std::string& result, const std::vector<std::string>& args);

    // `pch_update job <kind> <result>`: runs a job of the manifest.
    int RunJob(const std::vector<std::string>& args);

    // Arguments of the jobs.
    namespace NJobArgs {
        // The sources the jobs compile are files the tool writes before the
        // build (named by their content), inputs of the jobs. A compile argv
        // has no output, depfile or diagnostics options: the job adds them.
        // A link argv has OBJ and SO for the object and the shared object.
        //
        // trace <trace>
        // syms <library>
        // header <cwd> <source> <link argv...> -- <compile argv...>: -c
        // pch <cwd> <pch> <header> -- <compile argv...>: <header> -o <pch>
        // tu <cwd> <pch result> -- <compile argv...>: a file with -include-pch
        // link <cwd> <pch result> <link argv...> -- <compile argv...>: -c
        // pp <cwd> -- <compile argv...>: -E to stdout
        // defines <cwd> -- <compile argv...>: -E -dD to stdout
        // gn: see GnJob
        inline constexpr const char* OBJ = "<obj>";
        inline constexpr const char* SO = "<so>";
        inline constexpr const char* SEPARATOR = "--";

        // The arguments in the response file of a job: each "=" and its
        // bytes, % space tab CR LF as %XX, separated by spaces.
        std::string Encode(const std::vector<std::string>& args);
        std::vector<std::string> Decode(std::string_view text);
    } // namespace NJobArgs

} // namespace NPchUpdate
