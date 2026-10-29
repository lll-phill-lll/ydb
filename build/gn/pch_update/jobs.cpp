#include "jobs.h"
#include "ninja.h"

#include <fcntl.h>
#include <sys/file.h>
#include <unistd.h>

#include <algorithm>
#include <cerrno>
#include <cstring>

namespace NPchUpdate {

    namespace {

        constexpr uint64_t REGISTRY_TAG = 0x70636875'72656701ull + JOBS_VERSION;
        constexpr uint64_t RESULT_TAG = 0x70636875'72657301ull + JOBS_VERSION;
        // A job no run took for so many runs leaves the manifest.
        constexpr uint64_t KEEP_RUNS = 5;

        std::string RuleName(const TWorkDir::TJob& job) {
            std::string name = "job";
            if (job.Depfile) {
                name += "_d";
            }
            if (job.Restat) {
                name += "_r";
            }
            return name;
        }

        void SaveDiagnostic(TBinWriter& out, const TDiagnostic& d) {
            out.I64(d.Level);
            out.Str(d.File);
            out.U64(d.Line);
            out.U64(d.Column);
            out.Str(d.Category);
            out.Str(d.Flag);
            out.Str(d.Message);
            out.U64(d.Notes.size());
            for (const TDiagnostic& note : d.Notes) {
                SaveDiagnostic(out, note);
            }
        }

        TDiagnostic LoadDiagnostic(TBinReader& in) {
            TDiagnostic d;
            d.Level = int(in.I64());
            d.File = in.Str();
            d.Line = uint32_t(in.U64());
            d.Column = uint32_t(in.U64());
            d.Category = in.Str();
            d.Flag = in.Str();
            d.Message = in.Str();
            uint64_t notes = in.U64();
            for (uint64_t i = 0; i < notes; ++i) {
                d.Notes.push_back(LoadDiagnostic(in));
            }
            return d;
        }

        void SavePairs(TBinWriter& out, const std::vector<std::pair<std::string, std::string>>& pairs) {
            out.U64(pairs.size());
            for (const auto& [a, b] : pairs) {
                out.Str(a);
                out.Str(b);
            }
        }

        std::vector<std::pair<std::string, std::string>> LoadPairs(TBinReader& in) {
            std::vector<std::pair<std::string, std::string>> pairs(in.U64());
            for (auto& [a, b] : pairs) {
                a = in.Str();
                b = in.Str();
            }
            return pairs;
        }

        TBinWriter StartResult(std::string_view kind) {
            TBinWriter out;
            out.U64(RESULT_TAG);
            out.Str(kind);
            return out;
        }

        void FinishResult(const std::string& path, const TBinWriter& out) {
            WriteFileIfChanged(path, out.Data());
        }

        // The reader of a result past its header; fails for another kind or
        // version of the tool.
        class TResultReader {
        public:
            TResultReader(const std::string& path, std::string_view kind)
                : Data_(ReadFile(path))
                , In_(Data_)
            {
                if (In_.U64() != RESULT_TAG || In_.Str() != kind) {
                    Fail(Format("%s: not a %.*s result of this version", path.c_str(), int(kind.size()), kind.data()));
                }
            }

            TBinReader& In() {
                return In_;
            }

        private:
            std::string Data_;
            TBinReader In_;
        };

    } // namespace

    namespace NJobArgs {

        std::string Encode(const std::vector<std::string>& args) {
            static const char HEX[] = "0123456789ABCDEF";
            std::string out;
            for (const std::string& arg : args) {
                if (!out.empty()) {
                    out += ' ';
                }
                out += '=';
                for (char c : arg) {
                    if (c == '%' || c == ' ' || c == '\t' || c == '\r' || c == '\n') {
                        out += '%';
                        out += HEX[(unsigned char)c >> 4];
                        out += HEX[(unsigned char)c & 15];
                    } else {
                        out += c;
                    }
                }
            }
            return out;
        }

        std::vector<std::string> Decode(std::string_view text) {
            auto hex = [&](char c) {
                if (c >= '0' && c <= '9') {
                    return c - '0';
                }
                if (c >= 'A' && c <= 'F') {
                    return c - 'A' + 10;
                }
                Fail("bad job arguments: a bad %XX");
            };
            std::vector<std::string> args;
            while (!text.empty() && (text.back() == '\n' || text.back() == '\r')) {
                text.remove_suffix(1);
            }
            if (text.empty()) {
                return args;
            }
            for (std::string_view word : Split(text, ' ')) {
                if (word.empty() || word[0] != '=') {
                    Fail("bad job arguments: a word without '='");
                }
                std::string arg;
                for (size_t i = 1; i < word.size(); ++i) {
                    if (word[i] == '%') {
                        if (i + 2 >= word.size()) {
                            Fail("bad job arguments: a cut %XX");
                        }
                        arg += char(hex(word[i + 1]) * 16 + hex(word[i + 2]));
                        i += 2;
                    } else {
                        arg += word[i];
                    }
                }
                args.push_back(std::move(arg));
            }
            return args;
        }

    } // namespace NJobArgs

    TWorkDir::TWorkDir(const std::string& dir, const std::string& ninja, int jobs)
        : Dir_(AbsPath(dir))
        , Ninja_(ninja)
        , Jobs_(std::max(jobs, 1))
        , Tool_(SelfExe())
    {
        MakeDirs(Dir_);
        std::string lock = JoinPath(Dir_, "lock");
        LockFd_ = open(lock.c_str(), O_RDWR | O_CREAT | O_CLOEXEC, 0644);
        if (LockFd_ < 0) {
            Fail(Format("%s: %s", lock.c_str(), strerror(errno)));
        }
        if (flock(LockFd_, LOCK_EX | LOCK_NB) != 0) {
            Fail(Format("%s: another run of the tool has the work dir", Dir_.c_str()));
        }
        std::string data;
        if (TryReadFile(JoinPath(Dir_, "jobs.bin"), &data)) {
            TBinReader in(data);
            if (in.U64() == REGISTRY_TAG) {
                Run_ = in.U64();
                uint64_t n = in.U64();
                for (uint64_t i = 0; i < n; ++i) {
                    std::string key(in.Str());
                    TEdge& edge = Edges_[key];
                    edge.Kind = in.Str();
                    edge.Text = in.Str();
                    edge.LastRun = in.U64();
                }
            }
        }
        ++Run_;
        Dirty_ = true;
    }

    TWorkDir::~TWorkDir() {
        try {
            if (Dirty_) {
                Save();
            }
        } catch (const std::exception& e) {
            Log(Format("saving the jobs of %s: %s", Dir_.c_str(), e.what()));
        }
        if (LockFd_ >= 0) {
            close(LockFd_);
        }
    }

    std::string TWorkDir::SideDir(const std::string& kind) const {
        std::string dir = JoinPath(JoinPath(Dir_, "side"), kind);
        MakeDirs(dir);
        return dir;
    }

    std::string TWorkDir::ResultPath(const std::string& kind, const std::string& key) const {
        return JoinPath(Dir_, "j/" + kind + "/" + key.substr(0, 2) + "/" + key);
    }

    std::string TWorkDir::Add(const TJob& job) {
        THasher hasher;
        hasher.Field(int64_t(JOBS_VERSION));
        hasher.Field(job.Kind);
        hasher.Field(int64_t(job.Args.size()));
        for (const std::string& arg : job.Args) {
            hasher.Field(arg);
        }
        hasher.Field(int64_t(job.Inputs.size()));
        for (const std::string& input : job.Inputs) {
            hasher.Field(input);
        }
        hasher.Field(job.Pool);
        hasher.Field(int64_t(job.Depfile));
        hasher.Field(int64_t(job.Restat));
        std::string key = hasher.Hex();
        std::string result = ResultPath(job.Kind, key);

        TEdge& edge = Edges_[key];
        if (edge.Text.empty()) {
            edge.Kind = job.Kind;
            std::string text = "build " + NinjaEscapePath(result) + ": " + RuleName(job);
            for (const std::string& input : job.Inputs) {
                text += " " + NinjaEscapePath(input);
            }
            text += "\n  kind = " + job.Kind + "\n";
            text += "  spec = " + NinjaEscapeValue(NJobArgs::Encode(job.Args)) + "\n";
            if (!job.Pool.empty()) {
                text += "  pool = " + job.Pool + "\n";
            }
            edge.Text = std::move(text);
        }
        if (edge.LastRun != Run_) {
            edge.LastRun = Run_;
            Dirty_ = true;
        }
        return result;
    }

    void TWorkDir::Save() {
        TBinWriter out;
        out.U64(REGISTRY_TAG);
        out.U64(Run_);
        std::vector<const std::pair<const std::string, TEdge>*> kept;
        for (auto it = Edges_.begin(); it != Edges_.end();) {
            if (it->second.LastRun + KEEP_RUNS < Run_) {
                // Not in the manifest any more: no job takes the result.
                RemoveFile(ResultPath(it->second.Kind, it->first));
                it = Edges_.erase(it);
            } else {
                kept.push_back(&*it);
                ++it;
            }
        }
        out.U64(kept.size());
        for (const auto* edge : kept) {
            out.Str(edge->first);
            out.Str(edge->second.Kind);
            out.Str(edge->second.Text);
            out.U64(edge->second.LastRun);
        }
        WriteFile(JoinPath(Dir_, "jobs.bin"), out.Data());
        Dirty_ = false;
    }

    void TWorkDir::Build(const std::vector<std::string>& results, const std::string& what) {
        if (results.empty()) {
            return;
        }
        // The jobs known before ninja runs any: a crash of the tool leaves
        // no result ninja does not know.
        Save();

        std::string manifest;
        manifest += "# " + std::string("written by build/gn/pch_update: the jobs of its runs") + "\n";
        manifest += "ninja_required_version = 1.10\n\n";
        manifest += Format("pool heavy\n  depth = %d\n\n", std::max(Jobs_ / 3, 1));
        for (bool depfile : {false, true}) {
            for (bool restat : {false, true}) {
                TJob job;
                job.Depfile = depfile;
                job.Restat = restat;
                manifest += "rule " + RuleName(job) + "\n";
                manifest += "  command = " + NinjaEscapeValue(ShellQuote(Tool_)) + " job $kind $out\n";
                manifest += "  description = $kind $out\n";
                manifest += "  rspfile = $out.rsp\n";
                manifest += "  rspfile_content = $spec\n";
                if (depfile) {
                    manifest += "  depfile = $out.d\n";
                    manifest += "  deps = gcc\n";
                }
                if (restat) {
                    manifest += "  restat = 1\n";
                }
                manifest += "\n";
            }
        }
        for (const auto& [key, edge] : Edges_) {
            manifest += edge.Text;
        }
        manifest += "\nbuild want: phony";
        for (const std::string& result : results) {
            manifest += " " + NinjaEscapePath(result);
        }
        manifest += "\ndefault want\n";
        WriteFileIfChanged(JoinPath(Dir_, "build.ninja"), manifest);

        ++Builds_;
        TRunOptions options;
        options.CaptureStdout = false;
        options.CaptureStderr = false;
        std::vector<std::string> argv = {Ninja_, "-C", Dir_, "-j", std::to_string(Jobs_), "-k", "0"};
        if (isatty(STDOUT_FILENO)) {
            options.Env.push_back("NINJA_STATUS=[" + what + " %f/%t] ");
        } else {
            argv.push_back("--quiet");
        }
        argv.push_back("want");
        Log(Format("%s: %zu jobs", what.c_str(), results.size()));
        TRunResult run = Run(argv, options);
        if (run.ExitCode != 0) {
            Fail(Format("%s: ninja of %s failed with %d", what.c_str(), Dir_.c_str(), run.ExitCode));
        }
    }

    void TCompileResult::Save(TBinWriter& out) const {
        out.I64(ExitCode);
        out.U64(Errors.size());
        for (const TDiagnostic& d : Errors) {
            SaveDiagnostic(out, d);
        }
        out.Str(StderrTail);
        out.StrList(Deps);
    }

    void TCompileResult::Load(TBinReader& in) {
        ExitCode = int(in.I64());
        Errors.resize(in.U64());
        for (TDiagnostic& d : Errors) {
            d = LoadDiagnostic(in);
        }
        StderrTail = in.Str();
        Deps = in.StrList();
    }

    std::string TCompileResult::FirstError() const {
        for (const TDiagnostic& d : Errors) {
            if (d.Level >= TDiagnostic::Error && d.Level <= TDiagnostic::Fatal) {
                if (d.File.empty()) {
                    return d.Message;
                }
                return Format("%s:%u: %s", d.File.c_str(), d.Line, d.Message.c_str());
            }
        }
        std::string_view tail = StderrTail;
        while (!tail.empty() && tail.back() == '\n') {
            tail.remove_suffix(1);
        }
        if (!tail.empty()) {
            size_t line = tail.rfind('\n');
            return std::string(line == std::string_view::npos ? tail : tail.substr(line + 1));
        }
        return Format("exit code %d", ExitCode);
    }

    void SaveResult(const std::string& path, const TCompileResult& result) {
        TBinWriter out = StartResult("compile");
        result.Save(out);
        FinishResult(path, out);
    }

    void SaveResult(const std::string& path, const THeaderResult& result) {
        TBinWriter out = StartResult("header");
        result.Compile.Save(out);
        SavePairs(out, result.Edges);
        out.I64(result.LinkExit);
        out.StrList(result.Unresolved);
        FinishResult(path, out);
    }

    void SaveResult(const std::string& path, const TPchResult& result) {
        TBinWriter out = StartResult("pch");
        result.Compile.Save(out);
        out.Str(result.Pch);
        out.StrList(result.JobArgs);
        FinishResult(path, out);
    }

    void SaveResult(const std::string& path, const TLinkResult& result) {
        TBinWriter out = StartResult("link");
        result.Compile.Save(out);
        out.I64(result.LinkExit);
        out.Str(result.LinkError);
        out.StrList(result.Unresolved);
        FinishResult(path, out);
    }

    void SaveResult(const std::string& path, const TPreprocessResult& result) {
        TBinWriter out = StartResult("pp");
        result.Compile.Save(out);
        out.U64(result.Digests.size());
        for (const auto& [name, digest] : result.Digests) {
            out.Str(name);
            out.Str(digest);
        }
        SavePairs(out, result.Edges);
        FinishResult(path, out);
    }

    void SaveResult(const std::string& path, const TDefinesResult& result) {
        TBinWriter out = StartResult("defines");
        out.I64(result.ExitCode);
        SavePairs(out, result.Defines);
        FinishResult(path, out);
    }

    void SaveResult(const std::string& path, const TTraceResult& result) {
        TBinWriter out = StartResult("trace");
        out.U64(result.Ok);
        out.Str(result.Error);
        if (result.Ok) {
            result.Summary.Save(out);
        }
        FinishResult(path, out);
    }

    TTraceResult LoadTraceResult(const std::string& path) {
        TResultReader reader(path, "trace");
        TTraceResult result;
        result.Ok = reader.In().U64() != 0;
        result.Error = reader.In().Str();
        if (result.Ok) {
            result.Summary = TTraceSummary::Load(reader.In());
        }
        return result;
    }

    void SaveSymbolsResult(const std::string& path, const std::vector<std::string>& symbols) {
        TBinWriter out = StartResult("syms");
        out.StrList(symbols);
        FinishResult(path, out);
    }

    TCompileResult LoadCompileResult(const std::string& path) {
        TResultReader reader(path, "compile");
        TCompileResult result;
        result.Load(reader.In());
        return result;
    }

    THeaderResult LoadHeaderResult(const std::string& path) {
        TResultReader reader(path, "header");
        THeaderResult result;
        result.Compile.Load(reader.In());
        result.Edges = LoadPairs(reader.In());
        result.LinkExit = int(reader.In().I64());
        result.Unresolved = reader.In().StrList();
        return result;
    }

    TPchResult LoadPchResult(const std::string& path) {
        TResultReader reader(path, "pch");
        TPchResult result;
        result.Compile.Load(reader.In());
        result.Pch = reader.In().Str();
        result.JobArgs = reader.In().StrList();
        return result;
    }

    TLinkResult LoadLinkResult(const std::string& path) {
        TResultReader reader(path, "link");
        TLinkResult result;
        result.Compile.Load(reader.In());
        result.LinkExit = int(reader.In().I64());
        result.LinkError = reader.In().Str();
        result.Unresolved = reader.In().StrList();
        return result;
    }

    TPreprocessResult LoadPreprocessResult(const std::string& path) {
        TResultReader reader(path, "pp");
        TPreprocessResult result;
        result.Compile.Load(reader.In());
        uint64_t n = reader.In().U64();
        for (uint64_t i = 0; i < n; ++i) {
            std::string name(reader.In().Str());
            result.Digests[name] = reader.In().Str();
        }
        result.Edges = LoadPairs(reader.In());
        return result;
    }

    TDefinesResult LoadDefinesResult(const std::string& path) {
        TResultReader reader(path, "defines");
        TDefinesResult result;
        result.ExitCode = int(reader.In().I64());
        result.Defines = LoadPairs(reader.In());
        return result;
    }

    std::vector<std::string> LoadSymbolsResult(const std::string& path) {
        TResultReader reader(path, "syms");
        return reader.In().StrList();
    }

} // namespace NPchUpdate
