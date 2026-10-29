#include "elf.h"
#include "jobs.h"
#include "ninja.h"

#include <fcntl.h>
#include <sys/file.h>
#include <unistd.h>

#include <algorithm>
#include <cerrno>
#include <cstring>
#include <set>

namespace NPchUpdate {

    namespace {

        // Of stderr a failure keeps: its end.
        constexpr size_t STDERR_TAIL = 4000;

        std::string Tail(const std::string& text) {
            return text.size() <= STDERR_TAIL ? text : text.substr(text.size() - STDERR_TAIL);
        }

        // A job: its result, its temporary dir, the files it read.
        class TJobContext {
        public:
            explicit TJobContext(const std::string& result)
                : Result_(result)
                , Tmp_(result + ".tmp")
            {
                RemoveTree(Tmp_);
                MakeDirs(Tmp_);
            }

            ~TJobContext() {
                try {
                    RemoveTree(Tmp_);
                } catch (const std::exception&) {
                }
            }

            const std::string& Result() const {
                return Result_;
            }

            std::string Tmp(const std::string& name) const {
                return JoinPath(Tmp_, name);
            }

            // Inputs of the job for its depfile: absolute, but its temporary
            // files and the ones that come and go (a PCH of the checks).
            void AddDeps(const std::vector<std::string>& deps) {
                for (const std::string& dep : deps) {
                    if (!StartsWith(dep, Tmp_ + "/") && !Skip_.count(dep)) {
                        Deps_.insert(dep);
                    }
                }
            }

            void Skip(const std::string& path) {
                Skip_.insert(path);
            }

            void WriteDepfile() const {
                WriteFile(Result_ + ".d", NPchUpdate::WriteDepfile(Result_, {Deps_.begin(), Deps_.end()}));
            }

        private:
            std::string Result_;
            std::string Tmp_;
            std::set<std::string> Deps_;
            std::set<std::string> Skip_;
        };

        struct TCompileRun {
            TCompileResult Result;
            std::string Stderr;
        };

        // Runs a compile with its diagnostics serialized and its depfile:
        // the errors and the files read come from them, not from stderr.
        TCompileRun Compile(TJobContext& job, const std::string& cwd, std::vector<std::string> argv,
                            const std::function<void(std::string_view)>& onStdout = {}) {
            std::string dia = job.Tmp("compile.dia");
            std::string depfile = job.Tmp("compile.d");
            argv.insert(argv.end(), {"--serialize-diagnostics", dia, "-MD", "-MF", depfile, "-MT", "x"});
            TRunOptions options;
            options.Cwd = cwd;
            options.CaptureStdout = bool(onStdout);
            options.OnStdout = onStdout;
            TRunResult run = Run(argv, options);

            TCompileRun out;
            out.Result.ExitCode = run.ExitCode;
            std::string data;
            if (TryReadFile(dia, &data) && !data.empty()) {
                for (TDiagnostic& d : ReadSerializedDiagnostics(data)) {
                    if (d.Level == TDiagnostic::Error || d.Level == TDiagnostic::Fatal) {
                        out.Result.Errors.push_back(std::move(d));
                    }
                }
            }
            if (run.ExitCode != 0) {
                out.Result.StderrTail = Tail(run.Stderr);
            }
            if (TryReadFile(depfile, &data)) {
                for (const std::string& dep : ReadDepfile(data)) {
                    out.Result.Deps.push_back(NormPath(IsAbs(dep) ? dep : JoinPath(cwd, dep)));
                }
                std::sort(out.Result.Deps.begin(), out.Result.Deps.end());
                out.Result.Deps.erase(std::unique(out.Result.Deps.begin(), out.Result.Deps.end()), out.Result.Deps.end());
            }
            out.Stderr = std::move(run.Stderr);
            return out;
        }

        // A link argv with its OBJ and SO.
        std::vector<std::string> LinkArgv(const std::vector<std::string>& argv, const std::string& obj, const std::string& so) {
            std::vector<std::string> out;
            for (const std::string& a : argv) {
                out.push_back(a == NJobArgs::OBJ ? obj : a == NJobArgs::SO ? so : a);
            }
            return out;
        }

        struct TLinked {
            int ExitCode = -1;
            std::string Error;
            std::vector<std::string> Unresolved;
        };

        // The object linked alone: what of the libraries it needs, as the
        // dynamic symbols of the shared object have it.
        TLinked Link(const std::string& cwd, const std::vector<std::string>& argv, const std::string& obj,
                     const std::string& so) {
            TRunOptions options;
            options.Cwd = cwd;
            options.CaptureStdout = false;
            TRunResult run = Run(LinkArgv(argv, obj, so), options);
            TLinked out;
            out.ExitCode = run.ExitCode;
            if (run.ExitCode != 0) {
                out.Error = Tail(run.Stderr);
                return out;
            }
            out.Unresolved = ReadSharedObjectSymbols(so).Unresolved;
            return out;
        }

        // args = <head...> -- <compile argv...>
        void SplitArgs(const std::vector<std::string>& args, size_t head, std::vector<std::string>* before,
                       std::vector<std::string>* compile) {
            auto sep = std::find(args.begin(), args.end(), std::string(NJobArgs::SEPARATOR));
            if (sep == args.end() || size_t(sep - args.begin()) < head) {
                Fail("job: bad arguments");
            }
            before->assign(args.begin(), sep);
            compile->assign(sep + 1, args.end());
            if (compile->empty()) {
                Fail("job: no compile");
            }
        }

        void TraceJob(const std::string& result, const std::vector<std::string>& args) {
            if (args.size() != 1) {
                Fail("trace: bad arguments");
            }
            TTraceResult out;
            std::string error;
            {
                TMappedFile file(args[0]);
                out.Ok = SummarizeTrace(file.Data(), &out.Summary, &error);
            }
            out.Error = error;
            SaveResult(result, out);
        }

        void SymbolsJob(const std::string& result, const std::vector<std::string>& args) {
            if (args.size() != 1) {
                Fail("syms: bad arguments");
            }
            SaveSymbolsResult(result, ReadDefinedSymbols(args[0]));
        }

        void HeaderJob(const std::string& result, const std::vector<std::string>& args) {
            std::vector<std::string> head, argv;
            SplitArgs(args, 2, &head, &argv);
            const std::string& cwd = head[0];
            const std::string& source = head[1];
            std::vector<std::string> link(head.begin() + 2, head.end());

            TJobContext job(result);
            std::string obj = job.Tmp("h.o");
            std::string trace = job.Tmp("h.json");
            argv.insert(argv.end(), {"-ftime-trace=" + trace, "-ftime-trace-granularity=0", "-c", source, "-o", obj});
            TCompileRun run = Compile(job, cwd, argv);

            THeaderResult out;
            out.Compile = std::move(run.Result);
            if (out.Compile.ExitCode == 0) {
                TTraceSummary summary;
                std::string error;
                TMappedFile file(trace);
                if (!SummarizeTrace(file.Data(), &summary, &error)) {
                    Fail("header: the trace of " + source + ": " + error);
                }
                for (const auto& [a, b] : summary.Edges) {
                    out.Edges.emplace_back(summary.Names[a], summary.Names[b]);
                }
                if (!link.empty()) {
                    TLinked linked = Link(cwd, link, obj, job.Tmp("h.so"));
                    out.LinkExit = linked.ExitCode;
                    out.Unresolved = std::move(linked.Unresolved);
                }
            }
            job.AddDeps(out.Compile.Deps);
            SaveResult(result, out);
            job.WriteDepfile();
        }

        // Builds the PCH of a pch job: written whole or not at all.
        TPchResult BuildPch(TJobContext& job, const std::vector<std::string>& args) {
            std::vector<std::string> head, argv;
            SplitArgs(args, 3, &head, &argv);
            const std::string& cwd = head[0];
            const std::string& pch = head[1];
            const std::string& header = head[2];
            std::string tmp = job.Tmp("out.pch");
            argv.insert(argv.end(), {header, "-o", tmp});
            TCompileRun run = Compile(job, cwd, argv);
            TPchResult out;
            out.Compile = std::move(run.Result);
            out.Pch = pch;
            out.JobArgs = args;
            if (out.Compile.ExitCode == 0) {
                MakeDirs(DirName(pch));
                if (rename(tmp.c_str(), pch.c_str()) != 0) {
                    Fail(Format("rename %s: %s", pch.c_str(), strerror(errno)));
                }
            }
            return out;
        }

        void PchJob(const std::string& result, const std::vector<std::string>& args) {
            TJobContext job(result);
            TPchResult out = BuildPch(job, args);
            job.AddDeps(out.Compile.Deps);
            SaveResult(result, out);
            job.WriteDepfile();
        }

        // The PCH of a pch result, made again if the checks removed it.
        TPchResult TakePch(const std::string& pchResult) {
            TPchResult pch = LoadPchResult(pchResult);
            if (pch.Compile.ExitCode != 0 || Exists(pch.Pch)) {
                return pch;
            }
            std::string lock = pch.Pch + ".lock";
            MakeDirs(DirName(lock));
            int fd = open(lock.c_str(), O_RDWR | O_CREAT | O_CLOEXEC, 0644);
            if (fd < 0 || flock(fd, LOCK_EX) != 0) {
                Fail(Format("%s: %s", lock.c_str(), strerror(errno)));
            }
            if (!Exists(pch.Pch)) {
                TJobContext job(pchResult + ".again");
                TPchResult again = BuildPch(job, pch.JobArgs);
                if (again.Compile.ExitCode != 0) {
                    pch.Compile = again.Compile;
                }
            }
            close(fd);
            return pch;
        }

        // A compile with a PCH: the PCH failing, none (an exit code of -1).
        bool CompileWithPch(TJobContext& job, const std::string& cwd, const std::string& pchResult,
                            const std::vector<std::string>& argv, TCompileRun* run) {
            TPchResult pch = TakePch(pchResult);
            job.Skip(pch.Pch);
            // what the PCH read is read by the file too
            job.AddDeps(pch.Compile.Deps);
            if (pch.Compile.ExitCode != 0) {
                run->Result.ExitCode = -1;
                run->Result.StderrTail = "the PCH failed";
                return false;
            }
            *run = Compile(job, cwd, argv);
            job.AddDeps(run->Result.Deps);
            return true;
        }

        void TuJob(const std::string& result, const std::vector<std::string>& args) {
            std::vector<std::string> head, argv;
            SplitArgs(args, 2, &head, &argv);
            TJobContext job(result);
            TCompileRun run;
            CompileWithPch(job, head[0], head[1], argv, &run);
            SaveResult(result, run.Result);
            job.WriteDepfile();
        }

        void LinkJob(const std::string& result, const std::vector<std::string>& args) {
            std::vector<std::string> head, argv;
            SplitArgs(args, 3, &head, &argv);
            const std::string& cwd = head[0];
            std::vector<std::string> link(head.begin() + 2, head.end());
            TJobContext job(result);
            std::string empty = job.Tmp("empty.cpp");
            WriteFile(empty, "");
            std::string obj = job.Tmp("empty.o");
            argv.insert(argv.end(), {"-c", empty, "-o", obj});
            TCompileRun run;
            TLinkResult out;
            if (CompileWithPch(job, cwd, head[1], argv, &run) && run.Result.ExitCode == 0) {
                TLinked linked = Link(cwd, link, obj, job.Tmp("empty.so"));
                out.LinkExit = linked.ExitCode;
                out.LinkError = std::move(linked.Error);
                out.Unresolved = std::move(linked.Unresolved);
            }
            out.Compile = std::move(run.Result);
            SaveResult(result, out);
            job.WriteDepfile();
        }

        // Lines of a stream given in chunks.
        class TLineReader {
        public:
            explicit TLineReader(std::function<void(std::string_view)> onLine)
                : OnLine_(std::move(onLine))
            {
            }

            void operator()(std::string_view chunk) {
                while (!chunk.empty()) {
                    size_t end = chunk.find('\n');
                    if (end == std::string_view::npos) {
                        Partial_ += chunk;
                        return;
                    }
                    if (Partial_.empty()) {
                        OnLine_(chunk.substr(0, end));
                    } else {
                        Partial_ += chunk.substr(0, end);
                        OnLine_(Partial_);
                        Partial_.clear();
                    }
                    chunk.remove_prefix(end + 1);
                }
            }

            void Finish() {
                if (!Partial_.empty()) {
                    OnLine_(Partial_);
                    Partial_.clear();
                }
            }

        private:
            std::function<void(std::string_view)> OnLine_;
            std::string Partial_;
        };

        // A line marker of clang's -E: # <line> "<file>" <flags>.
        struct TMarker {
            std::string File;
            bool Enter = false;
            bool Leave = false;
        };

        bool ParseMarker(std::string_view line, TMarker* marker) {
            if (!StartsWith(line, "# ")) {
                return false;
            }
            size_t i = 2;
            size_t digits = i;
            while (i < line.size() && line[i] >= '0' && line[i] <= '9') {
                ++i;
            }
            if (i == digits || i + 1 >= line.size() || line[i] != ' ' || line[i + 1] != '"') {
                return false;
            }
            i += 2;
            marker->File.clear();
            // clang escapes \ and " of the name, and the bytes not printable as octal
            for (; i < line.size() && line[i] != '"'; ++i) {
                if (line[i] != '\\' || i + 1 >= line.size()) {
                    marker->File += line[i];
                    continue;
                }
                ++i;
                if (line[i] >= '0' && line[i] <= '7') {
                    int value = 0;
                    for (int k = 0; k < 3 && i < line.size() && line[i] >= '0' && line[i] <= '7'; ++k, ++i) {
                        value = value * 8 + (line[i] - '0');
                    }
                    --i;
                    marker->File += char(value);
                } else {
                    marker->File += line[i];
                }
            }
            if (i >= line.size()) {
                return false;
            }
            marker->Enter = marker->Leave = false;
            for (std::string_view flag : Split(line.substr(i + 1), ' ')) {
                if (flag == "1") {
                    marker->Enter = true;
                } else if (flag == "2") {
                    marker->Leave = true;
                }
            }
            return true;
        }

        void PreprocessJob(const std::string& result, const std::vector<std::string>& args) {
            std::vector<std::string> head, argv;
            SplitArgs(args, 1, &head, &argv);
            TJobContext job(result);

            std::map<std::string, THasher> digests;
            std::set<std::pair<std::string, std::string>> edges;
            THasher* current = nullptr;
            std::string currentName;
            TMarker marker;
            TLineReader lines([&](std::string_view line) {
                if (ParseMarker(line, &marker)) {
                    if (marker.Enter && current) {
                        current->Update(std::string_view("\0include ", 9));
                        current->Update(marker.File);
                        edges.emplace(currentName, marker.File);
                    }
                    currentName = marker.File;
                    current = &digests[currentName];
                    return;
                }
                if (current && line.find_first_not_of(" \t\r") != std::string_view::npos) {
                    current->Update(line);
                    current->Update("\n");
                }
            });
            TCompileRun run = Compile(job, head[0], argv, std::ref(lines));
            lines.Finish();

            TPreprocessResult out;
            out.Compile = std::move(run.Result);
            if (out.Compile.ExitCode == 0) {
                for (const auto& [name, hasher] : digests) {
                    out.Digests[name] = hasher.Hex();
                }
                out.Edges.assign(edges.begin(), edges.end());
            }
            job.AddDeps(out.Compile.Deps);
            SaveResult(result, out);
            job.WriteDepfile();
        }

        bool IsIdentStart(char c) {
            return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || c == '_';
        }

        bool IsIdent(char c) {
            return IsIdentStart(c) || (c >= '0' && c <= '9');
        }

        void DefinesJob(const std::string& result, const std::vector<std::string>& args) {
            std::vector<std::string> head, argv;
            SplitArgs(args, 1, &head, &argv);
            TJobContext job(result);

            // -E -dD: the #define and #undef in place; of the source itself
            // (the file of the first marker) the ones before its last #include
            std::vector<std::pair<std::string, std::string>> found;
            size_t last = 0;
            std::string main;
            bool inMain = false;
            bool first = true;
            TMarker marker;
            TLineReader lines([&](std::string_view line) {
                if (ParseMarker(line, &marker)) {
                    if (first) {
                        main = marker.File;
                        first = false;
                    } else if (inMain && marker.Enter) {
                        last = found.size();
                    }
                    inMain = marker.File == main;
                    return;
                }
                if (!inMain) {
                    return;
                }
                if (StartsWith(line, "#define ")) {
                    std::string_view rest = line.substr(8);
                    size_t n = 0;
                    if (rest.empty() || !IsIdentStart(rest[0])) {
                        return;
                    }
                    while (n < rest.size() && IsIdent(rest[n])) {
                        ++n;
                    }
                    std::string name(rest.substr(0, n));
                    std::string params;
                    if (n < rest.size() && rest[n] == '(') {
                        size_t close = rest.find(')', n);
                        if (close == std::string_view::npos) {
                            return;
                        }
                        params = rest.substr(n, close + 1 - n);
                        n = close + 1;
                    }
                    std::string_view body = rest.substr(n);
                    while (!body.empty() && (body.front() == ' ' || body.front() == '\t')) {
                        body.remove_prefix(1);
                    }
                    while (!body.empty() && (body.back() == ' ' || body.back() == '\t' || body.back() == '\r')) {
                        body.remove_suffix(1);
                    }
                    found.emplace_back(name, "-D" + name + params + "=" + std::string(body));
                } else if (StartsWith(line, "#undef ")) {
                    std::string_view name = line.substr(7);
                    while (!name.empty() && (name.back() == ' ' || name.back() == '\r')) {
                        name.remove_suffix(1);
                    }
                    size_t kept = 0;
                    for (size_t i = 0; i < found.size(); ++i) {
                        if (found[i].first != name) {
                            found[kept++] = std::move(found[i]);
                        } else if (i < last) {
                            --last;
                        }
                    }
                    found.resize(kept);
                }
            });
            argv.insert(argv.end(), {"-E", "-dD"});
            TCompileRun run = Compile(job, head[0], argv, std::ref(lines));
            lines.Finish();

            TDefinesResult out;
            out.ExitCode = run.Result.ExitCode;
            if (out.ExitCode == 0) {
                found.resize(last);
                out.Defines = std::move(found);
            }
            job.AddDeps(run.Result.Deps);
            SaveResult(result, out);
            job.WriteDepfile();
        }

    } // namespace

    int RunJob(const std::vector<std::string>& args) {
        if (args.size() != 2) {
            Fail("job: <kind> <result>");
        }
        const std::string& kind = args[0];
        const std::string& result = args[1];
        std::vector<std::string> spec = NJobArgs::Decode(ReadFile(result + ".rsp"));
        if (kind == "trace") {
            TraceJob(result, spec);
        } else if (kind == "syms") {
            SymbolsJob(result, spec);
        } else if (kind == "header") {
            HeaderJob(result, spec);
        } else if (kind == "pch") {
            PchJob(result, spec);
        } else if (kind == "tu") {
            TuJob(result, spec);
        } else if (kind == "link") {
            LinkJob(result, spec);
        } else if (kind == "pp") {
            PreprocessJob(result, spec);
        } else if (kind == "defines") {
            DefinesJob(result, spec);
        } else if (kind == "gn") {
            GnJob(result, spec);
        } else {
            Fail("job: unknown kind " + kind);
        }
        return 0;
    }

} // namespace NPchUpdate
