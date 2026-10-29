#include "gni.h"
#include "jobs.h"
#include "ninja.h"
#include "steps.h"

#include <algorithm>
#include <ctime>
#include <functional>
#include <iterator>
#include <map>
#include <numeric>
#include <set>

namespace NPchUpdate {

    namespace {

        // In-process work: at most so many threads.
        constexpr size_t THREADS = 8;
        // Traces summarized at once: their summaries kept in memory.
        constexpr size_t TRACE_CHUNK = 1024;
        constexpr int TOP_HEADERS = 20;
        constexpr int TOP_CHANGED = 10;
        constexpr int TOP_CLASSES = 10;
        constexpr int64_t HISTORY_WEEKS = 4;
        // us of parse in all the files: the headers parsed less are not compiled alone
        constexpr int64_t HEADER_MIN_PARSE = 30000000;

        // Flags of a compile that make no other flag class (the ones with a
        // value as one token: -Ifoo, -Wfoo...); FLAG_ARGS have their value next.
        constexpr std::string_view NOT_CLASS_PREFIXES[] = {"-I", "-W", "-D", "-U", "-ftime-trace"};
        constexpr std::string_view NOT_CLASS[] = {"-c", "-MD", "-w"};
        constexpr std::string_view INCLUDE_DIR_ARGS[] = {"-I", "-isystem", "-iquote", "-idirafter"};
        constexpr std::string_view FLAG_ARGS[] = {"-o", "-MF", "-I", "-isystem", "-iquote", "-idirafter", "-include", "-D", "-U", "-x"};
        // What the checks drop of the command of a file (with a value: the next word).
        constexpr std::string_view CHECK_DROP_ARGS[] = {"-o", "-MF", "-MT", "-MQ"};
        constexpr std::string_view CHECK_DROP[] = {"-c", "-MD", "-MMD"};
        // Words of the shell ending the first command of a ninja command.
        constexpr std::string_view COMMAND_ENDS[] = {"&&", "||", ";", "|"};

        template <size_t N>
        bool OneOf(std::string_view word, const std::string_view (&words)[N]) {
            return std::find(std::begin(words), std::end(words), word) != std::end(words);
        }

        template <size_t N>
        bool StartsWithOneOf(std::string_view word, const std::string_view (&prefixes)[N]) {
            return std::any_of(std::begin(prefixes), std::end(prefixes), [&](std::string_view p) {
                return StartsWith(word, p);
            });
        }

        size_t Threads() {
            return std::max<size_t>(1, std::min(THREADS, CpuCount()));
        }

        std::string Trim(std::string_view text) {
            size_t begin = text.find_first_not_of(" \t\r\n");
            if (begin == std::string_view::npos) {
                return {};
            }
            size_t end = text.find_last_not_of(" \t\r\n");
            return std::string(text.substr(begin, end - begin + 1));
        }

        std::string FirstLine(std::string_view text) {
            return std::string(text.substr(0, text.find('\n')));
        }

        // Problems found: what -> examples, in the order found.
        using TProblems = std::map<std::string, std::vector<std::string>>;

        size_t ReportProblems(const TProblems& problems, bool unique) {
            size_t total = 0;
            std::vector<std::pair<const std::string*, std::vector<std::string>>> rows;
            for (const auto& [what, examples] : problems) {
                std::vector<std::string> list = examples;
                if (unique) {
                    std::sort(list.begin(), list.end());
                    list.erase(std::unique(list.begin(), list.end()), list.end());
                }
                total += list.size();
                rows.emplace_back(&what, std::move(list));
            }
            Report(Format("problems:         %zu", total));
            for (const auto& [what, list] : rows) {
                Report(Format("  %zu %s, e.g. %s", list.size(), what->c_str(),
                              (unique ? Display(list[0]) : list[0]).c_str()));
            }
            return total;
        }

        // The names of `paths` normalized, unique, in the order of the first
        // one: of each path its name in `names`.
        std::vector<int32_t> NormalizeAll(const TPaths& paths, const std::vector<std::string>& raw,
                                          std::vector<std::string>* names, std::unordered_map<std::string, int32_t>* idOf) {
            std::vector<std::string> normalized(raw.size());
            ParallelFor(raw.size(), Threads(), [&](size_t i) {
                normalized[i] = paths.Normalize(raw[i]);
            });
            std::vector<int32_t> ids(raw.size());
            for (size_t i = 0; i < raw.size(); ++i) {
                auto [it, added] = idOf->emplace(normalized[i], int32_t(names->size()));
                if (added) {
                    names->push_back(normalized[i]);
                }
                ids[i] = it->second;
            }
            return ids;
        }

        std::string BuildPath(const TFacts& facts, const std::string& path) {
            return IsAbs(path) ? path : JoinPath(facts.BuildDir, path);
        }

        // =====================================================================
        // traces
        // =====================================================================

        // The includes of the PCH of //build/gn/pch/global now: libc++ headers
        // spelled as the traces name them; by their base name.
        class TPchRoots {
        public:
            explicit TPchRoots(const std::string& root) {
                std::string dir = JoinPath(root, GLOBAL_DIR);
                if (!IsDir(dir)) {
                    return;
                }
                std::vector<std::string> headers;
                for (const std::string& d : ListDir(dir)) {
                    if (!IsDir(JoinPath(dir, d))) {
                        continue;
                    }
                    for (const std::string& f : ListDir(JoinPath(dir, d))) {
                        if (EndsWith(f, ".h")) {
                            headers.push_back(d + "/" + f);
                        }
                    }
                }
                std::sort(headers.begin(), headers.end());
                std::map<std::string, std::set<int32_t>> pchOf;
                for (const std::string& header : headers) {
                    int32_t pch = int32_t(Pch.size());
                    Pch.push_back(header);
                    std::string text = ReadFile(JoinPath(dir, header));
                    for (std::string_view line : Split(text, '\n')) {
                        std::string name;
                        if (ParseInclude(line, &name)) {
                            pchOf[name].insert(pch);
                        }
                    }
                }
                for (const auto& [name, pchs] : pchOf) {
                    ByBase_[BaseName(name)].emplace_back(name, std::vector<int32_t>(pchs.begin(), pchs.end()));
                }
            }

            // The PCH whose includes `name` may be: `#include "x/y.h"` is any
            // header named .../x/y.h, the include dir it is found in unknown.
            std::vector<int32_t> Of(const std::string& name) const {
                std::vector<int32_t> out;
                auto it = ByBase_.find(BaseName(name));
                if (it == ByBase_.end()) {
                    return out;
                }
                for (const auto& [spelling, pchs] : it->second) {
                    if (name == spelling || (name.size() > spelling.size() && EndsWith(name, spelling) &&
                                             name[name.size() - spelling.size() - 1] == '/'))
                    {
                        out.insert(out.end(), pchs.begin(), pchs.end());
                    }
                }
                std::sort(out.begin(), out.end());
                out.erase(std::unique(out.begin(), out.end()), out.end());
                return out;
            }

            std::vector<std::string> Pch;

        private:
            // ^\s*#\s*include\s*([<"])([^>"]+)[>"]
            static bool ParseInclude(std::string_view line, std::string* name) {
                size_t i = 0;
                auto skipSpace = [&] {
                    while (i < line.size() && (line[i] == ' ' || line[i] == '\t')) {
                        ++i;
                    }
                };
                skipSpace();
                if (i >= line.size() || line[i] != '#') {
                    return false;
                }
                ++i;
                skipSpace();
                if (line.substr(i, 7) != "include") {
                    return false;
                }
                i += 7;
                skipSpace();
                if (i >= line.size() || (line[i] != '<' && line[i] != '"')) {
                    return false;
                }
                bool system = line[i] == '<';
                size_t end = line.find_first_of(">\"", i + 1);
                if (end == std::string_view::npos || end == i + 1) {
                    return false;
                }
                std::string_view spelling = line.substr(i + 1, end - i - 1);
                *name = system ? std::string(LIBCXX) + std::string(spelling) : std::string(spelling);
                return true;
            }

            std::unordered_map<std::string, std::vector<std::pair<std::string, std::vector<int32_t>>>> ByBase_;
        };

        // A trace summarized with its names normalized (a header named twice
        // there merged), what the checks of the file found.
        struct TTraceFacts {
            bool Ok = false;
            std::string Error;
            std::vector<std::string> Names;
            std::vector<int64_t> Parse;
            std::vector<int64_t> Own;
            std::vector<std::pair<int32_t, int32_t>> Edges;
            int32_t First = -1;
            int64_t Outer = 0;
            int64_t Execute = 0;
            int64_t Frontend = 0;
            bool Unbalanced = false;
            // Of each header the PCH it is in: one of its includes or nested in one.
            std::vector<std::vector<int32_t>> InPch;
            // Ids of DepNames of the file in .ninja_deps.
            std::vector<int32_t> Deps;
            // The first header of the trace not among them.
            std::string Missing;
        };

        void MergeSorted(std::vector<int32_t>* into, const std::vector<int32_t>& from, bool* grew) {
            std::vector<int32_t> out;
            out.reserve(into->size() + from.size());
            std::set_union(into->begin(), into->end(), from.begin(), from.end(), std::back_inserter(out));
            if (out.size() != into->size()) {
                *grew = true;
                *into = std::move(out);
            }
        }

        TTraceFacts ReadTraceFacts(const TFacts& facts, const TPchRoots& roots, const std::string& result,
                                   const std::vector<int32_t>* deps, const std::unordered_map<std::string, int32_t>& depId,
                                   const std::vector<int32_t>& depOfPath) {
            TTraceFacts out;
            TTraceResult trace = LoadTraceResult(result);
            if (!trace.Ok) {
                out.Error = trace.Error;
                return out;
            }
            out.Ok = true;
            const TTraceSummary& s = trace.Summary;
            std::unordered_map<std::string, int32_t> local;
            std::vector<int32_t> idOf(s.Names.size());
            for (size_t h = 0; h < s.Names.size(); ++h) {
                std::string name = facts.Paths.Normalize(s.Names[h]);
                auto [it, added] = local.emplace(name, int32_t(out.Names.size()));
                if (added) {
                    out.Names.push_back(std::move(name));
                    out.Parse.push_back(0);
                    out.Own.push_back(0);
                }
                idOf[h] = it->second;
                out.Parse[it->second] += s.Parse[h];
                out.Own[it->second] += s.Own[h];
            }
            for (int64_t& own : out.Own) {
                own = std::max<int64_t>(own, 0);
            }
            for (const auto& [a, b] : s.Edges) {
                if (idOf[a] != idOf[b]) {
                    out.Edges.emplace_back(idOf[a], idOf[b]);
                }
            }
            std::sort(out.Edges.begin(), out.Edges.end());
            out.Edges.erase(std::unique(out.Edges.begin(), out.Edges.end()), out.Edges.end());
            out.First = s.First >= 0 ? idOf[s.First] : -1;
            out.Outer = s.Outer;
            out.Execute = s.Execute;
            out.Frontend = s.Frontend;
            out.Unbalanced = s.Unbalanced != 0;

            // the PCH of a header: its own, the ones of the headers including it
            size_t n = out.Names.size();
            out.InPch.resize(n);
            std::vector<std::vector<int32_t>> children(n);
            for (const auto& [a, b] : out.Edges) {
                children[a].push_back(b);
            }
            std::vector<int32_t> work;
            for (size_t h = 0; h < n; ++h) {
                out.InPch[h] = roots.Of(out.Names[h]);
                if (!out.InPch[h].empty()) {
                    work.push_back(int32_t(h));
                }
            }
            while (!work.empty()) {
                int32_t h = work.back();
                work.pop_back();
                for (int32_t c : children[h]) {
                    bool grew = false;
                    MergeSorted(&out.InPch[c], out.InPch[h], &grew);
                    if (grew) {
                        work.push_back(c);
                    }
                }
            }

            if (deps) {
                for (int32_t k : *deps) {
                    out.Deps.push_back(depOfPath[k]);
                }
                std::sort(out.Deps.begin(), out.Deps.end());
                out.Deps.erase(std::unique(out.Deps.begin(), out.Deps.end()), out.Deps.end());
                std::vector<std::string> missing;
                for (const std::string& name : out.Names) {
                    auto it = depId.find(name);
                    if (it == depId.end() || !std::binary_search(out.Deps.begin(), out.Deps.end(), it->second)) {
                        missing.push_back(name);
                    }
                }
                if (!missing.empty()) {
                    out.Missing = *std::min_element(missing.begin(), missing.end());
                }
            }
            return out;
        }

        bool KnownLocation(const std::string& name, const std::string& compilerDir) {
            return !IsAbs(name) || (!compilerDir.empty() && StartsWith(name, compilerDir + "/"));
        }

        // =====================================================================
        // history
        // =====================================================================

        std::string Git(const TFacts& facts, const std::vector<std::string>& args) {
            std::vector<std::string> argv = {"git", "-C", facts.Root};
            argv.insert(argv.end(), args.begin(), args.end());
            TRunOptions options;
            TRunResult run = Run(argv, options);
            if (run.ExitCode != 0) {
                Fail("git " + Join(args, " ") + ": " + Trim(run.Stderr));
            }
            return run.Stdout;
        }

        struct TCommits {
            std::string MergeBase;
            int64_t When = 0;
            // (commit, its files)
            std::vector<std::pair<std::string, std::vector<std::string>>> Commits;
        };

        // The first-parent commits of main in HISTORY_WEEKS weeks before the
        // merge base of HEAD and `mainRef`; a merge as its diff to the first parent.
        TCommits CommitsOf(const TFacts& facts, const std::string& mainRef) {
            TCommits out;
            out.MergeBase = Trim(Git(facts, {"merge-base", "HEAD", mainRef}));
            out.When = std::stoll(Trim(Git(facts, {"log", "-1", "--format=%ct", out.MergeBase})));
            std::string since = Format("--since=%lld", (long long)(out.When - HISTORY_WEEKS * 7 * 86400));
            // the commits in order: in the log a word is a commit only as the next of them
            std::vector<std::string> revs;
            std::string revList = Git(facts, {"rev-list", "--first-parent", since, out.MergeBase});
            for (std::string_view line : Split(revList, '\n')) {
                if (!line.empty()) {
                    revs.emplace_back(line);
                }
            }
            std::string log = Git(facts, {"log", "-z", "--first-parent", "-m", "--no-renames", "--name-only", "--format=%H",
                                          since, out.MergeBase});
            size_t next = 0;
            for (std::string_view word : Split(log, '\0')) {
                if (next < revs.size() && word == revs[next]) {
                    out.Commits.emplace_back(revs[next++], std::vector<std::string>());
                    continue;
                }
                // the files of a commit begin after a newline
                if (!word.empty() && word[0] == '\n') {
                    word.remove_prefix(1);
                }
                if (word.empty()) {
                    continue;
                }
                if (out.Commits.empty()) {
                    Fail("git log: a file before the first commit: " + std::string(word));
                }
                out.Commits.back().second.emplace_back(word);
            }
            if (next != revs.size()) {
                Fail(Format("git log: %zu of %zu commits of git rev-list", next, revs.size()));
            }
            return out;
        }

        // Of each generated file of the dependencies (normalized): the source
        // files it is made from -- the inputs of the edge writing it and its
        // .ninja_deps, through other generated files; not the build tools (the
        // programs of the build dir).
        std::unordered_map<std::string, std::set<std::string>> GeneratedSources(const TFacts& facts, const TNinjaDeps& deps) {
            std::unordered_map<std::string, std::vector<std::string>> direct;
            const auto& edges = facts.Manifest->Edges();
            for (const auto& e : edges) {
                if (e.Rule == "phony") {
                    continue;
                }
                std::vector<std::string> ins;
                bool any = false;
                for (const std::string& o : e.Outputs) {
                    if (!StartsWith(o, "gen/")) {
                        continue;
                    }
                    if (!any) {
                        any = true;
                        for (const std::string& p : e.Inputs) {
                            ins.push_back(facts.Paths.Normalize(p));
                        }
                        for (const std::string& p : e.ImplicitInputs) {
                            ins.push_back(facts.Paths.Normalize(p));
                        }
                    }
                    std::vector<std::string> insOfOutput = ins;
                    auto it = deps.Deps.find(o);
                    if (it != deps.Deps.end()) {
                        for (int32_t k : it->second) {
                            insOfOutput.push_back(facts.Paths.Normalize(deps.Paths[k]));
                        }
                    }
                    direct[facts.Paths.Normalize(o)] = std::move(insOfOutput);
                }
            }

            std::unordered_map<std::string, std::set<std::string>> out;
            std::set<std::string> stack;
            std::function<const std::set<std::string>&(const std::string&)> sources;
            static const std::set<std::string> NONE;
            sources = [&](const std::string& name) -> const std::set<std::string>& {
                auto known = out.find(name);
                if (known != out.end()) {
                    return known->second;
                }
                if (stack.count(name)) {
                    return NONE;
                }
                stack.insert(name);
                std::set<std::string> found;
                auto it = direct.find(name);
                if (it != direct.end()) {
                    for (const std::string& p : it->second) {
                        if (StartsWith(p, GEN)) {
                            const auto& more = sources(p);
                            found.insert(more.begin(), more.end());
                        } else if (!StartsWith(p, "<") && !IsAbs(p)) {
                            found.insert(p);
                        }
                    }
                }
                stack.erase(name);
                return out[name] = std::move(found);
            };
            for (const auto& [name, ins] : direct) {
                sources(name);
            }
            return out;
        }

        // Of each generated file (normalized): us its command ends at with
        // unlimited parallelism, the durations of .ninja_log.
        std::unordered_map<std::string, int64_t> ReadyTimes(const TFacts& facts) {
            const auto& edges = facts.Manifest->Edges();
            size_t n = edges.size();
            std::vector<int64_t> duration(n, 0);
            std::vector<std::vector<int64_t>> deps(n);
            for (size_t i = 0; i < n; ++i) {
                const auto& e = edges[i];
                if (!e.Outputs.empty()) {
                    auto it = facts.DurationMs.find(e.Outputs[0]);
                    if (it == facts.DurationMs.end()) {
                        it = facts.DurationMs.find(NormPath(e.Outputs[0]));
                    }
                    if (it != facts.DurationMs.end()) {
                        duration[i] = it->second;
                    }
                }
                for (const auto* list : {&e.Inputs, &e.ImplicitInputs, &e.OrderOnly}) {
                    for (const std::string& p : *list) {
                        int64_t j = facts.Manifest->Producer(p);
                        if (j >= 0) {
                            deps[i].push_back(j);
                        }
                    }
                }
                std::sort(deps[i].begin(), deps[i].end());
                deps[i].erase(std::unique(deps[i].begin(), deps[i].end()), deps[i].end());
            }
            // finish = the latest finish of the deps + the duration; a cycle: the dep not counted
            std::vector<int64_t> finish(n, -1);
            std::vector<char> open(n, 0);
            std::vector<std::pair<size_t, size_t>> stack;
            for (size_t root = 0; root < n; ++root) {
                if (finish[root] >= 0) {
                    continue;
                }
                stack.emplace_back(root, 0);
                open[root] = 1;
                while (!stack.empty()) {
                    auto& [node, next] = stack.back();
                    if (next < deps[node].size()) {
                        size_t d = size_t(deps[node][next++]);
                        if (finish[d] < 0 && !open[d]) {
                            open[d] = 1;
                            stack.emplace_back(d, 0);
                        }
                        continue;
                    }
                    int64_t best = 0;
                    for (int64_t d : deps[node]) {
                        best = std::max(best, finish[d]);
                    }
                    finish[node] = best + duration[node];
                    open[node] = 0;
                    stack.pop_back();
                }
            }
            std::unordered_map<std::string, int64_t> out;
            for (size_t i = 0; i < n; ++i) {
                for (const std::string& o : edges[i].Outputs) {
                    if (StartsWith(o, "gen/")) {
                        out[facts.Paths.Normalize(o)] = finish[i] * 1000;
                    }
                }
            }
            return out;
        }

        std::string Day(int64_t t) {
            time_t when = time_t(t);
            struct tm tm;
            localtime_r(&when, &tm);
            char buf[32];
            strftime(buf, sizeof(buf), "%Y-%m-%d", &tm);
            return buf;
        }

        // =====================================================================
        // flag classes
        // =====================================================================

        // The compile of an edge's command: from the compiler to the end of
        // the first command; the wrapper before it dropped.
        std::vector<std::string> CompilerArgv(const std::string& command, const std::string& compiler) {
            std::vector<std::string> argv;
            size_t pos = compiler.empty() ? std::string::npos : command.find(compiler + " ");
            if (pos != std::string::npos) {
                argv = ShellSplit(std::string_view(command).substr(pos));
            } else {
                argv = ShellSplit(command);
                auto it = std::find_if(argv.begin(), argv.end(), [](const std::string& word) {
                    return StartsWith(BaseName(word), "clang++");
                });
                if (it == argv.end()) {
                    Fail("no compiler in: " + command.substr(0, 300));
                }
                argv.erase(argv.begin(), it);
            }
            auto end = std::find_if(argv.begin() + 1, argv.end(), [](const std::string& word) {
                return OneOf(word, COMMAND_ENDS);
            });
            argv.erase(end, argv.end());
            return argv;
        }

        struct TCompileFlags {
            std::vector<std::string> Flags;
            std::vector<std::string> Defines;
            std::vector<std::pair<std::string, std::string>> IncludeDirs;
        };

        // The flags making the class of a compile, its -D and -U, its include
        // dirs; the forced includes and the inputs of the edge are none of them.
        TCompileFlags CompileFlags(const std::vector<std::string>& argv, const std::vector<std::string>& inputs) {
            TCompileFlags out;
            size_t i = 1;
            while (i < argv.size()) {
                const std::string& a = argv[i];
                if (a == "-Xclang" && i + 2 < argv.size() && argv[i + 1] == "-include" && argv[i + 2] == "-Xclang") {
                    i += 4;
                    continue;
                }
                if (OneOf(a, FLAG_ARGS) && i + 1 < argv.size()) {
                    const std::string& value = argv[i + 1];
                    if (a == "-D" || a == "-U") {
                        out.Defines.push_back(a + value);
                    } else if (OneOf(a, INCLUDE_DIR_ARGS)) {
                        out.IncludeDirs.emplace_back(a, value);
                    } else if (a == "-x") {
                        out.Flags.push_back(a);
                        out.Flags.push_back(value);
                    }
                    i += 2;
                    continue;
                }
                if (StartsWith(a, "-D") || StartsWith(a, "-U")) {
                    out.Defines.push_back(a);
                } else if (StartsWith(a, "-I")) {
                    out.IncludeDirs.emplace_back("-I", a.substr(2));
                } else if (!OneOf(a, NOT_CLASS) && !StartsWithOneOf(a, NOT_CLASS_PREFIXES) &&
                           std::find(inputs.begin(), inputs.end(), a) == inputs.end())
                {
                    out.Flags.push_back(a);
                }
                ++i;
            }
            return out;
        }

        // The compile as the checks run it: the output, the depfile and
        // -ftime-trace dropped.
        std::vector<std::string> CheckArgv(const std::vector<std::string>& argv) {
            std::vector<std::string> out;
            size_t i = 0;
            while (i < argv.size()) {
                const std::string& a = argv[i];
                if (OneOf(a, CHECK_DROP_ARGS)) {
                    i += 2;
                    continue;
                }
                if (!OneOf(a, CHECK_DROP) && !StartsWith(a, "-ftime-trace")) {
                    out.push_back(a);
                }
                ++i;
            }
            return out;
        }

        // The target whose PCH the files of a target take: the one of its
        // enum serialization for <target>_enum_objs (serialize_enums,
        // //build/gn/base.gni) but for enum_serialization_pch_exclude.
        std::string PchOwner(const std::string& label, const std::vector<std::string>& exclude) {
            size_t colon = label.find(':');
            std::string_view name = colon == std::string::npos ? std::string_view() : std::string_view(label).substr(colon + 1);
            if (!EndsWith(name, ENUM_OBJS)) {
                return label;
            }
            std::string owner = label.substr(0, label.size() - ENUM_OBJS.size());
            return LabelMatches(owner, exclude) ? label : owner;
        }

        std::vector<std::string> EnumExclude(const TFacts& facts) {
            std::string text;
            if (!TryReadFile(JoinPath(facts.Root, ENUM_EXCLUDE_GNI), &text)) {
                return {};
            }
            auto values = ParseGnFile(text);
            const TGnValue* value = FindGnValue(values, "enum_serialization_pch_exclude");
            return value ? value->Strings() : std::vector<std::string>();
        }

    } // namespace

    // =========================================================================
    // the names
    // =========================================================================

    std::string Display(std::string_view name) {
        if (StartsWith(name, LIBCXX)) {
            return "<" + std::string(name.substr(LIBCXX.size())) + ">";
        }
        if (StartsWith(name, "<out>/")) {
            return std::string(name.substr(6));
        }
        return std::string(name);
    }

    std::string Spelling(const TFacts& facts, std::string_view name) {
        if (StartsWith(name, LIBCXX)) {
            return "<" + std::string(name.substr(LIBCXX.size())) + ">";
        }
        if (StartsWith(name, GEN)) {
            return "\"" + std::string(name.substr(GEN.size())) + "\"";
        }
        if (IsAbs(name) || StartsWith(name, "<")) {
            return "\"" + facts.Paths.Real(name) + "\"";
        }
        return "\"" + std::string(name) + "\"";
    }

    int32_t TFacts::Target(const std::string& label) {
        auto [it, added] = TargetId.emplace(label, int32_t(TargetNames.size()));
        if (added) {
            TargetNames.push_back(label);
        }
        return it->second;
    }

    // =========================================================================
    // traces
    // =========================================================================

    bool ReadTraces(const TOptions& options, TWorkDir& work, TFacts& facts) {
        std::vector<std::pair<std::string, std::string>> traced = facts.Traced;
        std::sort(traced.begin(), traced.end());
        std::vector<std::string> results;
        results.reserve(traced.size());
        for (auto& [obj, trace] : traced) {
            trace = BuildPath(facts, trace);
            TWorkDir::TJob job;
            job.Kind = "trace";
            job.Args = {trace};
            job.Inputs = {trace};
            job.Depfile = false;
            results.push_back(work.Add(job));
        }
        Log(Format("summarizing %zu traces...", traced.size()));
        work.Build(results, "traces");

        Log("reading .ninja_deps...");
        TNinjaDeps deps = ReadNinjaDeps(JoinPath(facts.BuildDir, ".ninja_deps"));
        facts.DepNames.clear();
        std::unordered_map<std::string, int32_t> depId;
        std::vector<int32_t> depOfPath = NormalizeAll(facts.Paths, deps.Paths, &facts.DepNames, &depId);
        TPchRoots roots(facts.Root);

        facts.Objs.clear();
        facts.FileOf.clear();
        facts.Traces.clear();
        facts.NinjaMs.clear();
        facts.CompileUs.clear();
        facts.HeaderUs.clear();
        facts.FirstHeader.clear();
        facts.EdgeOf.clear();
        facts.HeaderNames.clear();
        facts.HeaderId.clear();
        facts.HeaderFiles.clear();
        facts.HeaderOwn.clear();
        facts.HeaderParse.clear();
        facts.HeaderEdges.clear();
        facts.DepsOfFile.clear();

        TProblems problems;
        std::vector<std::set<int32_t>> inPch;
        int64_t execute = 0;
        int64_t outer = 0;
        Log(Format("reading %zu traces with %zu threads...", traced.size(), Threads()));
        std::vector<TTraceFacts> chunk;
        for (size_t begin = 0; begin < traced.size(); begin += TRACE_CHUNK) {
            size_t end = std::min(traced.size(), begin + TRACE_CHUNK);
            chunk.assign(end - begin, TTraceFacts());
            ParallelFor(end - begin, Threads(), [&](size_t i) {
                auto it = deps.Deps.find(traced[begin + i].first);
                chunk[i] = ReadTraceFacts(facts, roots, results[begin + i], it == deps.Deps.end() ? nullptr : &it->second,
                                          depId, depOfPath);
            });
            for (size_t i = 0; i < chunk.size(); ++i) {
                const std::string& obj = traced[begin + i].first;
                TTraceFacts& t = chunk[i];
                if (!t.Ok) {
                    problems["files: the trace is unreadable"].push_back(obj + " (" + t.Error + ")");
                    continue;
                }
                int32_t f = int32_t(facts.Objs.size());
                facts.Objs.push_back(obj);
                facts.FileOf[obj] = f;
                facts.Traces.push_back(traced[begin + i].second);
                auto ms = facts.DurationMs.find(obj);
                int64_t ninjaMs = ms == facts.DurationMs.end() ? -1 : ms->second;
                facts.NinjaMs.push_back(std::max<int64_t>(ninjaMs, 0));
                facts.CompileUs.push_back(t.Execute);
                facts.HeaderUs.push_back(t.Outer);
                facts.EdgeOf.push_back(facts.Manifest->Producer(obj));
                execute += t.Execute;
                outer += t.Outer;

                std::vector<int32_t> ids(t.Names.size());
                for (size_t h = 0; h < t.Names.size(); ++h) {
                    auto [it, added] = facts.HeaderId.emplace(t.Names[h], int32_t(facts.HeaderNames.size()));
                    if (added) {
                        facts.HeaderNames.push_back(t.Names[h]);
                        facts.HeaderFiles.emplace_back();
                        facts.HeaderOwn.emplace_back();
                        facts.HeaderParse.emplace_back();
                        inPch.emplace_back();
                    }
                    int32_t id = it->second;
                    ids[h] = id;
                    facts.HeaderFiles[id].push_back(f);
                    facts.HeaderOwn[id].push_back(t.Own[h]);
                    facts.HeaderParse[id].push_back(t.Parse[h]);
                    inPch[id].insert(t.InPch[h].begin(), t.InPch[h].end());
                }
                for (const auto& [a, b] : t.Edges) {
                    facts.HeaderEdges.insert(uint64_t(uint32_t(ids[a])) << 32 | uint32_t(ids[b]));
                }
                facts.FirstHeader.push_back(t.First >= 0 ? ids[t.First] : -1);
                facts.DepsOfFile.push_back(std::move(t.Deps));

                if (facts.EdgeOf.back() < 0) {
                    problems["files: no edge of the manifest writes the object"].push_back(obj);
                }
                if (t.Unbalanced) {
                    problems["files: Source events without a pair"].push_back(obj);
                }
                if (double(t.Outer) > double(t.Frontend) * 1.01 + 1000 || double(t.Frontend) > double(t.Execute) * 1.01 + 1000) {
                    problems["files: headers longer than Frontend or Frontend longer than ExecuteCompiler"].push_back(obj);
                }
                if (ninjaMs >= 0 && double(t.Execute) / 1000.0 > double(ninjaMs) * 1.05 + 100) {
                    problems["files: clang longer than the ninja command (a trace of another compile?)"].push_back(obj);
                }
                if (facts.DepsOfFile.back().empty()) {
                    problems["files: no dependencies in .ninja_deps"].push_back(obj);
                } else if (!t.Missing.empty()) {
                    problems["files: headers of the trace not among the dependencies of .ninja_deps"].push_back(
                        obj + " (" + Display(t.Missing) + ")");
                }
            }
            if (end / 2000 != begin / 2000) {
                Log(Format("%zu of %zu", end, traced.size()));
            }
        }
        chunk.clear();

        std::vector<int64_t> parse(facts.HeaderNames.size());
        for (size_t h = 0; h < parse.size(); ++h) {
            parse[h] = std::accumulate(facts.HeaderParse[h].begin(), facts.HeaderParse[h].end(), int64_t(0));
        }
        std::vector<std::string> sorted = facts.HeaderNames;
        std::sort(sorted.begin(), sorted.end());
        for (const std::string& name : sorted) {
            if (!KnownLocation(name, facts.CompilerDir)) {
                problems["headers: neither in the source tree, the build dir, the sysroot nor the compiler"].push_back(name);
            }
        }

        Report(Format("traces:           %zu of %zu read", facts.Objs.size(), traced.size()));
        ReportProblems(problems, false);
        Report(Format("C++ compile:      %s, headers %s (%.0f%%)", Hours(double(execute)).c_str(), Hours(double(outer)).c_str(),
                      100.0 * double(outer) / double(std::max<int64_t>(execute, 1))));
        Report("top headers (parse, files, PCH):");
        std::vector<int32_t> order(parse.size());
        std::iota(order.begin(), order.end(), 0);
        std::sort(order.begin(), order.end(), [&](int32_t a, int32_t b) {
            return parse[a] != parse[b] ? parse[a] > parse[b] : a < b;
        });
        for (size_t k = 0; k < order.size() && k < size_t(TOP_HEADERS); ++k) {
            int32_t h = order[k];
            std::vector<std::string> pchs;
            for (int32_t p : inPch[h]) {
                pchs.push_back(roots.Pch[p]);
            }
            Report(Format("  %-60s %8s %7zu   %s", Display(facts.HeaderNames[h]).c_str(), Hours(double(parse[h])).c_str(),
                          facts.HeaderFiles[h].size(), pchs.empty() ? "-" : Join(pchs, ", ").c_str()));
        }
        return problems.empty();
    }

    // =========================================================================
    // history
    // =========================================================================

    bool ReadHistory(const TOptions& options, TFacts& facts) {
        TProblems problems;
        Log("reading the history of " + options.MainRef + "...");
        TCommits history = CommitsOf(facts, options.MainRef);
        TNinjaDeps deps = ReadNinjaDeps(JoinPath(facts.BuildDir, ".ninja_deps"));
        auto madeOf = GeneratedSources(facts, deps);
        deps = TNinjaDeps();
        std::unordered_map<std::string, std::set<std::string>> bySource;
        for (const auto& [generated, sources] : madeOf) {
            for (const std::string& f : sources) {
                bySource[f].insert(generated);
            }
        }

        std::unordered_map<std::string, int32_t> depId;
        for (size_t k = 0; k < facts.DepNames.size(); ++k) {
            depId.emplace(facts.DepNames[k], int32_t(k));
        }

        // per commit: its files with the generated files made of them
        std::vector<std::set<std::string>> changed;
        std::set<std::string> touched;
        for (const auto& [commit, files] : history.Commits) {
            std::set<std::string> c(files.begin(), files.end());
            for (const std::string& f : files) {
                auto it = bySource.find(f);
                if (it != bySource.end()) {
                    c.insert(it->second.begin(), it->second.end());
                }
            }
            touched.insert(c.begin(), c.end());
            changed.push_back(std::move(c));
        }
        std::vector<char> isTouched(facts.DepNames.size(), 0);
        for (const std::string& f : touched) {
            auto it = depId.find(f);
            if (it != depId.end()) {
                isTouched[it->second] = 1;
            }
        }

        // the files the window changes -> the C++ files depending on them
        std::unordered_map<int32_t, std::vector<int32_t>> rdeps;
        for (int32_t f = 0; f < int32_t(facts.Objs.size()); ++f) {
            for (int32_t k : facts.DepsOfFile[f]) {
                const std::string& name = facts.DepNames[k];
                if (StartsWith(name, GEN) && !madeOf.count(name)) {
                    problems["generated dependencies made by no command of the manifest"].push_back(name);
                }
                if (isTouched[k]) {
                    rdeps[k].push_back(f);
                }
            }
        }
        auto dependentsOf = [&](const std::string& name) -> const std::vector<int32_t>* {
            auto id = depId.find(name);
            if (id == depId.end()) {
                return nullptr;
            }
            auto it = rdeps.find(id->second);
            return it == rdeps.end() ? nullptr : &it->second;
        };

        facts.Commits.clear();
        facts.CommitFiles.clear();
        facts.Rebuilt.clear();
        std::vector<size_t> rebuilt;
        std::vector<double> cost;
        for (size_t k = 0; k < changed.size(); ++k) {
            std::set<int32_t> hit;
            for (const std::string& f : changed[k]) {
                if (const auto* files = dependentsOf(f)) {
                    hit.insert(files->begin(), files->end());
                }
            }
            double us = 0;
            for (int32_t t : hit) {
                us += double(facts.CompileUs[t]);
            }
            facts.Commits.push_back(history.Commits[k].first);
            facts.CommitFiles.emplace_back(changed[k].begin(), changed[k].end());
            facts.Rebuilt.emplace_back(hit.begin(), hit.end());
            rebuilt.push_back(hit.size());
            cost.push_back(us);
        }

        // a changed file with the generated files made of it: the C++ files
        // depending on them, their compiles over its commits
        std::map<std::string, int64_t> commitsOfFile;
        for (const auto& [commit, files] : history.Commits) {
            for (const std::string& f : std::set<std::string>(files.begin(), files.end())) {
                ++commitsOfFile[f];
            }
        }
        struct TDependents {
            size_t Files = 0;
            double Us = 0;
        };
        std::map<std::string, TDependents> dependents;
        std::vector<std::pair<double, std::string>> costOfFile;
        for (const auto& [f, commits] : commitsOfFile) {
            std::set<int32_t> hit;
            if (const auto* files = dependentsOf(f)) {
                hit.insert(files->begin(), files->end());
            }
            auto gens = bySource.find(f);
            if (gens != bySource.end()) {
                for (const std::string& g : gens->second) {
                    if (const auto* files = dependentsOf(g)) {
                        hit.insert(files->begin(), files->end());
                    }
                }
            }
            if (hit.empty()) {
                continue;
            }
            double us = 0;
            for (int32_t t : hit) {
                us += double(facts.CompileUs[t]);
            }
            dependents[f] = {hit.size(), us};
            costOfFile.emplace_back(double(commits) * us, f);
        }
        std::sort(costOfFile.begin(), costOfFile.end(), [](const auto& a, const auto& b) {
            return a.first != b.first ? a.first > b.first : a.second < b.second;
        });
        size_t reaching = 0;
        size_t generated = 0;
        for (const std::string& f : touched) {
            if (dependentsOf(f)) {
                ++reaching;
                generated += StartsWith(f, GEN) ? 1 : 0;
            }
        }

        facts.ReadyOf = ReadyTimes(facts);

        size_t empty = 0;
        for (const auto& [commit, files] : history.Commits) {
            empty += files.empty() ? 1 : 0;
        }
        std::vector<size_t> sortedRebuilt = rebuilt;
        std::sort(sortedRebuilt.begin(), sortedRebuilt.end());
        size_t median = sortedRebuilt.empty() ? 0 : sortedRebuilt[sortedRebuilt.size() / 2];
        double meanRebuilt = double(std::accumulate(rebuilt.begin(), rebuilt.end(), size_t(0))) / double(std::max<size_t>(rebuilt.size(), 1));
        double meanCost = std::accumulate(cost.begin(), cost.end(), 0.0) / double(std::max<size_t>(cost.size(), 1));
        size_t none = size_t(std::count(rebuilt.begin(), rebuilt.end(), size_t(0)));

        Report(Format("history:          %zu commits %s..%s (%s), empty %zu", history.Commits.size(),
                      Day(history.When - HISTORY_WEEKS * 7 * 86400).c_str(), Day(history.When).c_str(),
                      history.MergeBase.substr(0, 11).c_str(), empty));
        ReportProblems(problems, true);
        Report(Format("changed:          %zu files, in C++ deps %zu, generated %zu", touched.size(), reaching, generated));
        Report(Format("rebuilt/commit:   median %zu, mean %.0f (%s), none %zu of %zu", median, meanRebuilt,
                      Hours(meanCost).c_str(), none, history.Commits.size()));
        Report("top changed (commits, dependents, compile):");
        for (size_t k = 0; k < costOfFile.size() && k < size_t(TOP_CHANGED); ++k) {
            const std::string& f = costOfFile[k].second;
            Report(Format("  %-60s %4lld %6zu %9s", f.c_str(), (long long)commitsOfFile[f], dependents[f].Files,
                          Hours(costOfFile[k].first).c_str()));
        }
        return problems.empty();
    }

    // =========================================================================
    // flag classes
    // =========================================================================

    bool FlagClasses(const TOptions& options, TFacts& facts) {
        std::vector<std::string> exclude = EnumExclude(facts);
        size_t n = facts.Objs.size();
        std::vector<TCompileFlags> parsed(n);
        facts.ArgvOf.assign(n, {});
        facts.SourceOf.assign(n, {});
        ParallelFor(n, Threads(), [&](size_t f) {
            int64_t edge = facts.EdgeOf[f];
            if (edge < 0) {
                Fail("no edge of the manifest writes " + facts.Objs[f]);
            }
            const auto& e = facts.Manifest->Edges()[edge];
            if (e.Inputs.empty()) {
                Fail("no source for " + facts.Objs[f]);
            }
            std::vector<std::string> argv = CompilerArgv(facts.Manifest->Command(size_t(edge)), facts.Compiler);
            parsed[f] = CompileFlags(argv, e.Inputs);
            facts.ArgvOf[f] = CheckArgv(argv);
            facts.SourceOf[f] = e.Inputs[0];
        });

        std::map<std::vector<std::string>, int32_t> classOfFlags;
        std::vector<std::vector<std::string>> flagsOf;
        std::vector<std::vector<int32_t>> filesOf;
        std::map<std::string, std::set<int32_t>> targetClasses;
        std::vector<int32_t> firstClass(n);
        facts.TargetOf.assign(n, 0);
        facts.LabelOf.assign(n, 0);
        facts.DefinesOf.assign(n, {});
        facts.IncludeDirsOf.assign(n, {});
        for (size_t f = 0; f < n; ++f) {
            auto label = facts.LabelOfOutput.find(facts.Objs[f]);
            if (label == facts.LabelOfOutput.end()) {
                Fail("no gn label writes " + facts.Objs[f]);
            }
            auto [it, added] = classOfFlags.emplace(parsed[f].Flags, int32_t(flagsOf.size()));
            if (added) {
                flagsOf.push_back(parsed[f].Flags);
                filesOf.emplace_back();
            }
            firstClass[f] = it->second;
            filesOf[it->second].push_back(int32_t(f));
            targetClasses[label->second].insert(it->second);
            facts.LabelOf[f] = facts.Target(label->second);
            facts.TargetOf[f] = facts.Target(PchOwner(label->second, exclude));
            facts.DefinesOf[f] = std::move(parsed[f].Defines);
            facts.IncludeDirsOf[f] = std::move(parsed[f].IncludeDirs);
        }
        parsed.clear();

        // the classes of the most compile first
        std::vector<int64_t> compile(flagsOf.size(), 0);
        std::vector<int64_t> headers(flagsOf.size(), 0);
        for (size_t f = 0; f < n; ++f) {
            compile[firstClass[f]] += facts.CompileUs[f];
            headers[firstClass[f]] += facts.HeaderUs[f];
        }
        std::vector<int32_t> order(flagsOf.size());
        std::iota(order.begin(), order.end(), 0);
        std::stable_sort(order.begin(), order.end(), [&](int32_t a, int32_t b) {
            return compile[a] > compile[b];
        });
        std::vector<int32_t> rank(order.size());
        facts.Classes.clear();
        facts.ClassFlags.clear();
        for (size_t k = 0; k < order.size(); ++k) {
            rank[order[k]] = int32_t(k);
            facts.Classes.push_back(std::move(filesOf[order[k]]));
            facts.ClassFlags.push_back(std::move(flagsOf[order[k]]));
        }
        facts.ClassOf.assign(n, 0);
        for (size_t f = 0; f < n; ++f) {
            facts.ClassOf[f] = rank[firstClass[f]];
        }

        std::vector<std::string> split;
        for (const auto& [target, classes] : targetClasses) {
            if (classes.size() > 1) {
                split.push_back(target);
            }
        }
        Report(Format("flag classes:     %zu of %zu C++ files, targets in several classes %zu%s", facts.Classes.size(), n,
                      split.size(), split.empty() ? "" : (", e.g. " + split[0]).c_str()));
        Report(Format("  %-3s %7s %7s %9s %9s %11s  %s", "#", "targets", "files", "compile", "headers", "-D common", "flags"));
        const std::vector<std::string> noFlags;
        const std::vector<std::string>& first = facts.ClassFlags.empty() ? noFlags : facts.ClassFlags[0];
        size_t restTargets = 0;
        size_t restFiles = 0;
        int64_t restCompile = 0;
        int64_t restHeaders = 0;
        for (size_t k = 0; k < facts.Classes.size(); ++k) {
            const auto& files = facts.Classes[k];
            std::set<int32_t> targets;
            for (int32_t f : files) {
                targets.insert(facts.TargetOf[f]);
            }
            int64_t classCompile = compile[order[k]];
            int64_t classHeaders = headers[order[k]];
            if (k >= size_t(TOP_CLASSES)) {
                restTargets += targets.size();
                restFiles += files.size();
                restCompile += classCompile;
                restHeaders += classHeaders;
                continue;
            }
            std::set<std::string> common(facts.DefinesOf[files[0]].begin(), facts.DefinesOf[files[0]].end());
            std::set<std::string> names;
            for (int32_t f : files) {
                std::set<std::string> own(facts.DefinesOf[f].begin(), facts.DefinesOf[f].end());
                std::set<std::string> both;
                std::set_intersection(common.begin(), common.end(), own.begin(), own.end(), std::inserter(both, both.end()));
                common = std::move(both);
                for (const std::string& d : facts.DefinesOf[f]) {
                    names.insert(d.substr(0, d.find('=')));
                }
            }
            const auto& flags = facts.ClassFlags[k];
            std::vector<std::string> diff;
            for (const std::string& flag : flags) {
                if (std::find(first.begin(), first.end(), flag) == first.end()) {
                    diff.push_back("+" + flag);
                }
            }
            for (const std::string& flag : first) {
                if (std::find(flags.begin(), flags.end(), flag) == flags.end()) {
                    diff.push_back("-" + flag);
                }
            }
            Report(Format("  %-3zu %7zu %7zu %9s %9s %5zu/%-5zu  %s", k + 1, targets.size(), files.size(),
                          Hours(double(classCompile)).c_str(), Hours(double(classHeaders)).c_str(), common.size(), names.size(),
                          diff.empty() ? "-" : Join(diff, " ").c_str()));
        }
        if (facts.Classes.size() > size_t(TOP_CLASSES)) {
            Report(Format("  %-3s %7zu %7zu %9s %9s", "...", restTargets, restFiles, Hours(double(restCompile)).c_str(),
                          Hours(double(restHeaders)).c_str()));
        }
        return split.empty();
    }

    // =========================================================================
    // headers alone
    // =========================================================================

    bool HeaderFacts(const TOptions& options, TWorkDir& work, TFacts& facts) {
        size_t nHeaders = facts.HeaderNames.size();
        std::vector<int32_t> candidates;
        for (size_t h = 0; h < nHeaders; ++h) {
            const auto& parse = facts.HeaderParse[h];
            if (facts.HeaderFiles[h].size() >= 2 && std::accumulate(parse.begin(), parse.end(), int64_t(0)) >= HEADER_MIN_PARSE) {
                candidates.push_back(int32_t(h));
            }
        }

        // each with the flags of a file of its class of the most files parsing it
        std::string srcDir = work.SideDir("src");
        std::vector<std::string> results;
        size_t before = 0;
        for (int32_t h : candidates) {
            const auto& files = facts.HeaderFiles[h];
            std::map<int32_t, size_t> count;
            int32_t best = -1;
            for (int32_t t : files) {
                size_t c = ++count[facts.ClassOf[t]];
                if (best < 0 || c > count[best]) {
                    best = facts.ClassOf[t];
                }
            }
            int32_t file = -1;
            for (int32_t t : files) {
                if (facts.ClassOf[t] == best && (file < 0 || facts.Objs[t] < facts.Objs[file])) {
                    file = t;
                }
            }
            const auto& flags = facts.ClassFlags[best];
            std::vector<std::string> compile = {facts.Compiler};
            compile.insert(compile.end(), flags.begin(), flags.end());
            compile.insert(compile.end(), facts.DefinesOf[file].begin(), facts.DefinesOf[file].end());
            for (const auto& [option, dir] : facts.IncludeDirsOf[file]) {
                compile.push_back(option);
                compile.push_back(dir);
            }
            compile.insert(compile.end(), {"-w", "-ferror-limit=1", "-O0", "-g0"});
            std::vector<std::string> link = {facts.Compiler};
            for (size_t i = 0; i < flags.size(); ++i) {
                if (StartsWith(flags[i], "--sysroot")) {
                    link.push_back(flags[i]);
                    if (flags[i] == "--sysroot" && i + 1 < flags.size()) {
                        link.push_back(flags[++i]);
                    }
                }
            }
            link.insert(link.end(), {"-shared", "-nodefaultlibs", "-fuse-ld=lld", "-Wl,--gc-sections", NJobArgs::OBJ, "-o",
                                     NJobArgs::SO, "-lc", "-lm", "-ldl", "-lpthread", "-lrt"});

            std::string text = "#include " + Spelling(facts, facts.HeaderNames[h]) + "\n";
            std::string source = JoinPath(srcDir, HashHex(text) + ".cpp");
            WriteFileIfChanged(source, text);

            TWorkDir::TJob job;
            job.Kind = "header";
            job.Args = {facts.BuildDir, source};
            job.Args.insert(job.Args.end(), link.begin(), link.end());
            job.Args.push_back(NJobArgs::SEPARATOR);
            job.Args.insert(job.Args.end(), compile.begin(), compile.end());
            job.Inputs = {source};
            results.push_back(work.Add(job));
            before += Exists(results.back()) ? 1 : 0;
        }
        Log(Format("compiling %zu headers alone (%zu done before)...", candidates.size(), before));
        work.Build(results, "headers alone");

        std::vector<THeaderResult> loaded(candidates.size());
        ParallelFor(candidates.size(), Threads(), [&](size_t i) {
            loaded[i] = LoadHeaderResult(results[i]);
        });

        facts.SelfContained.assign(nHeaders, 0);
        facts.NeedsOfHeader.assign(nHeaders, {});
        size_t ok = 0;
        size_t learned = 0;
        size_t linkFailed = 0;
        size_t needing = 0;
        std::string example;
        for (size_t i = 0; i < candidates.size(); ++i) {
            int32_t h = candidates[i];
            THeaderResult& r = loaded[i];
            if (r.Compile.ExitCode == 0) {
                facts.SelfContained[h] = 1;
                ++ok;
                if (r.LinkExit != 0) {
                    ++linkFailed;
                }
                needing += r.Unresolved.empty() ? 0 : 1;
                facts.NeedsOfHeader[h] = std::move(r.Unresolved);
            } else if (example.empty()) {
                std::string error = FirstLine(r.Compile.FirstError());
                if (!error.empty()) {
                    example = Display(facts.HeaderNames[h]) + ": " + error;
                }
            }
            for (const auto& [including, included] : r.Edges) {
                auto a = facts.HeaderId.find(facts.Paths.Normalize(including));
                auto b = facts.HeaderId.find(facts.Paths.Normalize(included));
                if (a == facts.HeaderId.end() || b == facts.HeaderId.end() || a->second == b->second) {
                    continue;
                }
                uint64_t edge = uint64_t(uint32_t(a->second)) << 32 | uint32_t(b->second);
                learned += facts.HeaderEdges.insert(edge).second ? 1 : 0;
            }
        }
        Report(Format("headers alone:    %zu of %zu compile (parsed %s+ in 2+ files): done before %zu, compiled %zu; "
                      "includes learned %zu%s",
                      ok, candidates.size(), Duration(double(HEADER_MIN_PARSE)).c_str(), before, candidates.size() - before,
                      learned, example.empty() ? "" : ("; e.g. " + example).c_str()));
        Report(Format("  linked alone: %zu need symbols of the libraries, %zu fail to link", needing, linkFailed));
        Report(Format("  the rest in a PCH only with a header including them: %zu of %zu", nHeaders - ok, nHeaders));
        return true;
    }

} // namespace NPchUpdate
