#include "elf.h"
#include "ninja.h"
#include "steps.h"

#include <cxxabi.h>

#include <algorithm>
#include <cstdlib>
#include <functional>
#include <iterator>
#include <map>
#include <mutex>
#include <optional>
#include <unordered_map>
#include <unordered_set>

namespace NPchUpdate {

    namespace {

        // Share of the targets with a PCH the checks may leave: fewer, and
        // nothing is written.
        constexpr double MIN_KEPT = 0.5;
        constexpr int TOP_FAILURES = 5;
        // A set of macros changing -E is tried again as these parts.
        constexpr int MACRO_PARTS = 4;
        constexpr size_t MESSAGE_CUT = 300;

        const std::vector<std::string> LINK_FLAGS = {"-shared", "-nodefaultlibs", "-fuse-ld=lld", "-Wl,--gc-sections"};
        const std::vector<std::string> SYSTEM_LIBS = {"-lc", "-lm", "-ldl", "-lpthread", "-lrt"};

        // gn's tools: the objects and the links of the baseline.
        bool IsCompileRule(const std::string& rule) {
            return rule == "cc" || rule == "cxx" || rule == "asm";
        }

        bool IsLinkRule(const std::string& rule) {
            return rule == "link" || rule == "solink";
        }

        using THeaders = std::set<int32_t>;
        using TSignature = std::pair<std::vector<int32_t>, std::vector<int32_t>>;

        // What the checks of a round find for a group; headers as ids.
        struct TRoundResult {
            // header -> the error of the PCH on it
            std::map<int32_t, std::string> Errors;
            // macro -> headers depending on it
            std::map<std::string, THeaders> Macros;
            // target -> (header or -1, message): files of it failing with the PCH, its links
            std::map<int32_t, std::vector<std::pair<int32_t, std::string>>> Conflicts;
            // macro -> targets having it
            std::map<std::string, std::set<int32_t>> Targets;
            int64_t Missing = 0;
            int64_t Extra = 0;
            std::string Failed;
            std::string FailedKind;
            // includes -E shows, as -E names them
            std::vector<std::pair<std::string, std::string>> Edges;
            std::map<std::string, int64_t> Files;

            void Fail(const std::string& kind, const std::string& message) {
                if (Failed.empty()) {
                    Failed = message.substr(0, MESSAGE_CUT);
                    FailedKind = kind;
                }
            }
        };

        std::string LastLine(std::string_view text) {
            while (!text.empty() && (text.back() == '\n' || text.back() == ' ')) {
                text.remove_suffix(1);
            }
            size_t line = text.rfind('\n');
            return std::string(line == std::string_view::npos ? text : text.substr(line + 1));
        }

        std::string Demangled(const std::string& symbol) {
            int status = 0;
            char* text = abi::__cxa_demangle(symbol.c_str(), nullptr, nullptr, &status);
            if (status != 0 || !text) {
                return symbol;
            }
            std::string out = text;
            free(text);
            return out;
        }

        // The flags of a class as arguments: "-x <language>" is one flag there.
        std::vector<std::string> ClassArgv(const std::vector<std::string>& flags) {
            std::vector<std::string> out;
            for (const std::string& f : flags) {
                if (StartsWith(f, "-x ")) {
                    out.push_back("-x");
                    out.push_back(f.substr(3));
                } else {
                    out.push_back(f);
                }
            }
            return out;
        }

        // [(name, -D argument)] of the #define of a source's text before its
        // last #include, its #if not looked at: a superset of what the
        // preprocessor defines there, telling which sources need -E -dD.
        std::vector<std::pair<std::string, std::string>> ScannedDefines(const std::string& path) {
            std::string text;
            if (!TryReadFile(path, &text)) {
                return {};
            }
            std::string joined;
            joined.reserve(text.size());
            for (size_t i = 0; i < text.size(); ++i) {
                if (text[i] == '\\' && i + 1 < text.size() && text[i + 1] == '\n') {
                    joined += ' ';
                    ++i;
                } else {
                    joined += text[i];
                }
            }
            auto isSpace = [](char c) {
                return c == ' ' || c == '\t' || c == '\r' || c == '\f' || c == '\v';
            };
            auto isIdent = [](char c) {
                return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || c == '_' || (c >= '0' && c <= '9');
            };
            std::vector<std::pair<std::string, std::string>> found;
            int64_t last = -1;
            for (std::string_view line : Split(joined, '\n')) {
                size_t i = 0;
                while (i < line.size() && isSpace(line[i])) {
                    ++i;
                }
                if (i >= line.size() || line[i] != '#') {
                    continue;
                }
                ++i;
                while (i < line.size() && isSpace(line[i])) {
                    ++i;
                }
                size_t word = i;
                while (i < line.size() && isIdent(line[i])) {
                    ++i;
                }
                std::string_view directive = line.substr(word, i - word);
                if (directive != "define" && directive != "include" && directive != "import") {
                    continue;
                }
                if (directive != "define") {
                    last = int64_t(found.size());
                    continue;
                }
                while (i < line.size() && isSpace(line[i])) {
                    ++i;
                }
                if (i >= line.size() || !(isIdent(line[i]) && !(line[i] >= '0' && line[i] <= '9'))) {
                    continue;
                }
                size_t name = i;
                while (i < line.size() && isIdent(line[i])) {
                    ++i;
                }
                std::string macro(line.substr(name, i - name));
                std::string params;
                if (i < line.size() && line[i] == '(') {
                    size_t close = line.find(')', i);
                    if (close == std::string_view::npos) {
                        continue;
                    }
                    params = line.substr(i, close + 1 - i);
                    i = close + 1;
                }
                std::string_view body = line.substr(i);
                body = body.substr(0, body.find("//"));
                while (!body.empty() && isSpace(body.front())) {
                    body.remove_prefix(1);
                }
                while (!body.empty() && isSpace(body.back())) {
                    body.remove_suffix(1);
                }
                found.emplace_back(macro, "-D" + macro + params + "=" + std::string(body));
            }
            if (last <= 0) {
                return {};
            }
            found.resize(last);
            return found;
        }

        // A set of macros -E of a probe runs with: the files they change, its
        // error, what its parts explain.
        struct TMacroSet {
            int64_t Group = 0;
            std::vector<std::string> Macros;
            int64_t Parent = -1;
            std::string Result;
            int Pending = 0;
            std::set<std::string> Diff;
            std::optional<std::pair<std::string, std::string>> Error;
            std::set<std::string> Explained;
            bool ErrorExplained = false;
        };

        // The search of the root of the PCH breaking a file (or the PCH):
        // the PCH of the first `Lo` roots passes, of the first `Hi` fails.
        struct TBisect {
            int64_t Group = 0;
            // the file, -1 for the PCH itself
            int32_t File = -1;
            int32_t Target = -1;
            std::string Message;
            size_t Lo = 0;
            size_t Hi = 0;
            // prefix -> the result telling it
            std::vector<std::pair<size_t, std::string>> Tried;
        };

        // What the checks compile for a group: a header including the roots
        // of its PCH, with the flags of its class, the -D all its files have
        // and the include dirs of its files.
        struct TProbe {
            int64_t Id = 0;
            int32_t Class = 0;
            const TSelection* Sel = nullptr;
            std::vector<int32_t> Headers;
            THeaders HeaderSet;
            TGroupInputs In;
            std::string Header;
            std::string Source;
            std::vector<std::string> Common;
            std::vector<std::string> PchArgv;
            std::vector<std::string> LinkArgv;
            std::string Pch;
            std::string PchResult;
            std::string E0;
            std::string Link;
            std::map<int32_t, std::string> Tu;
            // macro -> its -D argument of the most files; -> the targets having it
            std::map<std::string, std::string> Macros;
            std::map<std::string, std::set<int32_t>> Having;
            // prefix of the roots -> (its PCH, its pch result)
            std::map<size_t, std::pair<std::string, std::string>> Prefix;
            std::vector<std::string> SideFiles;
        };

        class TChecker {
        public:
            TChecker(const TOptions& options, TWorkDir& work, TFacts& facts)
                : Options_(options)
                , Work_(work)
                , Facts_(facts)
                , Root_({"-isysroot", RelPath(facts.Root, facts.BuildDir) + "/"})
                , PchOnly_(PchOnlyArgv(facts))
            {
            }

            std::map<int64_t, TRoundResult> Round(const std::map<int64_t, std::pair<int32_t, const TSelection*>>& groups);

        private:
            std::string Normalize(std::string_view name) const {
                return Facts_.Paths.Normalize(name);
            }

            int32_t HeaderOf(std::string_view name) const {
                auto it = Facts_.HeaderId.find(Normalize(name));
                return it == Facts_.HeaderId.end() ? -1 : it->second;
            }

            std::string Where(const TDiagnostic& d) const {
                if (d.File.empty()) {
                    return d.Message;
                }
                return Format("%s:%u: %s", Display(Normalize(d.File)).c_str(), d.Line, d.Message.c_str());
            }

            // A file the jobs read, named by its content.
            std::string WriteSource(const std::string& text, const std::string& ext) {
                std::string path = JoinPath(Work_.SideDir("src"), HashHex(text) + ext);
                WriteFileIfChanged(path, text);
                return path;
            }

            std::string IncludeText(const std::vector<int32_t>& roots, size_t n) const {
                std::string text;
                for (size_t i = 0; i < n; ++i) {
                    text += "#include " + Spelling(Facts_, Facts_.HeaderNames[roots[i]]) + "\n";
                }
                return text;
            }

            // The pch job of a header: (the PCH, the result).
            std::pair<std::string, std::string> AddPch(TProbe& p, const std::string& header) {
                THasher hasher;
                hasher.Field(header);
                for (const std::string& a : p.PchArgv) {
                    hasher.Field(a);
                }
                std::string pch = JoinPath(Work_.SideDir("pch"), hasher.Hex() + ".pch");
                TWorkDir::TJob job;
                job.Kind = "pch";
                job.Args = {Facts_.BuildDir, pch, header, NJobArgs::SEPARATOR};
                job.Args.insert(job.Args.end(), p.PchArgv.begin(), p.PchArgv.end());
                job.Inputs = {header};
                job.Pool = "heavy";
                job.Restat = true;
                p.SideFiles.push_back(pch);
                return {pch, Work_.Add(job)};
            }

            std::string AddTu(int32_t t, const std::string& pch, const std::string& pchResult) {
                TWorkDir::TJob job;
                job.Kind = "tu";
                job.Args = {Facts_.BuildDir, pchResult, NJobArgs::SEPARATOR};
                job.Args.insert(job.Args.end(), Facts_.ArgvOf[t].begin(), Facts_.ArgvOf[t].end());
                job.Args.insert(job.Args.end(), {"-include-pch", pch});
                job.Args.insert(job.Args.end(), Root_.begin(), Root_.end());
                job.Args.insert(job.Args.end(), {"-fsyntax-only", "-ferror-limit=1"});
                job.Inputs = {pchResult};
                return Work_.Add(job);
            }

            std::string AddPreprocess(const TProbe& p, const std::vector<std::string>& macros) {
                TWorkDir::TJob job;
                job.Kind = "pp";
                job.Args = {Facts_.BuildDir, NJobArgs::SEPARATOR};
                job.Args.insert(job.Args.end(), p.Common.begin(), p.Common.end());
                for (const std::string& c : macros) {
                    job.Args.push_back(p.Macros.at(c));
                }
                job.Args.insert(job.Args.end(), {"-E", p.Source});
                job.Inputs = {p.Source};
                return Work_.Add(job);
            }

            std::string AddLink(const TProbe& p) {
                TWorkDir::TJob job;
                job.Kind = "link";
                job.Args = {Facts_.BuildDir, p.PchResult};
                job.Args.insert(job.Args.end(), p.LinkArgv.begin(), p.LinkArgv.end());
                job.Args.push_back(NJobArgs::SEPARATOR);
                job.Args.insert(job.Args.end(), p.Common.begin(), p.Common.end());
                job.Args.insert(job.Args.end(), {"-include-pch", p.Pch});
                job.Args.insert(job.Args.end(), Root_.begin(), Root_.end());
                job.Inputs = {p.PchResult};
                return Work_.Add(job);
            }

            TProbe MakeProbe(int64_t id, int32_t n, const TSelection* sel);
            void SourceDefines(const std::vector<TProbe*>& probes);
            void MacrosOf(TProbe& p);
            void RunChunk(std::vector<TProbe*>& chunk, std::map<int64_t, TRoundResult>& out);
            void PchErrors(const TProbe& p, const TPchResult& pch, TRoundResult& r, std::vector<TBisect>& bisects);
            void StepBisect(TProbe& p, TBisect& b, std::vector<std::string>& wave);
            void LinkConflicts(const std::vector<TProbe*>& probes, std::map<int64_t, TRoundResult>& out);
            const std::map<std::string, std::set<std::string>>& Definers(const std::set<std::string>& symbols);
            const std::unordered_map<std::string, std::vector<std::set<std::string>>>& LinkLines();

            const TOptions& Options_;
            TWorkDir& Work_;
            TFacts& Facts_;
            std::vector<std::string> Root_;
            std::vector<std::string> PchOnly_;
            // source -> its #define before its last #include; kept for the run
            std::unordered_map<std::string, std::vector<std::pair<std::string, std::string>>> DefinesIn_;
            std::map<std::string, std::set<std::string>> Definers_;
            std::optional<std::unordered_map<std::string, std::vector<std::set<std::string>>>> LinkLines_;
            std::vector<std::pair<std::string, std::string>> Libraries_;
        };

        TProbe TChecker::MakeProbe(int64_t id, int32_t n, const TSelection* sel) {
            TProbe p;
            p.Id = id;
            p.Class = n;
            p.Sel = sel;
            p.Headers = SelectedHeaders(Facts_, *sel);
            p.HeaderSet = {p.Headers.begin(), p.Headers.end()};
            p.In = GroupInputs(Facts_, *sel);
            std::string text = IncludeText(p.In.Roots, p.In.Roots.size());
            p.Header = WriteSource(text, ".h");
            p.Source = WriteSource(text, ".cpp");
            std::vector<std::string> flags = ClassArgv(Facts_.ClassFlags[n]);
            p.Common = {Facts_.Compiler};
            p.Common.insert(p.Common.end(), flags.begin(), flags.end());
            p.Common.insert(p.Common.end(), p.In.Defines.begin(), p.In.Defines.end());
            for (const auto& [opt, dir] : p.In.IncludeDirs) {
                p.Common.push_back(opt);
                p.Common.push_back(dir);
            }
            p.Common.push_back("-w");
            p.PchArgv = p.Common;
            p.PchArgv.insert(p.PchArgv.end(), PchOnly_.begin(), PchOnly_.end());
            p.PchArgv.insert(p.PchArgv.end(), Root_.begin(), Root_.end());
            p.PchArgv.push_back("-ferror-limit=0");
            p.LinkArgv = {Facts_.Compiler};
            for (const std::string& f : flags) {
                if (StartsWith(f, "--sysroot")) {
                    p.LinkArgv.push_back(f);
                }
            }
            p.LinkArgv.insert(p.LinkArgv.end(), LINK_FLAGS.begin(), LINK_FLAGS.end());
            p.LinkArgv.insert(p.LinkArgv.end(), {NJobArgs::OBJ, "-o", NJobArgs::SO});
            p.LinkArgv.insert(p.LinkArgv.end(), SYSTEM_LIBS.begin(), SYSTEM_LIBS.end());
            return p;
        }

        void TChecker::SourceDefines(const std::vector<TProbe*>& probes) {
            std::vector<int32_t> todo;
            std::set<std::string> seen;
            for (const TProbe* p : probes) {
                for (int32_t t : p->In.Files) {
                    const std::string& src = Facts_.SourceOf[t];
                    if (!DefinesIn_.count(src) && seen.insert(src).second) {
                        todo.push_back(t);
                    }
                }
            }
            std::vector<std::vector<std::pair<std::string, std::string>>> scanned(todo.size());
            ParallelFor(todo.size(), 8, [&](size_t i) {
                const std::string& src = Facts_.SourceOf[todo[i]];
                scanned[i] = ScannedDefines(IsAbs(src) ? src : JoinPath(Facts_.BuildDir, src));
            });
            std::vector<std::pair<size_t, std::string>> jobs;
            for (size_t i = 0; i < todo.size(); ++i) {
                const std::string& src = Facts_.SourceOf[todo[i]];
                if (scanned[i].empty()) {
                    DefinesIn_[src] = {};
                    continue;
                }
                TWorkDir::TJob job;
                job.Kind = "defines";
                job.Args = {Facts_.BuildDir, NJobArgs::SEPARATOR};
                job.Args.insert(job.Args.end(), Facts_.ArgvOf[todo[i]].begin(), Facts_.ArgvOf[todo[i]].end());
                jobs.emplace_back(i, Work_.Add(job));
            }
            if (jobs.empty()) {
                return;
            }
            std::vector<std::string> results;
            for (const auto& [_, result] : jobs) {
                results.push_back(result);
            }
            Work_.Build(results, "#define before #include");
            size_t failed = 0;
            std::string example;
            for (const auto& [i, result] : jobs) {
                const std::string& src = Facts_.SourceOf[todo[i]];
                TDefinesResult d = LoadDefinesResult(result);
                std::set<std::string> names;
                for (const auto& [name, _] : scanned[i]) {
                    names.insert(name);
                }
                auto& kept = DefinesIn_[src];
                kept.clear();
                if (d.ExitCode != 0) {
                    if (!failed++) {
                        example = src;
                    }
                    continue;
                }
                for (auto& [name, arg] : d.Defines) {
                    if (names.count(name)) {
                        kept.emplace_back(name, arg);
                    }
                }
            }
            Log(Format("checks: #define before #include in %zu sources, -E -dD of them%s", jobs.size(),
                       failed ? Format(", failed for %zu, e.g. %s", failed, example.c_str()).c_str() : ""));
        }

        void TChecker::MacrosOf(TProbe& p) {
            std::set<std::string> commonNames;
            std::set<std::string> common(p.In.Defines.begin(), p.In.Defines.end());
            for (const std::string& d : p.In.Defines) {
                commonNames.insert(d.substr(2, d.find('=') == std::string::npos ? std::string::npos : d.find('=') - 2));
            }
            // name -> (-D argument -> files, in the order first seen)
            std::map<std::string, std::vector<std::pair<std::string, int64_t>>> values;
            auto count = [&](const std::string& name, const std::string& arg, int32_t t) {
                auto& v = values[name];
                auto it = std::find_if(v.begin(), v.end(), [&](const auto& x) { return x.first == arg; });
                if (it == v.end()) {
                    v.emplace_back(arg, 1);
                } else {
                    ++it->second;
                }
                p.Having[name].insert(Facts_.TargetOf[t]);
            };
            for (int32_t t : p.In.Files) {
                for (const std::string& d : Facts_.DefinesOf[t]) {
                    if (StartsWith(d, "-D") && !common.count(d)) {
                        size_t eq = d.find('=');
                        count(d.substr(2, eq == std::string::npos ? std::string::npos : eq - 2), d, t);
                    }
                }
                auto it = DefinesIn_.find(Facts_.SourceOf[t]);
                if (it != DefinesIn_.end()) {
                    for (const auto& [name, arg] : it->second) {
                        if (!commonNames.count(name)) {
                            count(name, arg, t);
                        }
                    }
                }
            }
            for (const auto& [name, v] : values) {
                auto best = v.begin();
                for (auto it = v.begin(); it != v.end(); ++it) {
                    if (it->second > best->second) {
                        best = it;
                    }
                }
                p.Macros[name] = best->first;
            }
        }

        void TChecker::PchErrors(const TProbe& p, const TPchResult& pch, TRoundResult& r, std::vector<TBisect>& bisects) {
            // an error on the root including it (the include stack has the
            // probe), else on the header it is in; else the roots are searched
            const auto& roots = p.In.Roots;
            auto rootAt = [&](const TDiagnostic& d) -> int32_t {
                if (NormPath(d.File) == p.Header && d.Line >= 1 && d.Line <= roots.size()) {
                    return roots[d.Line - 1];
                }
                return -1;
            };
            for (const TDiagnostic& d : pch.Compile.Errors) {
                int32_t root = rootAt(d);
                for (size_t i = 0; root < 0 && i < d.Notes.size(); ++i) {
                    root = rootAt(d.Notes[i]);
                }
                if (root < 0) {
                    root = HeaderOf(d.File);
                }
                if (root >= 0) {
                    r.Errors.emplace(root, Where(d));
                }
            }
            if (r.Errors.empty()) {
                std::string message = pch.Compile.FirstError();
                if (roots.size() == 1) {
                    r.Errors.emplace(roots[0], message);
                    return;
                }
                TBisect b;
                b.Group = p.Id;
                b.Message = message;
                b.Lo = 0;
                b.Hi = roots.size();
                bisects.push_back(std::move(b));
            }
        }

        void TChecker::StepBisect(TProbe& p, TBisect& b, std::vector<std::string>& wave) {
            // the prefixes tried: the PCH of each, the file with it
            size_t ways = std::min<size_t>(std::max(Options_.BisectWays, 1), b.Hi - b.Lo - 1);
            b.Tried.clear();
            for (size_t i = 1; i <= ways; ++i) {
                size_t n = b.Lo + (b.Hi - b.Lo) * i / (ways + 1);
                if (n <= b.Lo || n >= b.Hi || (!b.Tried.empty() && b.Tried.back().first == n)) {
                    continue;
                }
                auto it = p.Prefix.find(n);
                if (it == p.Prefix.end()) {
                    std::string header = WriteSource(IncludeText(p.In.Roots, n), ".h");
                    it = p.Prefix.emplace(n, AddPch(p, header)).first;
                }
                const auto& [pch, pchResult] = it->second;
                std::string result = b.File < 0 ? pchResult : AddTu(b.File, pch, pchResult);
                b.Tried.emplace_back(n, result);
                wave.push_back(result);
            }
        }

        void TChecker::RunChunk(std::vector<TProbe*>& chunk, std::map<int64_t, TRoundResult>& out) {
            std::map<int64_t, TProbe*> byId;
            std::vector<std::string> wave;
            std::vector<TMacroSet> sets;
            std::map<int64_t, std::map<std::string, std::pair<std::set<std::string>, std::vector<std::pair<std::string, std::string>>>>> found;
            std::vector<TBisect> bisects;
            std::vector<size_t> pendingSets;

            auto trySet = [&](int64_t g, std::vector<std::string> macros, int64_t parent) {
                TMacroSet s;
                s.Group = g;
                s.Macros = std::move(macros);
                s.Parent = parent;
                s.Result = AddPreprocess(*byId[g], s.Macros);
                wave.push_back(s.Result);
                sets.push_back(std::move(s));
                pendingSets.push_back(sets.size() - 1);
            };
            std::function<void(size_t)> finish = [&](size_t i) {
                TMacroSet& s = sets[i];
                if (s.Macros.size() == 1 || !s.Pending) {
                    for (const std::string& c : s.Macros) {
                        auto& [files, errors] = found[s.Group][c];
                        for (const std::string& f : s.Diff) {
                            if (!s.Explained.count(f)) {
                                files.insert(f);
                            }
                        }
                        if (s.Error && !s.ErrorExplained) {
                            errors.push_back(*s.Error);
                        }
                    }
                }
                if (s.Parent >= 0) {
                    TMacroSet& q = sets[s.Parent];
                    q.Explained.insert(s.Diff.begin(), s.Diff.end());
                    q.ErrorExplained |= bool(s.Error);
                    if (!--q.Pending) {
                        finish(s.Parent);
                    }
                }
            };

            // the first wave: all of each group at once
            for (TProbe* p : chunk) {
                byId[p->Id] = p;
                out[p->Id];
                std::tie(p->Pch, p->PchResult) = AddPch(*p, p->Header);
                p->E0 = AddPreprocess(*p, {});
                p->Link = AddLink(*p);
                wave.insert(wave.end(), {p->PchResult, p->E0, p->Link});
                for (int32_t t : p->In.Files) {
                    p->Tu[t] = AddTu(t, p->Pch, p->PchResult);
                    wave.push_back(p->Tu[t]);
                }
                if (!p->Macros.empty()) {
                    std::vector<std::string> all;
                    for (const auto& [name, _] : p->Macros) {
                        all.push_back(name);
                    }
                    trySet(p->Id, all, -1);
                }
            }
            Work_.Build(wave, Format("checks of %zu groups", chunk.size()));

            std::map<int64_t, TPreprocessResult> e0;
            std::set<int64_t> pchFailed;
            for (TProbe* p : chunk) {
                TRoundResult& r = out[p->Id];
                TPchResult pch = LoadPchResult(p->PchResult);
                if (pch.Compile.ExitCode != 0) {
                    pchFailed.insert(p->Id);
                    PchErrors(*p, pch, r, bisects);
                    continue;
                }
                // the headers the PCH reads: the ones selected?
                std::set<std::string> reached;
                for (const std::string& dep : pch.Compile.Deps) {
                    reached.insert(Normalize(dep));
                }
                std::set<std::string> inPch;
                for (int32_t h : p->Headers) {
                    inPch.insert(Facts_.HeaderNames[h]);
                }
                for (const std::string& name : inPch) {
                    r.Missing += !reached.count(name);
                }
                std::string self = Normalize(p->Header);
                for (const std::string& name : reached) {
                    r.Extra += !inPch.count(name) && name != self;
                }
                TPreprocessResult& d0 = e0[p->Id] = LoadPreprocessResult(p->E0);
                if (d0.Compile.ExitCode != 0) {
                    r.Fail("-E failed", "-E: " + d0.Compile.FirstError());
                } else {
                    r.Edges = d0.Edges;
                }

                // a file failing with the PCH: on the header of the PCH its
                // first error is in, or a note of it is in (not one the file
                // includes alone: a "did you mean" there names what it means);
                // else the roots are searched, one file of a target
                std::set<int32_t> searched;
                auto& fc = r.Files;
                fc["total"] += int64_t(p->In.Files.size());
                for (int32_t t : p->In.Files) {
                    TCompileResult tu = LoadCompileResult(p->Tu[t]);
                    ++fc["checked"];
                    if (tu.ExitCode == 0) {
                        continue;
                    }
                    ++fc["failing"];
                    int32_t target = Facts_.TargetOf[t];
                    std::string message = Display(Normalize(Facts_.SourceOf[t])) + ": " + tu.FirstError();
                    int32_t h = -1;
                    bool withPch = false;
                    if (!tu.Errors.empty()) {
                        const TDiagnostic& d = tu.Errors[0];
                        withPch = d.Category == "AST Deserialization Issue";
                        int32_t at = HeaderOf(d.File);
                        if (at >= 0 && p->HeaderSet.count(at)) {
                            h = at;
                        }
                        for (size_t i = 0; h < 0 && i < d.Notes.size(); ++i) {
                            int32_t nh = HeaderOf(d.Notes[i].File);
                            const auto& files = nh >= 0 ? Facts_.HeaderFiles[nh] : std::vector<int32_t>();
                            if (nh >= 0 && p->HeaderSet.count(nh) && !std::binary_search(files.begin(), files.end(), t)) {
                                h = nh;
                            }
                        }
                    }
                    if (withPch) {
                        r.Fail("a file with the PCH: AST Deserialization Issue",
                               Facts_.TargetNames[target] + " with the PCH: " + message);
                        continue;
                    }
                    if (h >= 0) {
                        ++fc["on headers"];
                        r.Conflicts[target].emplace_back(h, message);
                    } else if (searched.insert(target).second) {
                        ++fc["searched"];
                        TBisect b;
                        b.Group = p->Id;
                        b.File = t;
                        b.Target = target;
                        b.Message = message;
                        b.Lo = 0;
                        b.Hi = p->In.Roots.size();
                        bisects.push_back(std::move(b));
                    } else {
                        ++fc["searched with another file"];
                    }
                }
            }

            // the macro sets and the searches, a wave of each step of them
            auto doneSet = [&](size_t i) {
                TMacroSet& s = sets[i];
                TPreprocessResult res = LoadPreprocessResult(s.Result);
                const auto& d0 = e0[s.Group].Digests;
                if (res.Compile.ExitCode != 0) {
                    if (!res.Compile.Errors.empty()) {
                        const TDiagnostic& d = res.Compile.Errors[0];
                        s.Error = std::make_pair(d.File.empty() ? std::string() : Normalize(d.File), Where(d));
                    } else {
                        s.Error = std::make_pair(std::string(), res.Compile.FirstError());
                    }
                } else {
                    for (const auto& [f, digest] : d0) {
                        auto it = res.Digests.find(f);
                        if (it == res.Digests.end() || it->second != digest) {
                            s.Diff.insert(f);
                        }
                    }
                    for (const auto& [f, _] : res.Digests) {
                        if (!d0.count(f)) {
                            s.Diff.insert(f);
                        }
                    }
                }
                if ((!s.Diff.empty() || s.Error) && s.Macros.size() > 1) {
                    size_t k = std::min<size_t>(MACRO_PARTS, s.Macros.size());
                    s.Pending = int(k);
                    std::vector<std::string> macros = s.Macros;
                    for (size_t j = 0; j < k; ++j) {
                        trySet(s.Group, {macros.begin() + j * macros.size() / k, macros.begin() + (j + 1) * macros.size() / k},
                               int64_t(i));
                    }
                } else {
                    finish(i);
                }
            };
            for (TBisect& b : bisects) {
                if (b.Hi - b.Lo > 1) {
                    StepBisect(*byId[b.Group], b, wave);
                }
            }
            while (true) {
                std::vector<size_t> ready;
                ready.swap(pendingSets);
                wave.clear();
                for (size_t i : ready) {
                    const TMacroSet& s = sets[i];
                    TRoundResult& r = out[s.Group];
                    if (pchFailed.count(s.Group) || e0[s.Group].Compile.ExitCode != 0 || !r.Failed.empty()) {
                        continue;
                    }
                    doneSet(i);
                }
                // the results of a step of the searches: the first prefix failing
                for (TBisect& b : bisects) {
                    if (b.Tried.empty()) {
                        continue;
                    }
                    size_t lo = b.Lo;
                    size_t hi = b.Hi;
                    for (const auto& [n, result] : b.Tried) {
                        bool fails = b.File < 0 ? LoadPchResult(result).Compile.ExitCode != 0
                                                : LoadCompileResult(result).ExitCode != 0;
                        if (fails) {
                            hi = n;
                            break;
                        }
                        lo = n;
                    }
                    b.Lo = lo;
                    b.Hi = hi;
                    b.Tried.clear();
                    if (b.Hi - b.Lo > 1) {
                        StepBisect(*byId[b.Group], b, wave);
                    }
                }
                // the sets made by doneSet() are in `wave` already
                if (wave.empty()) {
                    break;
                }
                Work_.Build(wave, Format("checks of %zu groups: macros and searches", chunk.size()));
            }
            for (const TBisect& b : bisects) {
                TProbe& p = *byId[b.Group];
                TRoundResult& r = out[b.Group];
                int32_t culprit = p.In.Roots[b.Hi - 1];
                if (b.File < 0) {
                    r.Errors.emplace(culprit, b.Message);
                } else {
                    ++r.Files["on headers searched"];
                    r.Conflicts[b.Target].emplace_back(culprit, b.Message);
                }
            }
            for (TProbe* p : chunk) {
                TRoundResult& r = out[p->Id];
                if (pchFailed.count(p->Id) || e0[p->Id].Compile.ExitCode != 0) {
                    continue;
                }
                for (const auto& [c, fe] : found[p->Id]) {
                    const auto& [files, errors] = fe;
                    for (const std::string& f : files) {
                        int32_t h = HeaderOf(f);
                        // else a header no traced file parses: its includer differs too
                        if (h >= 0) {
                            r.Macros[c].insert(h);
                        }
                    }
                    for (const auto& [name, line] : errors) {
                        int32_t h = name.empty() ? -1 : HeaderOf(name);
                        if (h >= 0) {
                            r.Macros[c].insert(h);
                        } else {
                            r.Fail("-E with a macro failed", "-E with " + p->Macros[c] + ": " + line);
                        }
                    }
                }
                r.Targets = p->Having;
            }
        }

        const std::unordered_map<std::string, std::vector<std::set<std::string>>>& TChecker::LinkLines() {
            if (LinkLines_) {
                return *LinkLines_;
            }
            LinkLines_.emplace();
            const auto& manifest = *Facts_.Manifest;
            auto labelOf = [&](const std::string& path) -> const std::string* {
                auto it = Facts_.LabelOfOutput.find(path);
                return it == Facts_.LabelOfOutput.end() ? nullptr : &it->second;
            };
            std::set<std::pair<std::string, std::string>> libraries;
            for (const auto& e : manifest.Edges()) {
                if (e.Rule == "alink" || e.Rule == "solink") {
                    if (const std::string* label = labelOf(e.Outputs[0])) {
                        libraries.emplace(e.Outputs[0], *label);
                    }
                }
                if (!IsLinkRule(e.Rule)) {
                    continue;
                }
                const std::string* own = labelOf(e.Outputs[0]);
                std::set<std::string> line;
                std::set<std::string> objects;
                if (own) {
                    line.insert(*own);
                    objects.insert(*own);
                }
                for (const auto* inputs : {&e.Inputs, &e.ImplicitInputs}) {
                    for (const std::string& input : *inputs) {
                        const std::string* label = labelOf(input);
                        if (!label) {
                            continue;
                        }
                        line.insert(*label);
                        int64_t producer = manifest.Producer(input);
                        if (producer >= 0 && IsCompileRule(manifest.Edges()[producer].Rule)) {
                            objects.insert(*label);
                            // an object linked as is: a library of its own
                            libraries.emplace(input, *label);
                        }
                    }
                }
                for (const std::string& t : objects) {
                    (*LinkLines_)[t].push_back(line);
                }
            }
            Libraries_.assign(libraries.begin(), libraries.end());
            return *LinkLines_;
        }

        const std::map<std::string, std::set<std::string>>& TChecker::Definers(const std::set<std::string>& symbols) {
            std::set<std::string> fresh;
            for (const std::string& s : symbols) {
                if (!Definers_.count(s)) {
                    fresh.insert(s);
                    Definers_[s];
                }
            }
            if (fresh.empty()) {
                return Definers_;
            }
            LinkLines();
            std::vector<std::string> results;
            for (const auto& [path, _] : Libraries_) {
                TWorkDir::TJob job;
                job.Kind = "syms";
                std::string abs = JoinPath(Facts_.BuildDir, path);
                job.Args = {abs};
                job.Inputs = {abs};
                job.Depfile = false;
                results.push_back(Work_.Add(job));
            }
            Work_.Build(results, "symbols of the libraries");
            std::mutex lock;
            ParallelFor(results.size(), 8, [&](size_t i) {
                std::vector<std::string> defined = LoadSymbolsResult(results[i]);
                std::vector<const std::string*> hits;
                for (const std::string& s : defined) {
                    auto it = fresh.find(s);
                    if (it != fresh.end()) {
                        hits.push_back(&*it);
                    }
                }
                std::lock_guard<std::mutex> guard(lock);
                for (const std::string* s : hits) {
                    Definers_[*s].insert(Libraries_[i].second);
                }
            });
            Log(Format("checks: %zu libraries read for %zu symbols", results.size(), fresh.size()));
            return Definers_;
        }

        // The targets of a group having a link that lacks a library the PCH
        // needs: the empty file with the PCH, linked alone, needs what its
        // static initializers need. Put on the headers of the PCH whose own
        // link alone needs it, the innermost of them; else on none.
        void TChecker::LinkConflicts(const std::vector<TProbe*>& probes, std::map<int64_t, TRoundResult>& out) {
            std::map<int64_t, std::vector<std::string>> needs;
            std::set<std::string> all;
            for (const TProbe* p : probes) {
                TRoundResult& r = out[p->Id];
                if (!r.Failed.empty() || p->Link.empty()) {
                    continue;
                }
                TLinkResult link = LoadLinkResult(p->Link);
                if (link.Compile.ExitCode == -1) {
                    continue;                           // the PCH failed: its errors tell
                }
                if (link.Compile.ExitCode != 0) {
                    r.Fail("the empty file with the PCH failed", "the empty file with the PCH: " + link.Compile.FirstError());
                    continue;
                }
                if (link.LinkExit != 0) {
                    r.Fail("the link of the empty file with the PCH failed",
                           "the link of the empty file with the PCH: " + LastLine(link.LinkError));
                    continue;
                }
                needs[p->Id] = link.Unresolved;
                all.insert(link.Unresolved.begin(), link.Unresolved.end());
            }
            if (all.empty()) {
                return;
            }
            const auto& defined = Definers(all);
            const auto& lines = LinkLines();
            for (const TProbe* p : probes) {
                auto it = needs.find(p->Id);
                if (it == needs.end()) {
                    continue;
                }
                TRoundResult& r = out[p->Id];
                const TModelCore& core = p->Sel->M.Core();
                std::map<std::string, const std::set<std::string>*> need;
                for (const std::string& s : it->second) {
                    const auto& d = defined.at(s);
                    if (!d.empty()) {
                        need[s] = &d;
                    }
                }
                std::map<std::string, std::vector<int32_t>> culprits;
                auto culpritsOf = [&](const std::string& s) -> const std::vector<int32_t>& {
                    auto c = culprits.find(s);
                    if (c != culprits.end()) {
                        return c->second;
                    }
                    std::set<int32_t> units;
                    std::vector<int32_t> candidates;
                    for (int32_t h : p->Headers) {
                        const auto& n = h < int32_t(Facts_.NeedsOfHeader.size()) ? Facts_.NeedsOfHeader[h] : std::vector<std::string>();
                        if (std::binary_search(n.begin(), n.end(), s)) {
                            candidates.push_back(h);
                            units.insert(core.Comp[h]);
                        }
                    }
                    // the innermost: no other unit of them included by it
                    std::vector<int32_t> innermost;
                    for (int32_t h : candidates) {
                        int32_t u = core.Comp[h];
                        std::vector<int32_t> stack(core.Children[u].begin(), core.Children[u].end());
                        std::set<int32_t> seen;
                        bool inner = false;
                        while (!stack.empty() && !inner) {
                            int32_t v = stack.back();
                            stack.pop_back();
                            if (!seen.insert(v).second) {
                                continue;
                            }
                            inner = v != u && units.count(v);
                            stack.insert(stack.end(), core.Children[v].begin(), core.Children[v].end());
                        }
                        if (!inner) {
                            innermost.push_back(h);
                        }
                    }
                    if (innermost.empty()) {
                        innermost.push_back(-1);
                    }
                    return culprits[s] = std::move(innermost);
                };
                std::set<int32_t> members;
                for (int32_t t : p->In.Files) {
                    members.insert(Facts_.TargetOf[t]);
                }
                for (int32_t target : members) {
                    auto l = lines.find(Facts_.TargetNames[target]);
                    if (l == lines.end() || l->second.empty()) {
                        continue;
                    }
                    ++r.Files["links"];
                    std::set<std::string> lacking;
                    for (const auto& [s, d] : need) {
                        for (const auto& line : l->second) {
                            bool has = std::any_of(d->begin(), d->end(), [&](const std::string& lib) { return line.count(lib) != 0; });
                            if (!has) {
                                lacking.insert(s);
                                break;
                            }
                        }
                    }
                    if (lacking.empty()) {
                        continue;
                    }
                    ++r.Files["links lacking"];
                    for (const std::string& s : lacking) {
                        std::vector<std::string> libs(need[s]->begin(), need[s]->end());
                        std::string message = Format("%s: a link has no %s for %s", Facts_.TargetNames[target].c_str(),
                                                     Join(libs, " or ").c_str(), Demangled(s).substr(0, 120).c_str());
                        for (int32_t h : culpritsOf(s)) {
                            r.Conflicts[target].emplace_back(h, message);
                        }
                    }
                }
            }
        }

        std::map<int64_t, TRoundResult> TChecker::Round(const std::map<int64_t, std::pair<int32_t, const TSelection*>>& groups) {
            std::vector<TProbe> probes;
            probes.reserve(groups.size());
            for (const auto& [g, ns] : groups) {
                probes.push_back(MakeProbe(g, ns.first, ns.second));
            }
            std::vector<TProbe*> all;
            for (TProbe& p : probes) {
                all.push_back(&p);
            }
            SourceDefines(all);
            for (TProbe* p : all) {
                MacrosOf(*p);
            }
            // a few groups at a time, their PCH removed when they are done:
            // the PCH of all the groups at once do not fit on the disk
            std::map<int64_t, TRoundResult> out;
            size_t chunk = std::max<size_t>(4, size_t(Work_.Jobs()));
            for (size_t begin = 0; begin < all.size(); begin += chunk) {
                std::vector<TProbe*> part(all.begin() + begin, all.begin() + std::min(all.size(), begin + chunk));
                RunChunk(part, out);
                for (TProbe* p : part) {
                    for (const std::string& f : p->SideFiles) {
                        RemoveFile(f);
                    }
                }
                Log(Format("checks: %zu of %zu groups", std::min(all.size(), begin + chunk), all.size()));
            }
            LinkConflicts(all, out);
            return out;
        }

        TSignature Signature(const TFacts& facts, const TSelection& sel) {
            return {{sel.Members.begin(), sel.Members.end()}, SelectedHeaders(facts, sel)};
        }

        std::set<int32_t> Minus(const std::set<int32_t>& a, const std::set<int32_t>& b) {
            std::set<int32_t> out;
            std::set_difference(a.begin(), a.end(), b.begin(), b.end(), std::inserter(out, out.end()));
            return out;
        }

        // Counter.most_common: the most first, ties in the order first counted.
        class TCounter {
        public:
            void Add(const std::string& key, int64_t n = 1) {
                auto [it, added] = Index_.emplace(key, Items_.size());
                if (added) {
                    Items_.emplace_back(key, 0);
                }
                Items_[it->second].second += n;
            }

            int64_t Get(const std::string& key) const {
                auto it = Index_.find(key);
                return it == Index_.end() ? 0 : Items_[it->second].second;
            }

            std::vector<std::pair<std::string, int64_t>> MostCommon(size_t n) const {
                auto items = Items_;
                std::stable_sort(items.begin(), items.end(), [](const auto& a, const auto& b) {
                    return a.second > b.second;
                });
                items.resize(std::min(n, items.size()));
                return items;
            }

        private:
            std::vector<std::pair<std::string, int64_t>> Items_;
            std::unordered_map<std::string, size_t> Index_;
        };

    } // namespace

    bool Checks(const TOptions& options, TWorkDir& work, TFacts& facts, TPlan& plan) {
        const auto& names = facts.HeaderNames;
        TChecker checker(options, work, facts);

        struct TState {
            int32_t Class = 0;
            std::unique_ptr<TSelection> Sel;
            std::optional<std::set<int32_t>> Members;
        };
        std::map<int64_t, TState> groups;
        for (size_t g = 0; g < plan.Groups.size(); ++g) {
            groups[int64_t(g)] = {plan.Groups[g].Class, std::move(plan.Groups[g].Sel), std::nullopt};
        }
        plan.Groups.clear();
        bool perTarget = plan.PerTarget;

        auto bans = [&](const TModel& m, const std::set<int32_t>& headers) {
            return BannedHeaders(m, headers);
        };
        // True to drop the headers `hs` (with the ones including them), false
        // to move the targets `have` out: what gives more G, the headers of
        // `sel` only losing some.
        auto choose = [&](const TSelection& sel, const std::set<int32_t>& members, const std::set<int32_t>& now,
                          const std::set<int32_t>& hs, const std::set<int32_t>& have) {
            std::vector<int32_t> selected = SelectedHeaders(facts, sel);
            std::set<int32_t> headers(selected.begin(), selected.end());
            std::set<int32_t> both = now;
            both.insert(hs.begin(), hs.end());
            double a = Shrunk(sel.M, members, headers, bans(sel.M, both))->Value;
            std::set<int32_t> rest = Minus(members, have);
            double b = rest.empty() ? 0 : Shrunk(sel.M, rest, headers, bans(sel.M, now))->Value;
            return a >= b;
        };

        // what the checks found stays with the targets: a group made again of
        // them has no PCH with the headers dropped for them or breaking them;
        // the targets moved for a -D go together
        std::map<int32_t, std::set<int32_t>> targetBans;
        std::map<int32_t, std::set<std::string>> movedFor;
        std::set<int32_t> moved;
        for (int32_t t : plan.Contrib) {
            targetBans[t].insert(plan.ContribBans.begin(), plan.ContribBans.end());
        }
        auto groupBans = [&](const std::set<int32_t>& members) {
            std::set<int32_t> out;
            for (int32_t t : members) {
                auto it = targetBans.find(t);
                if (it != targetBans.end()) {
                    out.insert(it->second.begin(), it->second.end());
                }
            }
            return out;
        };
        std::map<int64_t, TSignature> checked;
        // a group made again as one checked: as it went then
        std::set<TSignature> clean, failed, seen;
        std::set<int32_t> noPch;
        TCounter noPchWhy;
        int64_t nextId = int64_t(groups.size());
        std::set<int64_t> pending;
        std::set<int32_t> had;
        for (const auto& [g, s] : groups) {
            pending.insert(g);
            had.insert(s.Sel->Members.begin(), s.Sel->Members.end());
        }
        int rounds = 0;
        while (!pending.empty()) {
            ++rounds;
            TModel m = plan.Model;
            std::vector<std::pair<std::string, std::string>> examples;
            auto example = [&](const std::string& key, const std::string& text) {
                for (const auto& [k, _] : examples) {
                    if (k == key) {
                        return;
                    }
                }
                examples.emplace_back(key, text);
            };
            TCounter failKinds;
            Log(Format("checks, round %d: %zu groups...", rounds, pending.size()));
            std::map<int64_t, std::pair<int32_t, const TSelection*>> round;
            for (int64_t g : pending) {
                round[g] = {groups[g].Class, groups[g].Sel.get()};
            }
            std::map<int64_t, TRoundResult> found = checker.Round(round);
            for (const auto& [g, _] : found) {
                checked[g] = Signature(facts, *groups[g].Sel);
                seen.insert(checked[g]);
            }
            TCounter counts;
            TCounter files;
            std::map<int32_t, std::set<int32_t>> leftovers;
            std::set<int64_t> changed;
            // includes the traces miss: a header included again after its guard
            int64_t learned = 0;
            for (const auto& [g, r] : found) {
                for (const auto& [a, b] : r.Edges) {
                    auto ia = facts.HeaderId.find(facts.Paths.Normalize(a));
                    auto ib = facts.HeaderId.find(facts.Paths.Normalize(b));
                    if (ia == facts.HeaderId.end() || ib == facts.HeaderId.end() || ia->second == ib->second) {
                        continue;
                    }
                    if (facts.HeaderEdges.insert(uint64_t(ia->second) << 32 | uint32_t(ib->second)).second) {
                        ++learned;
                    }
                }
            }
            counts.Add("includes learned", learned);
            for (auto& [g, r] : found) {
                TState& state = groups[g];
                const TSelection& sel = *state.Sel;
                counts.Add("not reached", r.Missing);
                counts.Add("reached, not in PCH", r.Extra);
                for (const auto& [k, v] : r.Files) {
                    files.Add(k, v);
                }
                if (!r.Failed.empty()) {
                    counts.Add("failed");
                    example("failed", r.Failed);
                    failKinds.Add(r.FailedKind);
                    failed.insert(checked[g]);
                    if (perTarget) {
                        // a PCH of the target's own failing: no other to try
                        noPch.insert(sel.Members.begin(), sel.Members.end());
                        noPchWhy.Add(r.FailedKind, int64_t(sel.Members.size()));
                    } else {
                        leftovers[state.Class].insert(sel.Members.begin(), sel.Members.end());
                    }
                    groups.erase(g);
                    continue;
                }
                std::set<int32_t> ban;
                // the PCH fails to compile with the flags of the group
                for (const auto& [h, message] : r.Errors) {
                    counts.Add("compile errors");
                    example("compile errors", message);
                    ban.insert(h);
                }
                std::set<int32_t> members = sel.Members;
                // a file of a target fails with the PCH: drop the header or move the targets it breaks
                std::map<int32_t, std::set<int32_t>> byHeader;
                for (const auto& [target, failures] : r.Conflicts) {
                    counts.Add("targets failing");
                    example("targets failing", failures[0].second);
                    bool none = std::any_of(failures.begin(), failures.end(), [](const auto& f) { return f.first < 0; });
                    if (none) {
                        noPch.insert(target);
                        noPchWhy.Add("a link lacking a library for no header of the PCH");
                        members.erase(target);
                        for (const auto& f : failures) {
                            if (f.first < 0) {
                                example("unattributed", f.second);
                                break;
                            }
                        }
                        continue;
                    }
                    for (const auto& f : failures) {
                        byHeader[f.first].insert(target);
                    }
                }
                std::vector<std::pair<int32_t, std::set<int32_t>>> byCount(byHeader.begin(), byHeader.end());
                std::stable_sort(byCount.begin(), byCount.end(), [](const auto& a, const auto& b) {
                    return a.second.size() > b.second.size();
                });
                for (auto& [h, have] : byCount) {
                    std::set<int32_t> in;
                    std::set_intersection(have.begin(), have.end(), members.begin(), members.end(), std::inserter(in, in.end()));
                    if (in.empty() || ban.count(h)) {
                        continue;
                    }
                    std::set<int32_t> now = groupBans(members);
                    now.insert(ban.begin(), ban.end());
                    if (choose(sel, members, now, {h}, in)) {
                        ban.insert(h);
                        counts.Add("failing: headers dropped");
                    } else {
                        members = Minus(members, in);
                        moved.insert(in.begin(), in.end());
                        for (int32_t target : in) {
                            targetBans[target].insert(h);
                        }
                        counts.Add("failing: targets moved", int64_t(in.size()));
                    }
                }
                // a header depending on a -D some files have: drop it or move the targets having it
                std::vector<std::pair<std::string, std::set<int32_t>>> macros(r.Macros.begin(), r.Macros.end());
                std::stable_sort(macros.begin(), macros.end(), [](const auto& a, const auto& b) {
                    return a.second.size() > b.second.size();
                });
                for (auto& [name, headers] : macros) {
                    counts.Add("-D");
                    std::set<int32_t> have;
                    const auto& having = r.Targets[name];
                    std::set_intersection(having.begin(), having.end(), members.begin(), members.end(),
                                          std::inserter(have, have.end()));
                    std::set<int32_t> hs = Minus(headers, ban);
                    if (have.empty() || hs.empty()) {
                        continue;
                    }
                    std::set<int32_t> now = groupBans(members);
                    now.insert(ban.begin(), ban.end());
                    if (choose(sel, members, now, hs, have)) {
                        ban.insert(hs.begin(), hs.end());
                        counts.Add("-D: headers dropped", int64_t(hs.size()));
                    } else {
                        members = Minus(members, have);
                        moved.insert(have.begin(), have.end());
                        for (int32_t target : have) {
                            movedFor[target].insert(name);
                        }
                        counts.Add("-D: targets moved", int64_t(have.size()));
                    }
                    example("-D", Format("%s in %s, targets with it %zu", name.c_str(), Display(names[*hs.begin()]).c_str(),
                                         have.size()));
                }
                if (ban.empty() && members == sel.Members) {
                    continue;
                }
                for (int32_t target : members) {
                    targetBans[target].insert(ban.begin(), ban.end());
                }
                state.Members = members;
                changed.insert(g);
            }
            // a group only loses headers: the files passed stay passed
            std::map<int64_t, std::set<int32_t>> hadHeaders;
            for (const auto& [g, s] : groups) {
                std::vector<int32_t> hs = SelectedHeaders(facts, *s.Sel);
                hadHeaders[g] = {hs.begin(), hs.end()};
            }
            if (learned) {
                // new units: every group takes its headers again
                plan.Core = BuildModel(facts, plan.Core->MaxGroupFiles);
                plan.Model = TModel(plan.Core, plan.Model.Q);
                m = plan.Model;
                for (const auto& [g, _] : groups) {
                    changed.insert(g);
                }
            }
            for (int64_t g : std::set<int64_t>(changed)) {
                auto it = groups.find(g);
                if (it == groups.end()) {
                    changed.erase(g);
                    continue;
                }
                TState& state = it->second;
                std::set<int32_t> members = state.Members ? *state.Members : state.Sel->Members;
                state.Members.reset();
                if (perTarget && members.empty()) {
                    leftovers[state.Class].insert(state.Sel->Members.begin(), state.Sel->Members.end());
                    groups.erase(it);
                    changed.erase(g);
                    continue;
                }
                // --per-target, --max-group-files: the model of the files of the group
                TModel own = perTarget ? PoolModel(m, {*members.begin()}, Q_PER_TARGET)
                    : m.Core().MaxGroupFiles ? PoolModel(m, {members.begin(), members.end()}, m.Q)
                                             : m;
                auto fresh = Shrunk(own, members, hadHeaders[g], bans(m, groupBans(members)));
                std::set<int32_t> kept;
                if (fresh->Value >= (perTarget ? double(MIN_TARGET_G) : double(MIN_GROUP_G))) {
                    kept = fresh->Members;
                }
                std::set<int32_t> lost = Minus(state.Sel->Members, kept);
                leftovers[state.Class].insert(lost.begin(), lost.end());
                if (!kept.empty()) {
                    state.Sel = std::move(fresh);
                } else {
                    groups.erase(it);
                    changed.erase(g);
                }
            }
            // the same targets and headers as checked: nothing to check again
            for (int64_t g : std::set<int64_t>(changed)) {
                if (checked.count(g) && checked[g] == Signature(facts, *groups[g].Sel)) {
                    changed.erase(g);
                }
            }
            for (const auto& [g, _] : found) {
                if (groups.count(g) && !changed.count(g)) {
                    clean.insert(Signature(facts, *groups[g].Sel));
                }
            }
            // a round checks only what no round did: the checks end
            for (int64_t g : std::set<int64_t>(changed)) {
                TSignature sig = Signature(facts, *groups[g].Sel);
                if (!seen.count(sig)) {
                    continue;
                }
                changed.erase(g);
                if (clean.count(sig)) {
                    checked[g] = sig;
                    counts.Add("made again as passed");
                } else {
                    const auto& ms = groups[g].Sel->Members;
                    noPch.insert(ms.begin(), ms.end());
                    noPchWhy.Add("the same group failed", int64_t(ms.size()));
                    counts.Add("made again as failed");
                    groups.erase(g);
                }
            }
            for (const auto& [n, targets] : leftovers) {
                std::map<std::pair<std::set<int32_t>, std::set<std::string>>, std::vector<int32_t>> parts;
                std::vector<int32_t> sorted;
                for (int32_t t : targets) {
                    if (!noPch.count(t)) {
                        sorted.push_back(t);
                    }
                }
                std::sort(sorted.begin(), sorted.end(), [&](int32_t a, int32_t b) {
                    return facts.TargetNames[a] < facts.TargetNames[b];
                });
                for (int32_t t : sorted) {
                    parts[{targetBans[t], movedFor[t]}].push_back(t);
                }
                std::vector<std::pair<std::set<int32_t>, std::vector<int32_t>>> ordered;
                for (auto& [key, part] : parts) {
                    ordered.emplace_back(key.first, std::move(part));
                }
                std::stable_sort(ordered.begin(), ordered.end(), [&](const auto& a, const auto& b) {
                    return facts.TargetNames[a.second[0]] < facts.TargetNames[b.second[0]];
                });
                for (auto& [hs, part] : ordered) {
                    std::vector<char> banned = bans(m, hs);
                    std::vector<std::unique_ptr<TSelection>> sels;
                    if (perTarget) {
                        for (int32_t t : part) {
                            if (auto sel = TargetSelection(m, t, &banned)) {
                                sels.push_back(std::move(sel));
                            }
                        }
                    } else {
                        bool contrib = std::all_of(part.begin(), part.end(), [&](int32_t t) { return plan.Contrib.count(t) != 0; });
                        sels = ClassGroups(facts, m, part, &banned, contrib);
                    }
                    for (auto& sel : sels) {
                        TSignature sig = Signature(facts, *sel);
                        if (failed.count(sig) || (seen.count(sig) && !clean.count(sig))) {
                            noPch.insert(sel->Members.begin(), sel->Members.end());
                            noPchWhy.Add("the same group failed", int64_t(sel->Members.size()));
                            counts.Add("made again as failed");
                            continue;
                        }
                        int64_t g = nextId++;
                        groups[g] = {n, std::move(sel), std::nullopt};
                        if (clean.count(sig)) {
                            checked[g] = sig;
                            counts.Add("made again as passed");
                        } else {
                            changed.insert(g);
                        }
                    }
                }
            }
            std::string filesText = Format(
                "files checked %ld of %ld, failing %ld: on headers %ld, searched %ld (with another file of the target %ld); "
                "targets whose link lacks a library %ld of %ld",
                long(files.Get("checked")), long(files.Get("total")), long(files.Get("failing")), long(files.Get("on headers")),
                long(files.Get("searched")), long(files.Get("searched with another file")), long(files.Get("links lacking")),
                long(files.Get("links")));
            std::vector<std::string> countText;
            for (const char* k : {"compile errors", "targets failing", "failing: headers dropped", "failing: targets moved", "-D",
                                  "-D: headers dropped", "-D: targets moved", "failed", "not reached", "reached, not in PCH",
                                  "includes learned", "made again as failed", "made again as passed"}) {
                countText.push_back(Format("%s %ld", k, long(counts.Get(k))));
            }
            Report(Format("%-18sround %d: %zu groups; %s; %s", rounds == 1 ? "checks:" : "", rounds, found.size(), filesText.c_str(),
                          Join(countText, ", ").c_str()));
            for (const auto& [k, v] : examples) {
                Report("  e.g. " + k + ": " + v);
            }
            for (const auto& [kind, k] : failKinds.MostCommon(TOP_FAILURES)) {
                Report(Format("  failed %ld: %s", long(k), kind.c_str()));
            }
            pending = changed;
        }

        std::set<int32_t> dropped;
        for (const auto& [t, hs] : targetBans) {
            for (int32_t h : hs) {
                if (!plan.ContribBans.count(h)) {
                    dropped.insert(h);
                }
            }
        }
        for (auto& [g, s] : groups) {
            plan.Groups.push_back({s.Class, std::move(s.Sel)});
        }
        Report(Format("checked:          %zu groups pass after %d rounds; headers dropped %zu, targets moved %zu, with no PCH %zu",
                      plan.Groups.size(), rounds, dropped.size(), moved.size(), noPch.size()));
        for (const auto& [why, k] : noPchWhy.MostCommon(TOP_FAILURES)) {
            Report(Format("  with no PCH %ld: %s", long(k), why.c_str()));
        }
        ReportGroups(facts, plan, "checked groups");
        std::set<int32_t> have;
        for (const TGroup& g : plan.Groups) {
            have.insert(g.Sel->Members.begin(), g.Sel->Members.end());
        }
        if (double(have.size()) < MIN_KEPT * double(had.size())) {
            Report(Format("  targets with a PCH %zu of %zu before the checks: fewer than %d%%, nothing is written", have.size(),
                          had.size(), int(MIN_KEPT * 100)));
            return false;
        }
        return true;
    }

} // namespace NPchUpdate
