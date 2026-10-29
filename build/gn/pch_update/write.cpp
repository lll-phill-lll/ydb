#include "gni.h"
#include "ninja.h"
#include "steps.h"

#include <rapidjson/document.h>
#include <rapidjson/error/en.h>

#include <algorithm>
#include <chrono>
#include <cstdlib>
#include <functional>
#include <initializer_list>
#include <map>
#include <set>
#include <tuple>
#include <unordered_map>

namespace NPchUpdate {

    namespace {

        // The name of the group of the targets without //util; contrib_2... for more.
        constexpr std::string_view CONTRIB = "contrib";
        // What a PCH depends on but the generators of its headers: the
        // targets depending on them cannot have it.
        constexpr std::string_view PCH_DEPS[] = {"//contrib/libs/linux-headers:linux-headers", "//contrib/libs/cxxsupp/libcxx:libcxx"};
        constexpr int TOP_GN_ERROR = 10;
        constexpr size_t THREADS = 8;
        // Compiles of the gn gen check parsed at once: their words kept in memory.
        constexpr size_t PARSE_CHUNK = 1024;

        // Flags of a compile that make no other flag class; FLAG_ARGS have
        // their value next (as compile_flags() of the checks).
        constexpr std::string_view NOT_CLASS_PREFIXES[] = {"-I", "-W", "-D", "-U", "-ftime-trace"};
        constexpr std::string_view NOT_CLASS[] = {"-c", "-MD", "-w"};
        constexpr std::string_view INCLUDE_DIR_ARGS[] = {"-I", "-isystem", "-iquote", "-idirafter"};
        constexpr std::string_view FLAG_ARGS[] = {"-o", "-MF", "-I", "-isystem", "-iquote", "-idirafter", "-include", "-D", "-U", "-x"};
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

        std::string LabelDir(std::string_view label) {
            return std::string(label.substr(0, label.find(':')));
        }

        // A label of ninja_outputs.json without its toolchain.
        std::string NoToolchain(std::string_view label) {
            return std::string(label.substr(0, label.find('(')));
        }

        // Problem -> its examples.
        using TProblems = std::map<std::string, std::vector<std::string>>;

        void ReportProblems(const TProblems& problems, bool display) {
            for (const auto& [what, examples] : problems) {
                const std::string& example = examples[0];
                Report(Format("  %zu %s, e.g. %s", examples.size(), what.c_str(), (display ? Display(example) : example).c_str()));
            }
        }

        std::vector<std::pair<std::string, TGnValue>> ReadGni(const std::string& path) {
            std::string text;
            if (!TryReadFile(path, &text)) {
                return {};
            }
            return ParseGnFile(text);
        }

        // pch_groups of groups.gni: (name, targets) in order.
        std::vector<std::pair<std::string, std::vector<std::string>>> ReadGroups(const std::string& path) {
            std::vector<std::pair<std::string, std::vector<std::string>>> out;
            auto values = ReadGni(path);
            const TGnValue* groups = FindGnValue(values, "pch_groups");
            if (!groups) {
                return out;
            }
            for (const TGnValue& group : groups->Items) {
                const TGnValue* name = group.Member("name");
                const TGnValue* targets = group.Member("targets");
                out.emplace_back(name ? name->Text : std::string(), targets ? targets->Strings() : std::vector<std::string>());
            }
            return out;
        }

        // pch_targets of targets.gni.
        std::vector<std::string> ReadPchTargets(const std::string& path) {
            auto values = ReadGni(path);
            const TGnValue* targets = FindGnValue(values, "pch_targets");
            return targets ? targets->Strings() : std::vector<std::string>();
        }

        // The prefixes of the dir of a label: a, a/b, a/b/c.
        std::vector<std::string> DirPrefixes(std::string_view label) {
            std::string_view dir = label.substr(0, label.find(':'));
            dir.remove_prefix(std::min<size_t>(2, dir.size()));
            std::vector<std::string> out;
            std::string prefix;
            for (std::string_view part : Split(dir, '/')) {
                if (!out.empty()) {
                    prefix += '/';
                }
                prefix += part;
                out.push_back(prefix);
            }
            return out;
        }

        // Patterns of gn matching `targets` and none of the rest of `all`: a
        // dir as //dir/* when all the targets of 2 or more under it are there.
        std::vector<std::string> Fold(const std::set<std::string>& targets, const std::set<std::string>& all) {
            // dir -> (targets under it, of them in `targets`)
            std::unordered_map<std::string, std::pair<int64_t, int64_t>> under;
            for (const std::string& t : all) {
                bool in = targets.count(t) != 0;
                for (const std::string& prefix : DirPrefixes(t)) {
                    auto& counts = under[prefix];
                    ++counts.first;
                    counts.second += in;
                }
            }
            std::set<std::string> out;
            for (const std::string& t : targets) {
                std::string pattern = t;
                for (const std::string& prefix : DirPrefixes(t)) {
                    auto it = under.find(prefix);
                    if (it != under.end() && it->second.first >= 2 && it->second.first == it->second.second) {
                        pattern = "//" + prefix + "/*";
                        break;
                    }
                }
                out.insert(pattern);
            }
            return {out.begin(), out.end()};
        }

        // The PCH of a target of pch_targets: //build/gn/pch/per_target/<dir>/<name>.h.
        std::string TargetHeader(std::string_view label) {
            std::string_view rest = label.substr(std::min<size_t>(2, label.size()));
            size_t colon = rest.find(':');
            std::string_view dir = rest.substr(0, colon);
            std::string_view name = colon == std::string_view::npos ? std::string_view() : rest.substr(colon + 1);
            return JoinPath(JoinPath(PER_TARGET_DIR, dir), std::string(name) + ".h");
        }

        // A group of groups.gni as written.
        struct TWrittenGroup {
            double Value = 0;
            const TSelection* Sel = nullptr;
            std::set<std::string> Members;
            std::vector<int32_t> Roots;
            std::vector<std::string> Deps;
            std::vector<std::string> Cflags;
            std::vector<std::string> Defines;
            std::vector<std::string> IncludeDirs;
            // What the checks built the PCH with: all its -D and -U, its include dirs.
            std::pair<std::vector<std::string>, std::vector<std::string>> Checked;
        };

        // =====================================================================
        // gn gen
        // =====================================================================

        // A compile of the manifest of the gn gen check.
        struct TCompile {
            bool Ok = false;
            // compile_flags() of the checks: "-x <language>" one flag.
            std::vector<std::string> Flags;
            std::vector<std::string> Defines;
            std::vector<std::pair<std::string, std::string>> IncludeDirs;
            // The -include of the driver (no -Xclang before), in order: clang
            // takes the PCH of the first one only.
            std::vector<std::string> DriverIncludes;
            std::string IncludePch;
            // -x c++-header: the compile of a PCH.
            bool Header = false;
        };

        // The compile of an edge's command: from `compiler` to the end of the
        // first command; not Ok without the compiler.
        TCompile ParseCompile(const std::string& command, const std::string& compiler) {
            TCompile out;
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
                    return out;
                }
                argv.erase(argv.begin(), it);
            }
            auto end = std::find_if(argv.begin() + 1, argv.end(), [](const std::string& word) {
                return OneOf(word, COMMAND_ENDS);
            });
            argv.erase(end, argv.end());
            out.Ok = true;

            size_t i = 1;
            while (i < argv.size()) {
                const std::string& a = argv[i];
                if (a == "-Xclang" && i + 3 < argv.size() && argv[i + 1] == "-include" && argv[i + 2] == "-Xclang") {
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
                        out.Flags.push_back(a + " " + value);
                        out.Header = out.Header || value == "c++-header";
                    } else if (a == "-include" && (i == 1 || argv[i - 1] != "-Xclang")) {
                        out.DriverIncludes.push_back(value);
                    }
                    i += 2;
                    continue;
                }
                if (a == "-include-pch" && i + 1 < argv.size() && out.IncludePch.empty()) {
                    out.IncludePch = argv[i + 1];
                }
                if (a == "-xc++-header") {
                    out.Header = true;
                }
                if (StartsWith(a, "-D") || StartsWith(a, "-U")) {
                    out.Defines.push_back(a);
                } else if (StartsWith(a, "-I")) {
                    out.IncludeDirs.emplace_back("-I", a.substr(2));
                } else if (!OneOf(a, NOT_CLASS) && !StartsWithOneOf(a, NOT_CLASS_PREFIXES)) {
                    out.Flags.push_back(a);
                }
                ++i;
            }
            return out;
        }

        // The flags of a compile the gn gen check compares, in order: the
        // options, not their values (the source, the PCH).
        std::vector<std::string> CompareFlags(const std::vector<std::string>& flags) {
            std::vector<std::string> out;
            for (const std::string& f : flags) {
                if (StartsWith(f, "-") && f != "-include-pch") {
                    out.push_back(f);
                }
            }
            return out;
        }

        std::vector<std::string> Sorted(std::vector<std::string> values) {
            std::sort(values.begin(), values.end());
            return values;
        }

        std::string_view Trim(std::string_view text) {
            size_t begin = text.find_first_not_of(" \t\r\n");
            if (begin == std::string_view::npos) {
                return {};
            }
            return text.substr(begin, text.find_last_not_of(" \t\r\n") - begin + 1);
        }

        // A line of args.gn setting pch_mode.
        bool SetsPchMode(std::string_view line) {
            line = Trim(line);
            if (!StartsWith(line, "pch_mode")) {
                return false;
            }
            return StartsWith(Trim(line.substr(8)), "=");
        }

        // pch_only_cflags of pch_flags.gni as the compile flags have them:
        // -x with its language, an -Xclang left out.
        std::set<std::string> PchOnlyFlags(const TFacts& facts) {
            std::vector<std::string> argv = PchOnlyArgv(facts);
            std::set<std::string> out;
            size_t i = 0;
            while (i < argv.size()) {
                if (argv[i] == "-x" && i + 1 < argv.size()) {
                    out.insert("-x " + argv[i + 1]);
                    i += 2;
                    continue;
                }
                if (argv[i] != "-Xclang") {
                    out.insert(argv[i]);
                }
                ++i;
            }
            return out;
        }

        // Values of many files, kept once.
        class TInterned {
        public:
            int32_t Id(std::vector<std::string> value) {
                auto [it, added] = Ids_.emplace(std::move(value), int32_t(Values_.size()));
                if (added) {
                    Values_.push_back(&it->first);
                }
                return it->second;
            }

            const std::vector<std::string>& Value(int32_t id) const {
                return *Values_[id];
            }

        private:
            std::map<std::vector<std::string>, int32_t> Ids_;
            std::vector<const std::vector<std::string>*> Values_;
        };

        // The gn gen of the tree as written, in a dir of the work dir: the
        // manifest and the labels of its outputs.
        class TGenerated {
        public:
            TGenerated(const std::string& out, const std::string& root, const std::string& compiler)
                : Out(out)
                , Root(root)
                , Manifest(out)
                , Compiler_(compiler)
            {
            }

            // A path of the manifest relative to the source root, or "".
            std::string SourceOf(const std::string& path) const {
                std::string p = NormPath(JoinPath(Out, path));
                return StartsWith(p, Root + "/") ? p.substr(Root.size() + 1) : std::string();
            }

            // An include dir of a command as gn spells it.
            std::string Spelled(const std::string& dir) const {
                std::string p = NormPath(JoinPath(Out, dir));
                if (StartsWith(p, Out + "/")) {
                    return "$root_build_dir/" + p.substr(Out.size() + 1);
                }
                if (p == Root) {
                    return "//";
                }
                return StartsWith(p, Root + "/") ? "//" + p.substr(Root.size() + 1) : p;
            }

            TCompile Parse(size_t edge) const {
                return ParseCompile(Manifest.Command(edge), Compiler_);
            }

            // Of each edge in `edges`, fn(edge, its compile) in order, the
            // compiles parsed on threads a chunk at a time.
            void ForCompiles(const std::vector<size_t>& edges, const std::function<void(size_t, TCompile&)>& fn) const {
                for (size_t begin = 0; begin < edges.size(); begin += PARSE_CHUNK) {
                    size_t end = std::min(edges.size(), begin + PARSE_CHUNK);
                    std::vector<TCompile> chunk(end - begin);
                    ParallelFor(end - begin, Threads(), [&](size_t i) {
                        chunk[i] = Parse(edges[begin + i]);
                    });
                    for (size_t i = 0; i < chunk.size(); ++i) {
                        if (chunk[i].Ok) {
                            fn(edges[begin + i], chunk[i]);
                        }
                    }
                }
            }

            // ninja_outputs.json: output -> its label, no toolchain; none if more list it.
            void ReadLabels(const std::string& path) {
                std::string json = ReadFile(path);
                rapidjson::Document doc;
                doc.Parse(json.data(), json.size());
                if (doc.HasParseError() || !doc.IsObject()) {
                    Fail(Format("%s: %s at %zu", path.c_str(), rapidjson::GetParseError_En(doc.GetParseError()), doc.GetErrorOffset()));
                }
                for (auto it = doc.MemberBegin(); it != doc.MemberEnd(); ++it) {
                    if (!it->value.IsArray()) {
                        continue;
                    }
                    std::string label = NoToolchain(std::string_view(it->name.GetString(), it->name.GetStringLength()));
                    for (const auto& output : it->value.GetArray()) {
                        if (!output.IsString()) {
                            continue;
                        }
                        // listed by more targets: an input of theirs too (.inputdeps), the owner not told
                        auto [at, added] = LabelOfOutput_.emplace(NormPath(std::string_view(output.GetString(), output.GetStringLength())), label);
                        if (!added && at->second != label) {
                            at->second.clear();
                        }
                    }
                }
            }

            const std::string* LabelOf(const std::string& output) const {
                auto it = LabelOfOutput_.find(output);
                return it == LabelOfOutput_.end() || it->second.empty() ? nullptr : &it->second;
            }

            std::string Out;
            std::string Root;
            TNinjaManifest Manifest;

        private:
            std::string Compiler_;
            std::unordered_map<std::string, std::string> LabelOfOutput_;
        };

        // The edges compiling a source of `dir` of the source root; the
        // edges writing an object of the baseline.
        void SplitEdges(const TGenerated& gen, const TFacts& facts, std::string_view dir, std::vector<size_t>* pch, std::vector<size_t>* files) {
            std::string prefix = std::string(dir) + "/";
            const auto& edges = gen.Manifest.Edges();
            for (size_t e = 0; e < edges.size(); ++e) {
                const auto& edge = edges[e];
                if (edge.Rule == "phony" || edge.Outputs.empty()) {
                    continue;
                }
                if (facts.FileOf.count(edge.Outputs[0])) {
                    files->push_back(e);
                } else if (!edge.Inputs.empty() && edge.Inputs[0].find(prefix) != std::string::npos &&
                           StartsWith(gen.SourceOf(edge.Inputs[0]), prefix))
                {
                    pch->push_back(e);
                }
            }
        }

        // pch_mode = "global": each C++ file of the baseline has the PCH of
        // the group written for its target, each PCH the flags of its files.
        bool CheckGlobal(const TFacts& facts, const TPlan& plan, const TGenerated& gen, double took) {
            const std::string prefix = std::string(GLOBAL_DIR) + "/";
            std::vector<std::pair<std::string, std::vector<std::string>>> groups = ReadGroups(JoinPath(facts.Root, GROUPS_GNI));
            std::set<std::string> written;
            for (const auto& [name, patterns] : groups) {
                written.insert(name);
            }
            std::map<std::string, std::pair<std::vector<std::string>, std::vector<std::string>>> wantInputs(
                plan.WrittenInputs.begin(), plan.WrittenInputs.end());
            std::set<std::string> pchOnly = PchOnlyFlags(facts);

            std::vector<size_t> pchEdges;
            std::vector<size_t> fileEdges;
            SplitEdges(gen, facts, GLOBAL_DIR, &pchEdges, &fileEdges);

            // the compiles of the PCH of the groups: group -> (flags, -D)
            std::unordered_map<size_t, std::string> groupOfEdge;
            std::map<std::string, std::pair<std::vector<std::string>, std::set<std::string>>> pchFlags;
            std::map<std::string, std::vector<std::string>> inputsBad;
            gen.ForCompiles(pchEdges, [&](size_t e, TCompile& c) {
                if (!c.Header) {
                    return;
                }
                std::string source = gen.SourceOf(gen.Manifest.Edges()[e].Inputs[0]).substr(prefix.size());
                std::string group = source.substr(0, source.find('/'));
                groupOfEdge[e] = group;
                std::vector<std::string> own;
                for (const std::string& f : CompareFlags(c.Flags)) {
                    if (pchOnly.count(f)) {
                        if (!own.empty() && own.back() == "-Xclang") {
                            own.pop_back();
                        }
                        continue;
                    }
                    own.push_back(f);
                }
                std::sort(own.begin(), own.end());
                std::set<std::string> defines(c.Defines.begin(), c.Defines.end());
                pchFlags[group] = {std::move(own), defines};

                auto want = wantInputs.find(group);
                if (want == wantInputs.end()) {
                    return;
                }
                std::set<std::string> dirs;
                for (const auto& [option, dir] : c.IncludeDirs) {
                    dirs.insert(gen.Spelled(dir));
                }
                std::vector<std::string> lack;
                for (const std::string& d : want->second.first) {
                    if (!defines.count(d)) {
                        lack.push_back(d);
                    }
                }
                for (const std::string& d : want->second.second) {
                    if (!dirs.count(d)) {
                        lack.push_back(d);
                    }
                }
                if (!lack.empty()) {
                    inputsBad[group] = std::move(lack);
                }
            });

            // The group of a PCH a file takes: the compile of the source its
            // file is copied from; else the path.
            auto groupOfPch = [&](const std::string& path) {
                int64_t e = gen.Manifest.Producer(NormPath(path));
                for (int hops = 0; e >= 0 && hops < 8; ++hops) {
                    auto it = groupOfEdge.find(size_t(e));
                    if (it != groupOfEdge.end()) {
                        return it->second;
                    }
                    const auto& edge = gen.Manifest.Edges()[size_t(e)];
                    if (edge.Inputs.size() != 1) {
                        break;
                    }
                    e = gen.Manifest.Producer(edge.Inputs[0]);
                }
                return path;
            };

            // obj -> (group of its PCH or "", flags, -D)
            struct TFileCompile {
                std::string Pch;
                int32_t Flags = 0;
                int32_t Defines = 0;
            };
            TInterned flagSets;
            TInterned defineSets;
            std::unordered_map<int32_t, TFileCompile> files;
            gen.ForCompiles(fileEdges, [&](size_t e, TCompile& c) {
                TFileCompile& file = files[facts.FileOf.at(gen.Manifest.Edges()[e].Outputs[0])];
                file.Pch = c.IncludePch.empty() ? std::string() : groupOfPch(c.IncludePch);
                file.Flags = flagSets.Id(Sorted(CompareFlags(c.Flags)));
                std::sort(c.Defines.begin(), c.Defines.end());
                c.Defines.erase(std::unique(c.Defines.begin(), c.Defines.end()), c.Defines.end());
                file.Defines = defineSets.Id(std::move(c.Defines));
            });

            std::set<std::string> targets;
            for (int32_t t : facts.TargetOf) {
                targets.insert(facts.TargetNames[t]);
            }
            std::unordered_map<std::string, std::string> groupOf;
            for (const std::string& target : targets) {
                for (const auto& [name, patterns] : groups) {
                    if (LabelMatches(target, patterns)) {
                        groupOf.emplace(target, name);
                        break;
                    }
                }
            }
            auto groupOfTarget = [&](const std::string& target) {
                auto it = groupOf.find(target);
                return it == groupOf.end() ? std::string() : it->second;
            };
            auto shown = [](const std::string& group) {
                return group.empty() ? std::string("none") : group;
            };

            std::map<std::string, std::vector<std::tuple<std::string, std::string, std::string>>> wrong;  // target -> (file, predicted, got)
            std::map<std::string, std::vector<std::pair<std::string, std::string>>> flagsBad;            // group -> (file, what)
            std::set<std::string> noHook;
            std::set<std::string> elsewhere;
            for (size_t f = 0; f < facts.Objs.size(); ++f) {
                const std::string& obj = facts.Objs[f];
                const std::string& target = facts.TargetNames[facts.TargetOf[f]];
                auto it = files.find(int32_t(f));
                if (it == files.end()) {
                    elsewhere.insert(target);   // a tool: built in tool_toolchain only
                    continue;
                }
                const TFileCompile& file = it->second;
                std::string predicted = groupOfTarget(target);
                if (file.Pch.empty() && !predicted.empty()) {
                    noHook.insert(target);      // its template takes no PCH (static_library ...)
                } else if (file.Pch != predicted) {
                    wrong[target].emplace_back(obj, predicted, file.Pch);
                } else if (!file.Pch.empty()) {
                    auto pch = pchFlags.find(file.Pch);
                    if (pch == pchFlags.end()) {
                        continue;
                    }
                    const auto& want = pch->second.first;
                    const auto& wantDefines = pch->second.second;
                    const auto& flags = flagSets.Value(file.Flags);
                    const auto& defines = defineSets.Value(file.Defines);
                    if (flags != want) {
                        std::vector<std::string> diff;
                        for (const std::string& x : flags) {
                            if (std::find(want.begin(), want.end(), x) == want.end()) {
                                diff.push_back("+" + x);
                            }
                        }
                        for (const std::string& x : want) {
                            if (std::find(flags.begin(), flags.end(), x) == flags.end()) {
                                diff.push_back("-" + x);
                            }
                        }
                        flagsBad[file.Pch].emplace_back(obj, Join(diff, " "));
                        continue;
                    }
                    std::vector<std::string> missing;
                    for (const std::string& d : wantDefines) {
                        if (!std::binary_search(defines.begin(), defines.end(), d)) {
                            missing.push_back(d);
                        }
                    }
                    if (!missing.empty()) {
                        flagsBad[file.Pch].emplace_back(obj, "PCH -D not here: " + Join(missing, " "));
                    }
                }
            }

            size_t noHookOnly = 0;
            size_t elsewhereOnly = 0;
            for (const std::string& t : noHook) {
                noHookOnly += !wrong.count(t);
            }
            for (const std::string& t : elsewhere) {
                elsewhereOnly += !wrong.count(t) && !noHook.count(t);
            }
            size_t flagsOk = 0;
            size_t inputsOk = 0;
            for (const std::string& g : written) {
                flagsOk += pchFlags.count(g) && !flagsBad.count(g);
                inputsOk += pchFlags.count(g) && !inputsBad.count(g);
            }
            Report(Format("gn gen:           ok in %s; PCH as predicted %zu of %zu targets (no PCH by template %zu, tools %zu); "
                          "PCH flags match its files %zu of %zu groups, -D and include dirs of the checks %zu of %zu",
                          Duration(took).c_str(), targets.size() - wrong.size(), targets.size(), noHookOnly, elsewhereOnly,
                          flagsOk, written.size(), inputsOk, written.size()));
            int shownWrong = 0;
            for (const auto& [target, bad] : wrong) {
                if (shownWrong++ == TOP_GN_ERROR) {
                    break;
                }
                const auto& [obj, want, got] = bad[0];
                Report(Format("  %s: %s has PCH %s, predicted %s", target.c_str(), Display(obj).c_str(), shown(got).c_str(),
                              shown(want).c_str()));
            }
            bool allCompiled = true;
            for (const std::string& g : written) {
                if (!pchFlags.count(g)) {
                    allCompiled = false;
                    Report(Format("  group %s: no compile of its PCH", g.c_str()));
                }
            }
            for (const auto& [g, bad] : flagsBad) {
                Report(Format("  group %s: %zu files differ, e.g. %s: %s", g.c_str(), bad.size(), Display(bad[0].first).c_str(),
                              bad[0].second.c_str()));
            }
            for (const auto& [g, lack] : inputsBad) {
                Report(Format("  group %s: PCH lacks %zu of the -D and include dirs the checks built it with, e.g. %s", g.c_str(),
                              lack.size(), lack[0].c_str()));
            }
            return wrong.empty() && flagsBad.empty() && inputsBad.empty() && allCompiled;
        }

        // pch_mode = "per_target": each C++ file has the PCH of its target if
        // one was written for it, each such target compiles its PCH.
        bool CheckPerTarget(const TFacts& facts, const TGenerated& gen, double took) {
            const std::string prefix = std::string(PER_TARGET_DIR) + "/";
            std::vector<std::string> pchTargets = ReadPchTargets(JoinPath(facts.Root, TARGETS_GNI));
            std::set<std::string> written(pchTargets.begin(), pchTargets.end());
            std::unordered_map<std::string, std::string> targetOfHeader;
            for (const std::string& target : written) {
                targetOfHeader.emplace(TargetHeader(target), target);
            }

            std::vector<size_t> pchEdges;
            std::vector<size_t> fileEdges;
            SplitEdges(gen, facts, PER_TARGET_DIR, &pchEdges, &fileEdges);

            // the compiles of the PCH of the targets: the target of each, by
            // ninja_outputs.json, else the one whose header it compiles
            std::unordered_map<size_t, std::string> targetOfEdge;
            std::set<std::string> compiled;
            gen.ForCompiles(pchEdges, [&](size_t e, TCompile& c) {
                if (!c.Header) {
                    return;
                }
                const auto& edge = gen.Manifest.Edges()[e];
                std::string label;
                for (const std::string& output : edge.Outputs) {
                    if (const std::string* l = gen.LabelOf(output)) {
                        label = *l;
                        break;
                    }
                }
                if (label.empty()) {
                    std::string source = gen.SourceOf(edge.Inputs[0]);
                    auto it = targetOfHeader.find(source);
                    label = it == targetOfHeader.end() ? source : it->second;
                }
                targetOfEdge[e] = label;
                compiled.insert(label);
            });

            // The target of the PCH of an -include: the compile of the file
            // clang takes for it (<include>.gch, <include>.pch), or none.
            auto targetOfInclude = [&](const std::string& include) -> const std::string* {
                for (const char* suffix : {".gch", ".pch", ""}) {
                    int64_t e = gen.Manifest.Producer(NormPath(include + suffix));
                    if (e < 0) {
                        continue;
                    }
                    auto it = targetOfEdge.find(size_t(e));
                    if (it != targetOfEdge.end()) {
                        return &it->second;
                    }
                }
                return nullptr;
            };

            const std::vector<int32_t>& labelOf = facts.LabelOf.size() == facts.Objs.size() ? facts.LabelOf : facts.TargetOf;
            std::unordered_map<int32_t, std::string> files;                                   // file -> target of its PCH
            std::map<std::string, std::vector<std::pair<std::string, std::string>>> notFirst;  // target -> (file, the -include before its PCH)
            gen.ForCompiles(fileEdges, [&](size_t e, TCompile& c) {
                int32_t f = facts.FileOf.at(gen.Manifest.Edges()[e].Outputs[0]);
                std::string& pch = files[f];
                for (size_t i = 0; i < c.DriverIncludes.size(); ++i) {
                    if (const std::string* target = targetOfInclude(c.DriverIncludes[i])) {
                        pch = *target;
                        if (i > 0) {
                            notFirst[facts.TargetNames[labelOf[f]]].emplace_back(facts.Objs[f], c.DriverIncludes[0]);
                        }
                        break;
                    }
                }
            });

            auto shown = [](const std::string& target) {
                return target.empty() ? std::string("none") : target;
            };
            std::map<std::string, std::vector<std::tuple<std::string, std::string, std::string>>> wrong;  // target -> (file, predicted, got)
            std::set<std::string> noHook;
            std::set<std::string> elsewhere;
            std::set<std::string> targets;
            for (size_t f = 0; f < facts.Objs.size(); ++f) {
                const std::string& obj = facts.Objs[f];
                const std::string& target = facts.TargetNames[labelOf[f]];
                targets.insert(target);
                auto it = files.find(int32_t(f));
                if (it == files.end()) {
                    elsewhere.insert(target);   // another toolchain
                    continue;
                }
                std::string want = written.count(target) ? target : std::string();
                if (it->second.empty() && !want.empty()) {
                    noHook.insert(target);      // its template takes no PCH (static_library ...)
                } else if (it->second != want) {
                    wrong[target].emplace_back(obj, want, it->second);
                }
            }

            size_t noHookOnly = 0;
            size_t elsewhereOnly = 0;
            for (const std::string& t : noHook) {
                noHookOnly += !wrong.count(t);
            }
            for (const std::string& t : elsewhere) {
                elsewhereOnly += !wrong.count(t) && !noHook.count(t);
            }
            size_t writtenCompiled = 0;
            for (const std::string& t : written) {
                writtenCompiled += compiled.count(t);
            }
            Report(Format("gn gen:           ok in %s; PCH as predicted %zu of %zu targets (no PCH by template %zu, "
                          "other toolchains %zu); PCH compiled %zu of %zu targets written",
                          Duration(took).c_str(), targets.size() - wrong.size() - noHookOnly, targets.size(), noHookOnly,
                          elsewhereOnly, writtenCompiled, written.size()));
            int n = 0;
            for (const auto& [target, bad] : wrong) {
                if (n++ == TOP_GN_ERROR) {
                    break;
                }
                const auto& [obj, want, got] = bad[0];
                Report(Format("  %s: %s has PCH %s, predicted %s", target.c_str(), Display(obj).c_str(), shown(got).c_str(),
                              shown(want).c_str()));
            }
            n = 0;
            for (const std::string& target : noHook) {
                if (n++ == TOP_GN_ERROR) {
                    break;
                }
                Report(Format("  %s: no PCH, its template takes none", target.c_str()));
            }
            n = 0;
            for (const std::string& target : written) {
                if (compiled.count(target)) {
                    continue;
                }
                if (n++ == TOP_GN_ERROR) {
                    break;
                }
                Report(Format("  %s: no compile of its PCH", target.c_str()));
            }
            n = 0;
            for (const auto& [target, bad] : notFirst) {
                if (n++ == TOP_GN_ERROR) {
                    break;
                }
                Report(Format("  %s: %zu files -include %s before the PCH, clang takes no PCH, e.g. %s", target.c_str(), bad.size(),
                              bad[0].second.c_str(), Display(bad[0].first).c_str()));
            }
            return wrong.empty() && noHook.empty() && notFirst.empty() && writtenCompiled == written.size();
        }

    } // namespace

    // =========================================================================
    // //build/gn/pch: a copy, put back
    // =========================================================================

    TPchDirBackup::TPchDirBackup(const TFacts& facts)
        : Root_(facts.Root)
    {
        const char* tmp = getenv("TMPDIR");
        Copy_ = MakeTempDir(tmp && *tmp ? tmp : "/tmp", "pch_update.pch.");
        RunChecked({"cp", "-a", "--", JoinPath(Root_, PCH_DIR), JoinPath(Copy_, "pch")});
    }

    TPchDirBackup::~TPchDirBackup() {
        try {
            if (!Keep_) {
                std::string dir = JoinPath(Root_, PCH_DIR);
                RemoveTree(dir);
                RunChecked({"cp", "-a", "--", JoinPath(Copy_, "pch"), dir});
                Report(Format("%-18s%.*s put back as it was before the write", "", int(PCH_DIR.size()), PCH_DIR.data()));
            }
            RemoveTree(Copy_);
        } catch (const std::exception& e) {
            Log(Format("%.*s not put back from %s: %s", int(PCH_DIR.size()), PCH_DIR.data(), Copy_.c_str(), e.what()));
        }
    }

    // =========================================================================
    // writing
    // =========================================================================

    bool Write(const TOptions& options, TFacts& facts, TPlan& plan) {
        (void)options;
        const auto& names = facts.HeaderNames;
        std::set<std::string> allTargets;
        for (int32_t t : facts.TargetOf) {
            allTargets.insert(facts.TargetNames[t]);
        }
        const std::vector<std::string> noFlags;
        const std::vector<std::string>& first = facts.ClassFlags.empty() ? noFlags : facts.ClassFlags[0];
        std::set<std::string> universal;
        std::set<std::pair<std::string, std::string>> universalDirs;
        if (!facts.Objs.empty()) {
            universal.insert(facts.DefinesOf[0].begin(), facts.DefinesOf[0].end());
            universalDirs.insert(facts.IncludeDirsOf[0].begin(), facts.IncludeDirsOf[0].end());
            for (size_t f = 1; f < facts.Objs.size(); ++f) {
                std::set<std::string> defines(facts.DefinesOf[f].begin(), facts.DefinesOf[f].end());
                std::set<std::pair<std::string, std::string>> dirs(facts.IncludeDirsOf[f].begin(), facts.IncludeDirsOf[f].end());
                std::erase_if(universal, [&](const std::string& d) {
                    return !defines.count(d);
                });
                std::erase_if(universalDirs, [&](const auto& d) {
                    return !dirs.count(d);
                });
            }
        }
        // the generator of a library is private, its <name>_codegen group
        // (codegen_target of //build/gn/protobuf.gni) is not
        std::map<std::string, std::string> codegen;
        for (const auto& [a, bs] : facts.DepsOf) {
            if (!EndsWith(a, "_codegen")) {
                continue;
            }
            for (const std::string& b : bs) {
                if (LabelDir(b) != LabelDir(a)) {
                    continue;
                }
                auto [it, added] = codegen.emplace(b, a);
                if (!added && a < it->second) {
                    it->second = a;
                }
            }
        }
        TProblems problems;

        std::vector<TWrittenGroup> written;
        int64_t outForCycles = 0;
        for (const TGroup& group : plan.Groups) {
            const TSelection& sel = *group.Sel;
            double value = sel.Evaluate(double(sel.Parse), double(sel.Build), double(sel.Hit.size()), double(sel.HitC),
                                        double(sel.NonhitP), double(sel.NonhitN), double(sel.Ready)).G;
            if (value < double(MIN_GROUP_G)) {
                continue;
            }
            const std::vector<std::string>& flags = facts.ClassFlags[group.Class];
            std::vector<std::string> lost;
            for (const std::string& f : first) {
                if (std::find(flags.begin(), flags.end(), f) == flags.end()) {
                    lost.push_back(f);
                }
            }
            if (!lost.empty()) {
                problems["groups of a class lacking flags of class 1"].push_back(Format("%d: %s", group.Class + 1, Join(lost, " ").c_str()));
                continue;
            }
            TGroupInputs inputs = GroupInputs(facts, sel);
            if (inputs.Roots.empty()) {
                problems["groups with no header"].push_back(std::to_string(group.Class + 1));
                continue;
            }
            std::set<std::string> gens;
            const std::vector<int32_t>& comp = plan.Core ? plan.Core->Comp : sel.M.Core().Comp;
            for (size_t h = 0; h < names.size(); ++h) {
                if (!sel.Selected[comp[h]] || !StartsWith(names[h], "<out>/")) {
                    continue;
                }
                auto producer = facts.LabelOfOutput.find(names[h].substr(6));
                if (producer == facts.LabelOfOutput.end() || !StartsWith(producer->second, "//")) {
                    problems["generated headers made by no target of the manifest"].push_back(names[h]);
                    continue;
                }
                auto it = codegen.find(producer->second);
                gens.insert(it == codegen.end() ? producer->second : it->second);
            }
            std::vector<std::string> start(gens.begin(), gens.end());
            start.insert(start.end(), std::begin(PCH_DEPS), std::end(PCH_DEPS));
            std::set<std::string> reachable = Reachable(facts, start);
            TWrittenGroup w;
            for (int32_t t : sel.Members) {
                if (!reachable.count(facts.TargetNames[t])) {
                    w.Members.insert(facts.TargetNames[t]);
                }
            }
            outForCycles += int64_t(sel.Members.size() - w.Members.size());
            if (w.Members.empty()) {
                continue;
            }
            for (const std::string& f : flags) {
                if (std::find(first.begin(), first.end(), f) != first.end()) {
                    continue;
                }
                for (std::string_view a : Split(f, ' ')) {
                    w.Cflags.emplace_back(a);
                }
            }
            for (const std::string& d : inputs.Defines) {
                if (StartsWith(d, "-U") && !universal.count(d)) {
                    w.Cflags.push_back(d);
                }
            }
            for (const std::string& d : inputs.Defines) {
                if (StartsWith(d, "-D") && !universal.count(d)) {
                    w.Defines.push_back(d.substr(2));
                }
            }
            for (const auto& dir : inputs.IncludeDirs) {
                if (!universalDirs.count(dir)) {
                    w.IncludeDirs.push_back(GnDir(facts, dir.second));
                }
            }
            w.Checked.first = inputs.Defines;
            for (const auto& dir : inputs.IncludeDirs) {
                w.Checked.second.push_back(GnDir(facts, dir.second));
            }
            w.Value = value;
            w.Sel = &sel;
            w.Roots = std::move(inputs.Roots);
            w.Deps.assign(gens.begin(), gens.end());
            written.push_back(std::move(w));
        }
        std::stable_sort(written.begin(), written.end(), [](const TWrittenGroup& a, const TWrittenGroup& b) {
            return a.Value > b.Value;
        });

        // the names of the groups now: contrib, contrib_2...; the others by
        // the most targets in common with a group before
        std::vector<std::pair<std::string, std::vector<std::string>>> before;
        for (auto& [name, patterns] : ReadGroups(JoinPath(facts.Root, GROUPS_GNI))) {
            if (StartsWith(name, CONTRIB)) {
                continue;
            }
            auto it = std::find_if(before.begin(), before.end(), [&](const auto& b) {
                return b.first == name;
            });
            if (it != before.end()) {
                it->second = std::move(patterns);
            } else {
                before.emplace_back(name, std::move(patterns));
            }
        }
        std::set<std::string> taken;
        std::vector<std::string> numbered;    // "": a number to give
        for (const TWrittenGroup& w : written) {
            if (IsContrib(plan, *w.Sel)) {
                size_t k = std::count_if(numbered.begin(), numbered.end(), [](const std::string& name) {
                    return StartsWith(name, CONTRIB);
                });
                numbered.push_back(std::string(CONTRIB) + (k ? Format("_%zu", k + 1) : std::string()));
                continue;
            }
            size_t bestCount = 0;
            std::string best;
            for (const auto& [name, patterns] : before) {
                if (taken.count(name)) {
                    continue;
                }
                size_t count = std::count_if(w.Members.begin(), w.Members.end(), [&](const std::string& t) {
                    return LabelMatches(t, patterns);
                });
                if (count > bestCount || (count && count == bestCount && name > best)) {
                    bestCount = count;
                    best = name;
                }
            }
            numbered.push_back(bestCount ? best : std::string());
            if (bestCount) {
                taken.insert(best);
            }
        }
        int k = 1;
        for (std::string& name : numbered) {
            if (!name.empty()) {
                continue;
            }
            while (taken.count(std::to_string(k))) {
                ++k;
            }
            name = std::to_string(k);
            taken.insert(name);
        }

        std::string pchDir = JoinPath(facts.Root, GLOBAL_DIR);
        std::string text = Format("# %.*s\n", int(WRITTEN.size()), WRITTEN.data()) + "pch_groups = [\n";
        size_t patterns = 0;
        size_t members = 0;
        std::set<std::string> deps;
        std::set<std::string> writtenNames;
        plan.WrittenInputs.clear();
        for (size_t i = 0; i < written.size(); ++i) {
            const std::string& name = numbered[i];
            const TWrittenGroup& w = written[i];
            std::vector<std::string> pats = Fold(w.Members, allTargets);
            patterns += pats.size();
            members += w.Members.size();
            deps.insert(w.Deps.begin(), w.Deps.end());
            writtenNames.insert(name);
            plan.WrittenInputs.emplace_back(name, w.Checked);
            text += "  {\n";
            text += "    name = " + GnQuote(name) + "\n";
            text += GnList("targets", pats, 4);
            text += GnList("deps", w.Deps, 4);
            text += GnList("cflags", w.Cflags, 4);
            text += GnList("defines", w.Defines, 4);
            text += GnList("include_dirs", w.IncludeDirs, 4, true);
            text += "  },\n";
            std::string header = Format("// %.*s\n", int(WRITTEN.size()), WRITTEN.data());
            for (int32_t h : w.Roots) {
                header += "#include " + Spelling(facts, names[h]) + "\n";
            }
            MakeDirs(JoinPath(pchDir, name));
            WriteFileIfChanged(JoinPath(JoinPath(pchDir, name), "pch.h"), header);
            WriteFileIfChanged(JoinPath(JoinPath(pchDir, name), "pch.cc"), "#include \"pch.h\"\n");
        }
        text += "]\n";
        WriteFileIfChanged(JoinPath(facts.Root, GROUPS_GNI), text);
        Report(Format("write:            groups %zu of %zu (G %s+), targets %zu in %zu patterns, generator deps %zu, "
                      "targets out for cycles %lld",
                      written.size(), plan.Groups.size(), Duration(double(MIN_GROUP_G)).c_str(), members, patterns, deps.size(),
                      (long long)outForCycles));

        // the dirs of no group
        std::vector<std::string> gone;
        for (const std::string& d : ListDir(pchDir)) {
            if (IsDir(JoinPath(pchDir, d)) && !writtenNames.count(d)) {
                gone.push_back(d);
            }
        }
        for (const std::string& d : gone) {
            RemoveTree(JoinPath(pchDir, d));
        }
        Report(Format("cleanup:          removed %zu dirs%s", gone.size(), gone.empty() ? "" : (": " + Join(gone, ", ")).c_str()));
        ReportProblems(problems, true);
        return problems.empty();
    }

    bool WritePerTarget(const TOptions& options, TFacts& facts, TPlan& plan) {
        (void)options;
        const auto& names = facts.HeaderNames;
        std::map<std::string, std::vector<int32_t>> written;
        TProblems problems;
        for (const TGroup& group : plan.Groups) {
            const TSelection& sel = *group.Sel;
            if (sel.Value < double(MIN_TARGET_G) || sel.Members.empty()) {
                continue;
            }
            const std::string& target = facts.TargetNames[*sel.Members.begin()];
            TGroupInputs inputs = GroupInputs(facts, sel);
            if (inputs.Roots.empty()) {
                problems["targets with no header"].push_back(target);
                continue;
            }
            written[target] = std::move(inputs.Roots);
        }
        std::string path = JoinPath(facts.Root, TARGETS_GNI);
        std::vector<std::string> beforeList = ReadPchTargets(path);
        std::set<std::string> before(beforeList.begin(), beforeList.end());

        // pch_targets replaced, the rest of targets.gni kept
        std::string text;
        TryReadFile(path, &text);
        std::vector<std::string> targets;
        for (const auto& [target, roots] : written) {
            targets.push_back(target);
        }
        std::string listed = GnList("pch_targets", targets, 0);
        constexpr std::string_view START = "pch_targets = [";
        size_t start = std::string::npos;
        for (size_t pos = text.find(START); pos != std::string::npos; pos = text.find(START, pos + 1)) {
            if (pos == 0 || text[pos - 1] == '\n') {
                start = pos;
                break;
            }
        }
        size_t end = start == std::string::npos ? std::string::npos : text.find("]\n", start + START.size());
        if (end != std::string::npos) {
            text = text.substr(0, start) + listed + text.substr(end + 2);
        } else {
            text = listed + "\n" + text;
        }
        WriteFileIfChanged(path, text);
        size_t included = 0;
        for (const auto& [target, roots] : written) {
            std::string header = JoinPath(facts.Root, TargetHeader(target));
            std::string content = Format("// %.*s\n#pragma once\n\n", int(WRITTEN.size()), WRITTEN.data());
            for (int32_t h : roots) {
                content += "#include " + Spelling(facts, names[h]) + "\n";
            }
            MakeDirs(DirName(header));
            WriteFileIfChanged(header, content);
            included += roots.size();
        }

        // the PCH of the targets gone, and the dirs they leave empty
        std::string pchDir = JoinPath(facts.Root, PER_TARGET_DIR);
        size_t kept = 0;
        size_t removed = 0;
        for (const std::string& target : before) {
            if (written.count(target)) {
                ++kept;
                continue;
            }
            ++removed;
            std::string header = JoinPath(facts.Root, TargetHeader(target));
            if (Exists(header)) {
                RemoveFile(header);
            }
            for (std::string d = DirName(header); d != pchDir && StartsWith(d, pchDir + "/") && IsDir(d) && ListDir(d).empty();
                 d = DirName(d))
            {
                RemoveTree(d);
            }
        }
        Report(Format("write:            pch_targets %zu of %zu (G %s+), headers included %zu; before %zu: kept %zu, new %zu, removed %zu",
                      written.size(), plan.Groups.size(), Duration(double(MIN_TARGET_G)).c_str(), included, before.size(), kept,
                      written.size() - kept, removed));
        ReportProblems(problems, false);
        return problems.empty();
    }

    // =========================================================================
    // gn gen
    // =========================================================================

    bool GnGenCheck(const TOptions& options, const TTools& tools, TWorkDir& work, TFacts& facts, TPlan& plan) {
        (void)options;
        std::string mode = plan.PerTarget ? "per_target" : "global";
        std::string root = RealPath(facts.Root);
        std::string out = work.SideDir("gn_gen");
        RemoveTree(out);
        MakeDirs(out);
        out = RealPath(out);

        // the args of the baseline, pch_mode as the mode needs
        std::string baseline = ReadFile(JoinPath(facts.BuildDir, "args.gn"));
        std::string args;
        for (size_t pos = 0; pos < baseline.size();) {
            size_t end = std::min(baseline.size(), baseline.find('\n', pos) + 1);
            if (end == 0) {
                end = baseline.size();
            }
            std::string_view line = std::string_view(baseline).substr(pos, end - pos);
            if (!SetsPchMode(line)) {
                args += line;
            }
            pos = end;
        }
        if (!args.empty() && args.back() != '\n') {
            args += '\n';
        }
        args += "pch_mode = \"" + mode + "\"\n";
        WriteFile(JoinPath(out, "args.gn"), args);

        // the options of the baseline's gn rule for its scripts
        std::vector<std::string> argv = {tools.Gn, "--root=" + root, "-q"};
        if (facts.Manifest) {
            const auto& edges = facts.Manifest->Edges();
            for (size_t e = 0; e < edges.size(); ++e) {
                if (edges[e].Rule != "gn") {
                    continue;
                }
                for (const std::string& word : ShellSplit(facts.Manifest->Command(e))) {
                    if (StartsWith(word, "--script-executable")) {
                        argv.push_back(word);
                    }
                }
                break;
            }
        }
        std::string outputs = JoinPath(out, "ninja_outputs.json");
        argv.insert(argv.end(), {"gen", out, "--ninja-outputs-file=" + outputs});

        auto started = std::chrono::steady_clock::now();
        TRunOptions run;
        run.MergeStderr = true;
        TRunResult result = Run(argv, run);
        double took = double(std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::steady_clock::now() - started).count());
        if (result.ExitCode != 0) {
            Report(Format("gn gen:           failed in %s:", Duration(took).c_str()));
            std::string_view output = Trim(result.Stdout);
            std::vector<std::string_view> lines = output.empty() ? std::vector<std::string_view>() : Split(output, '\n');
            for (size_t i = 0; i < lines.size() && i < size_t(TOP_GN_ERROR); ++i) {
                Report("  " + std::string(lines[i]));
            }
            return false;
        }

        TGenerated gen(out, root, facts.Compiler);
        if (plan.PerTarget) {
            gen.ReadLabels(outputs);
            return CheckPerTarget(facts, gen, took);
        }
        return CheckGlobal(facts, plan, gen, took);
    }

} // namespace NPchUpdate
