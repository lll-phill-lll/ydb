#include "gni.h"
#include "steps.h"

#include <algorithm>
#include <functional>
#include <map>
#include <tuple>
#include <unordered_map>

namespace NPchUpdate {

    namespace {

        constexpr int TOP_RESIDUAL = 15;
        constexpr int TOP_DIRS = 4;
        constexpr int TOP_ROOTS = 3;
        constexpr int TOP_TARGETS = 25;

        bool IsEnumObjs(std::string_view label) {
            return EndsWith(label, ENUM_OBJS);
        }

        // The targets depending on //util (not through an executable), the
        // targets of each dir.
        class TUtilWorld {
        public:
            explicit TUtilWorld(const TFacts& facts)
                : Facts_(facts)
            {
                for (const auto& [label, type] : facts.TypeOf) {
                    DirTargets_[std::string(LabelDir(label))].push_back(label);
                }
            }

            bool HasUtil(const std::string& label) {
                auto it = HasUtil_.find(label);
                if (it != HasUtil_.end()) {
                    return it->second;
                }
                bool has = label == UTIL;
                if (!has) {
                    auto deps = Facts_.DepsOf.find(label);
                    if (deps != Facts_.DepsOf.end()) {
                        for (const std::string& dep : deps->second) {
                            auto type = Facts_.TypeOf.find(dep);
                            if (type != Facts_.TypeOf.end() && type->second == "executable") {
                                continue;
                            }
                            if (HasUtil(dep)) {
                                has = true;
                                break;
                            }
                        }
                    }
                }
                HasUtil_[label] = has;
                return has;
            }

            // A header only targets depending on //util have: the ones of its
            // dir and the dirs above it, the producer of a generated one.
            bool OfUtil(std::string_view name) {
                if (StartsWith(name, LIBCXX) || StartsWith(name, "<sysroot>") || IsAbs(name)) {
                    return false;
                }
                std::vector<std::string> owners;
                std::string path(name);
                if (StartsWith(path, "<out>/")) {
                    path = path.substr(6);
                    auto producer = Facts_.LabelOfOutput.find(path);
                    if (producer != Facts_.LabelOfOutput.end()) {
                        owners.push_back(producer->second);
                    }
                    if (StartsWith(path, "gen/")) {
                        path = path.substr(4);
                    }
                }
                for (std::string dir = DirName(path); !dir.empty(); dir = DirName(dir)) {
                    auto it = DirTargets_.find(dir);
                    if (it != DirTargets_.end()) {
                        owners.insert(owners.end(), it->second.begin(), it->second.end());
                    }
                }
                if (owners.empty()) {
                    return false;
                }
                for (const std::string& owner : owners) {
                    if (!HasUtil(owner)) {
                        return false;
                    }
                }
                return true;
            }

        private:
            static std::string_view LabelDir(std::string_view label) {
                label.remove_prefix(StartsWith(label, "//") ? 2 : 0);
                return label.substr(0, label.find(':'));
            }

            const TFacts& Facts_;
            std::unordered_map<std::string, std::vector<std::string>> DirTargets_;
            std::unordered_map<std::string, bool> HasUtil_;
        };

        // The contrib part: the targets not depending on //util (NO_UTIL),
        // the headers they cannot have -- of only targets depending on //util,
        // parsed in no file of the part.
        void NoUtil(const TFacts& facts, TPlan& plan) {
            const TModelCore& core = *plan.Core;
            TUtilWorld world(facts);
            plan.Contrib.clear();
            plan.ContribBans.clear();
            std::vector<char> inPart(core.NFiles, 0);
            for (int32_t t = 0; t < int32_t(core.FilesOf.size()); ++t) {
                const std::string& label = facts.TargetNames[t];
                if (core.FilesOf[t].empty() || IsEnumObjs(label) || world.HasUtil(label)) {
                    continue;
                }
                plan.Contrib.insert(t);
                for (int32_t f : core.FilesOf[t]) {
                    inPart[f] = 1;
                }
            }
            for (int32_t u = 0; u < int32_t(core.Headers.size()); ++u) {
                const auto& files = core.Files[u].Files;
                if (std::any_of(files.begin(), files.end(), [&](int32_t f) { return inPart[f] != 0; })) {
                    continue;
                }
                for (int32_t h : core.Headers[u]) {
                    if (world.OfUtil(facts.HeaderNames[h])) {
                        plan.ContribBans.insert(h);
                    }
                }
            }
        }

        std::vector<char> ContribBanned(const TPlan& plan) {
            std::set<int32_t> units;
            for (int32_t h : plan.ContribBans) {
                units.insert(plan.Core->Comp[h]);
            }
            return BannedUnits(plan.Model, {units.begin(), units.end()});
        }

        int32_t TargetsWithFiles(const TModelCore& core, const TFacts& facts) {
            int32_t n = 0;
            for (int32_t t = 0; t < int32_t(core.FilesOf.size()); ++t) {
                n += !core.FilesOf[t].empty() && !IsEnumObjs(facts.TargetNames[t]);
            }
            return n;
        }

        // The targets of the files of a class, by name; the enum serialization
        // of enum_serialization_pch_exclude (its own target) left out.
        std::vector<int32_t> ClassTargets(const TFacts& facts, int32_t n) {
            std::set<int32_t> set;
            for (int32_t f : facts.Classes[n]) {
                if (!IsEnumObjs(facts.TargetNames[facts.TargetOf[f]])) {
                    set.insert(facts.TargetOf[f]);
                }
            }
            std::vector<int32_t> out(set.begin(), set.end());
            std::sort(out.begin(), out.end(), [&](int32_t a, int32_t b) {
                return facts.TargetNames[a] < facts.TargetNames[b];
            });
            return out;
        }

    } // namespace

    TGroupInputs GroupInputs(const TFacts& facts, const TSelection& sel) {
        TGroupInputs out;
        for (int32_t t = 0; t < int32_t(sel.Cons.size()); ++t) {
            if (sel.Cons[t]) {
                out.Files.push_back(t);
            }
        }
        auto order = [&](int32_t h) {
            const std::string& name = facts.HeaderNames[h];
            int rank = StartsWith(name, LIBCXX) || StartsWith(name, "<sysroot>") || IsAbs(name) ? 0
                : StartsWith(name, "contrib/")                                                  ? 1
                : StartsWith(name, "util/") || StartsWith(name, "library/")                     ? 2
                : StartsWith(name, "<out>")                                                     ? 4
                                                                                                : 3;
            return std::make_pair(rank, std::cref(name));
        };
        out.Roots = SelectionRoots(facts, sel);
        std::sort(out.Roots.begin(), out.Roots.end(), [&](int32_t a, int32_t b) {
            return order(a) < order(b);
        });

        if (!out.Files.empty()) {
            const auto& first = facts.DefinesOf[out.Files[0]];
            std::set<std::string> common(first.begin(), first.end());
            for (size_t i = 1; i < out.Files.size(); ++i) {
                const auto& d = facts.DefinesOf[out.Files[i]];
                std::set<std::string> has(d.begin(), d.end());
                for (auto it = common.begin(); it != common.end();) {
                    it = has.count(*it) ? std::next(it) : common.erase(it);
                }
            }
            for (const std::string& d : first) {
                if (common.count(d)) {
                    out.Defines.push_back(d);
                }
            }
        }
        // the include dir lists of the most files first, as Counter.most_common:
        // ties in the order first seen
        std::map<std::vector<std::pair<std::string, std::string>>, std::pair<int64_t, size_t>> lists;
        for (int32_t t : out.Files) {
            auto [it, added] = lists.emplace(facts.IncludeDirsOf[t], std::make_pair(int64_t(0), lists.size()));
            ++it->second.first;
        }
        std::vector<std::pair<std::pair<int64_t, size_t>, const std::vector<std::pair<std::string, std::string>>*>> byCount;
        for (const auto& [dirs, count] : lists) {
            byCount.push_back({{-count.first, count.second}, &dirs});
        }
        std::sort(byCount.begin(), byCount.end());
        std::set<std::pair<std::string, std::string>> seen;
        for (const auto& [_, dirs] : byCount) {
            for (const auto& d : *dirs) {
                if (seen.insert(d).second) {
                    out.IncludeDirs.push_back(d);
                }
            }
        }
        return out;
    }

    bool IsContrib(const TPlan& plan, const TSelection& sel) {
        if (sel.Members.empty()) {
            return false;
        }
        return std::all_of(sel.Members.begin(), sel.Members.end(), [&](int32_t t) {
            return plan.Contrib.count(t) != 0;
        });
    }

    bool MakeGroups(const TOptions& options, TFacts& facts, TPlan& plan) {
        plan.PerTarget = options.PerTarget;
        plan.Groups.clear();
        if (options.PerTarget) {
            Log("selecting the PCH of each target...");
            // the enum serialization has no PCH in this mode: the source_set of
            // serialize_enums takes no precompiled_header; its files are of a
            // target of their own, not of the one whose PCH they take in "global"
            facts.TargetOf = facts.LabelOf;
            plan.Core = BuildModel(facts, 0);
            plan.Model = TModel(plan.Core, Q_PER_TARGET);
            NoUtil(facts, plan);
            std::vector<char> banned = ContribBanned(plan);
            int64_t considered = 0;
            for (int32_t n = 0; n < int32_t(facts.Classes.size()); ++n) {
                for (int32_t target : ClassTargets(facts, n)) {
                    ++considered;
                    auto sel = TargetSelection(plan.Model, target, plan.Contrib.count(target) ? &banned : nullptr);
                    if (sel) {
                        plan.Groups.push_back({n, std::move(sel)});
                    }
                }
            }
            Report(Format("%-18s%zu of %ld targets gain %s+ per build from a PCH of their own (q = %g)", "per target:",
                          plan.Groups.size(), long(considered), Duration(MIN_TARGET_G).c_str(), Q_PER_TARGET));
            ReportGroups(facts, plan, "targets");
            return true;
        }

        Log("forming groups...");
        plan.Core = BuildModel(facts, options.MaxGroupFiles);
        plan.Model = TModel(plan.Core, options.Q);
        NoUtil(facts, plan);
        std::vector<char> banned = ContribBanned(plan);
        Report(Format("%-18s%zu of %d targets without //util, headers banned for them %zu", "contrib:", plan.Contrib.size(),
                      TargetsWithFiles(*plan.Core, facts), plan.ContribBans.size()));
        Report(Format("%-18sq = %g, %s", "groups:", options.Q,
                      options.MaxGroupFiles ? Format("parts of the dir tree of at most %d files", options.MaxGroupFiles).c_str()
                                            : "no limit of files"));
        for (int32_t n = 0; n < int32_t(facts.Classes.size()); ++n) {
            std::vector<int32_t> targets = ClassTargets(facts, n);
            Log(Format("class %d: %zu targets...", n + 1, targets.size()));
            std::vector<int32_t> rest, own;
            for (int32_t t : targets) {
                (plan.Contrib.count(t) ? own : rest).push_back(t);
            }
            for (auto& sel : ClassGroups(facts, plan.Model, rest, nullptr, false)) {
                plan.Groups.push_back({n, std::move(sel)});
            }
            if (!own.empty()) {
                for (auto& sel : ClassGroups(facts, plan.Model, own, &banned, true)) {
                    plan.Groups.push_back({n, std::move(sel)});
                }
            }
        }
        ReportGroups(facts, plan, "groups");
        return true;
    }

    void ReportGroups(const TFacts& facts, const TPlan& plan, const std::string& title) {
        const TModelCore& core = *plan.Core;
        const auto& found = plan.Groups;
        const auto& names = facts.HeaderNames;
        std::vector<int32_t> groupOf(core.NFiles, -1);
        for (int32_t g = 0; g < int32_t(found.size()); ++g) {
            for (int32_t target : found[g].Sel->Members) {
                for (int32_t t : core.FilesOf[target]) {
                    groupOf[t] = g;
                }
            }
        }

        // the parse not in PCH: by reason; by header, as its parse with the nested ones
        constexpr int NREASONS = sizeof(REASONS) / sizeof(REASONS[0]);
        std::vector<int64_t> byReason(NREASONS, 0);
        int64_t covered = 0;
        std::vector<int64_t> residual(names.size(), 0);
        std::vector<int64_t> residualFiles(names.size(), 0);
        std::vector<std::vector<int64_t>> residualReason(names.size());
        std::vector<std::unordered_map<int32_t, int>> reasons(found.size());
        for (int32_t h = 0; h < int32_t(names.size()); ++h) {
            int32_t u = core.Comp[h];
            for (size_t i = 0; i < facts.HeaderFiles[h].size(); ++i) {
                int32_t t = facts.HeaderFiles[h][i];
                int64_t own = facts.HeaderOwn[h][i];
                int64_t parse = facts.HeaderParse[h][i];
                int32_t g = groupOf[t];
                if (g >= 0 && found[g].Sel->Selected[u]) {
                    covered += own;
                    continue;
                }
                int r = 0;
                if (g >= 0) {
                    auto it = reasons[g].find(u);
                    if (it == reasons[g].end()) {
                        it = reasons[g].emplace(u, found[g].Sel->Reason(u)).first;
                    }
                    r = it->second;
                }
                byReason[r] += own;
                residual[h] += parse;
                residualFiles[h] += 1;
                if (residualReason[h].empty()) {
                    residualReason[h].assign(NREASONS, 0);
                }
                residualReason[h][r] += parse;
            }
        }
        // Counter.most_common(1): the most, the first seen of equal ones
        auto topReason = [&](int32_t h) -> std::string {
            const auto& r = residualReason[h];
            if (r.empty()) {
                return "-";
            }
            return REASONS[std::max_element(r.begin(), r.end()) - r.begin()];
        };

        int64_t totalParse = 0;
        for (int64_t us : facts.HeaderUs) {
            totalParse += us;
        }
        std::set<int32_t> classes;
        for (const TGroup& g : found) {
            classes.insert(g.Class);
        }
        Report(Format("%-18s%zu in %zu of %zu classes; C++ files with PCH %ld of %d", (title + ":").c_str(), found.size(),
                      classes.size(), facts.Classes.size(),
                      long(std::count_if(groupOf.begin(), groupOf.end(), [](int32_t g) { return g >= 0; })), core.NFiles));
        Report("  #   class targets   files headers   roots      pch    start commits     cold     warm        G");
        std::vector<int32_t> order(found.size());
        for (int32_t g = 0; g < int32_t(found.size()); ++g) {
            order[g] = g;
        }
        std::set<int32_t> shown;
        if (plan.PerTarget) {
            std::stable_sort(order.begin(), order.end(), [&](int32_t a, int32_t b) {
                return found[a].Sel->Value > found[b].Sel->Value;
            });
            shown.insert(order.begin(), order.begin() + std::min<size_t>(order.size(), TOP_TARGETS));
        } else {
            shown.insert(order.begin(), order.end());
        }
        std::vector<std::string> details;
        for (int32_t g = 0; g < int32_t(found.size()); ++g) {
            if (!shown.count(g)) {
                continue;
            }
            const TSelection& sel = *found[g].Sel;
            std::vector<int32_t> hs = SelectedHeaders(facts, sel);
            std::vector<int32_t> roots = SelectionRoots(facts, sel);
            auto value = sel.Evaluate(sel.Parse, sel.Build, double(sel.Hit.size()), sel.HitC, sel.NonhitP, sel.NonhitN, sel.Ready);
            Report(Format("  %-3d %5d %7zu %7ld %7zu %7zu %8s %8s %7zu %8s %8s %8s%s", g + 1, found[g].Class + 1,
                          sel.Members.size(), long(sel.NCons), hs.size(), roots.size(), Duration(sel.Build).c_str(),
                          Duration(sel.Ready + sel.Build).c_str(), sel.Hit.size(), Hours(value.Cold).c_str(),
                          Hours(value.Warm).c_str(), Hours(value.G).c_str(), IsContrib(plan, sel) ? "  contrib" : ""));
            // targets by dir (4 levels), the most first, ties as first seen in sorted order
            std::vector<std::pair<std::string, int64_t>> dirs;
            std::unordered_map<std::string, size_t> dirIndex;
            std::vector<std::string> members;
            for (int32_t t : sel.Members) {
                members.push_back(facts.TargetNames[t]);
            }
            std::sort(members.begin(), members.end());
            for (const std::string& label : members) {
                std::string key = label;
                if (!plan.PerTarget) {
                    std::vector<std::string> parts;
                    for (std::string_view p : Split(std::string_view(label).substr(0, label.find(':')), '/')) {
                        parts.emplace_back(p);
                    }
                    parts.resize(std::min<size_t>(parts.size(), 4));
                    key = Join(parts, "/");
                }
                auto [it, added] = dirIndex.emplace(key, dirs.size());
                if (added) {
                    dirs.push_back({key, 0});
                }
                ++dirs[it->second].second;
            }
            std::stable_sort(dirs.begin(), dirs.end(), [](const auto& a, const auto& b) {
                return a.second > b.second;
            });
            std::vector<std::pair<int64_t, int32_t>> rootParse;
            for (int32_t h : roots) {
                int64_t us = 0;
                for (size_t i = 0; i < facts.HeaderFiles[h].size(); ++i) {
                    if (sel.Cons[facts.HeaderFiles[h][i]]) {
                        us += facts.HeaderParse[h][i];
                    }
                }
                rootParse.push_back({us, h});
            }
            std::sort(rootParse.rbegin(), rootParse.rend());
            std::vector<std::string> dirText, rootText;
            for (size_t i = 0; i < dirs.size() && i < size_t(TOP_DIRS); ++i) {
                dirText.push_back(Format("%s %ld", dirs[i].first.c_str(), long(dirs[i].second)));
            }
            for (size_t i = 0; i < rootParse.size() && i < size_t(TOP_ROOTS); ++i) {
                rootText.push_back(Display(names[rootParse[i].second]) + " " + Hours(double(rootParse[i].first)));
            }
            details.push_back(Format("  %-3d %s%s; %s", g + 1, Join(dirText, ", ").c_str(),
                                     dirs.size() > size_t(TOP_DIRS) ? ", ..." : "", Join(rootText, ", ").c_str()));
        }
        if (shown.size() < found.size()) {
            double minShown = 0, minAll = 0;
            bool first = true;
            for (int32_t g = 0; g < int32_t(found.size()); ++g) {
                double v = found[g].Sel->Value;
                if (first || v < minAll) {
                    minAll = v;
                }
                first = false;
            }
            first = true;
            for (int32_t g : shown) {
                double v = found[g].Sel->Value;
                if (first || v < minShown) {
                    minShown = v;
                }
                first = false;
            }
            Report(Format("  ... %zu more, G %s down to %s", found.size() - shown.size(), Hours(minShown).c_str(),
                          Hours(minAll).c_str()));
        }
        Report(title + ": targets by dir; roots of the most parse:");
        for (const std::string& line : details) {
            Report(line);
        }
        Report(Format("header parse:     %s; in PCH %s (%.0f%%)", Hours(double(totalParse)).c_str(), Hours(double(covered)).c_str(),
                      100.0 * double(covered) / double(std::max<int64_t>(totalParse, 1))));
        std::vector<std::string> reasonText;
        for (int r = 0; r < NREASONS; ++r) {
            reasonText.push_back(std::string(REASONS[r]) + " " + Hours(double(byReason[r])));
        }
        Report("  not in PCH:     " + Join(reasonText, ", "));
        Report("top not in PCH (parse, files, reason):");
        std::vector<int32_t> top;
        for (int32_t h = 0; h < int32_t(names.size()); ++h) {
            if (residualFiles[h]) {
                top.push_back(h);
            }
        }
        std::stable_sort(top.begin(), top.end(), [&](int32_t a, int32_t b) {
            return residual[a] > residual[b];
        });
        for (size_t i = 0; i < top.size() && i < size_t(TOP_RESIDUAL); ++i) {
            int32_t h = top[i];
            Report(Format("  %-60s %8s %7ld   %s", Display(names[h]).c_str(), Hours(double(residual[h])).c_str(),
                          long(residualFiles[h]), topReason(h).c_str()));
        }

        if (plan.PerTarget) {
            // the targets of their own PCH now
            std::set<std::string> now;
            std::string text;
            if (TryReadFile(JoinPath(facts.Root, TARGETS_GNI), &text)) {
                auto values = ParseGnFile(text);
                if (const TGnValue* list = FindGnValue(values, "pch_targets")) {
                    for (const std::string& t : list->Strings()) {
                        now.insert(t);
                    }
                }
            }
            std::set<std::string> fresh;
            for (const TGroup& g : found) {
                for (int32_t t : g.Sel->Members) {
                    fresh.insert(facts.TargetNames[t]);
                }
            }
            size_t kept = 0;
            std::vector<std::string> gone;
            for (const std::string& t : now) {
                if (fresh.count(t)) {
                    ++kept;
                } else {
                    gone.push_back(t);
                }
            }
            gone.resize(std::min<size_t>(gone.size(), 3));
            Report(Format("current pch_targets: %zu; kept %zu, new %zu, gone %zu%s", now.size(), kept, fresh.size() - kept,
                          now.size() - kept, gone.empty() ? "" : (", e.g. " + Join(gone, ", ")).c_str()));
            return;
        }

        // the includes of the PCH of //build/gn/pch/global now: the pch.h the
        // tool wrote, its includes as Spelling() writes them
        std::unordered_map<std::string, int32_t> bySpelling;
        for (int32_t h = 0; h < int32_t(names.size()); ++h) {
            bySpelling.emplace(Spelling(facts, names[h]), h);
        }
        Report("current PCH (includes: in each group, in none):");
        std::string globalDir = JoinPath(facts.Root, GLOBAL_DIR);
        std::vector<std::string> dirs = IsDir(globalDir) ? ListDir(globalDir) : std::vector<std::string>();
        std::sort(dirs.begin(), dirs.end());
        for (const std::string& d : dirs) {
            std::string text;
            std::string pch = d + "/pch.h";
            if (!IsDir(JoinPath(globalDir, d)) || !TryReadFile(JoinPath(globalDir, pch), &text)) {
                continue;
            }
            std::set<std::string> spellings;
            for (std::string_view line : Split(text, '\n')) {
                if (StartsWith(line, "#include ")) {
                    spellings.emplace(line.substr(9));
                }
            }
            std::vector<int64_t> perGroup(found.size(), 0);
            std::vector<std::tuple<int64_t, std::string, std::string>> missed;
            for (const std::string& s : spellings) {
                auto it = bySpelling.find(s);
                bool any = false;
                if (it != bySpelling.end()) {
                    for (size_t g = 0; g < found.size(); ++g) {
                        if (found[g].Sel->Selected[core.Comp[it->second]]) {
                            ++perGroup[g];
                            any = true;
                        }
                    }
                }
                if (!any) {
                    int32_t h = it == bySpelling.end() ? -1 : it->second;
                    missed.emplace_back(h >= 0 ? residual[h] : 0, s, h >= 0 ? topReason(h) : "-");
                }
            }
            std::sort(missed.rbegin(), missed.rend());
            std::vector<std::string> counts, examples;
            for (int64_t n : perGroup) {
                counts.push_back(std::to_string(n));
            }
            for (size_t i = 0; i < missed.size() && i < 3; ++i) {
                examples.push_back(Display(std::get<1>(missed[i])) + " (" + std::get<2>(missed[i]) + ")");
            }
            Report(Format("  %-16s %zu: %s, in none %zu%s", pch.c_str(), spellings.size(), Join(counts, " ").c_str(),
                          missed.size(), missed.empty() ? "" : (", e.g. " + Join(examples, "; ")).c_str()));
        }
    }

} // namespace NPchUpdate
