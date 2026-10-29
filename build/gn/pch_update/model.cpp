#include "model.h"

#include "common.h"

#include <algorithm>
#include <functional>
#include <map>
#include <numeric>
#include <queue>
#include <tuple>

namespace NPchUpdate {

    namespace {
        const TUnitFiles NO_FILES;

        int64_t Median(std::vector<int64_t> values) {
            if (values.empty()) {
                return 0;
            }
            std::sort(values.begin(), values.end());
            return values[values.size() / 2];
        }

        // The component of each node, their number; a component is numbered
        // after the ones it reaches (Tarjan).
        int32_t StrongComponents(const std::vector<std::vector<int32_t>>& children, std::vector<int32_t>* comp) {
            int32_t n = int32_t(children.size());
            std::vector<int32_t> index(n, -1);
            std::vector<int32_t> low(n, 0);
            comp->assign(n, -1);
            std::vector<char> onStack(n, 0);
            std::vector<int32_t> stack;
            std::vector<std::pair<int32_t, size_t>> work;
            int32_t counter = 0;
            int32_t count = 0;
            for (int32_t root = 0; root < n; ++root) {
                if (index[root] != -1) {
                    continue;
                }
                index[root] = low[root] = counter++;
                stack.push_back(root);
                onStack[root] = 1;
                work.emplace_back(root, 0);
                while (!work.empty()) {
                    auto& [v, i] = work.back();
                    if (i < children[v].size()) {
                        int32_t w = children[v][i++];
                        if (index[w] == -1) {
                            index[w] = low[w] = counter++;
                            stack.push_back(w);
                            onStack[w] = 1;
                            work.emplace_back(w, 0);
                        } else if (onStack[w]) {
                            low[v] = std::min(low[v], index[w]);
                        }
                        continue;
                    }
                    int32_t done = v;
                    work.pop_back();
                    if (!work.empty()) {
                        int32_t u = work.back().first;
                        low[u] = std::min(low[u], low[done]);
                    }
                    if (low[done] == index[done]) {
                        int32_t w = -1;
                        while (w != done) {
                            w = stack.back();
                            stack.pop_back();
                            onStack[w] = 0;
                            (*comp)[w] = count;
                        }
                        ++count;
                    }
                }
            }
            return count;
        }

        // Units ordered by total parse, most first; ties as given.
        std::vector<int32_t> ByTotalParse(const TModelCore& core, std::vector<int32_t> units, size_t limit) {
            std::stable_sort(units.begin(), units.end(), [&](int32_t a, int32_t b) {
                return core.TotalParse[a] > core.TotalParse[b];
            });
            if (units.size() > limit) {
                units.resize(limit);
            }
            return units;
        }

        std::vector<int32_t> AllUnits(int32_t n) {
            std::vector<int32_t> out(n);
            std::iota(out.begin(), out.end(), 0);
            return out;
        }

        std::set<int32_t> Gaining(const TSelection& sel, const std::set<int32_t>& members) {
            std::unordered_map<int32_t, double> contrib = sel.Contributions(members);
            std::set<int32_t> out;
            for (int32_t t : members) {
                auto it = contrib.find(t);
                if (it != contrib.end() && it->second > 0) {
                    out.insert(t);
                }
            }
            return out;
        }

        // The units of the most parse in the files of `sel` not in its PCH.
        std::vector<int32_t> SplitSeeds(const TModel& m, const TSelection& sel) {
            const TModelCore& core = m.Core();
            std::vector<std::pair<int32_t, int64_t>> pot;
            for (int32_t u : ByTotalParse(core, AllUnits(m.NUnits()), SEED_CANDIDATES)) {
                if (sel.Selected[u]) {
                    continue;
                }
                const TUnitFiles& f = m.Files(u);
                int64_t sum = 0;
                for (size_t i = 0; i < f.Files.size(); ++i) {
                    if (sel.Cons[f.Files[i]]) {
                        sum += f.Parse[i];
                    }
                }
                pot.emplace_back(u, sum);
            }
            std::stable_sort(pot.begin(), pot.end(), [](const auto& a, const auto& b) {
                return a.second > b.second;
            });
            std::vector<int32_t> out;
            for (size_t i = 0; i < pot.size() && i < size_t(SPLIT_SEEDS); ++i) {
                out.push_back(pot[i].first);
            }
            return out;
        }

        std::string DirOf(const std::string& label) {
            std::string_view l = label;
            l.remove_prefix(2);
            return std::string(l.substr(0, l.find(':')));
        }
    } // namespace

    const TModelCore::TByFile& TModelCore::GetByFile() const {
        if (!ByFile) {
            auto b = std::make_shared<TByFile>();
            b->Units.resize(NFiles);
            b->Own.resize(NFiles);
            b->Parse.resize(NFiles);
            for (int32_t u = 0; u < int32_t(Files.size()); ++u) {
                const TUnitFiles& f = Files[u];
                for (size_t i = 0; i < f.Files.size(); ++i) {
                    int32_t t = f.Files[i];
                    b->Units[t].push_back(u);
                    b->Own[t].push_back(f.Own[i]);
                    b->Parse[t].push_back(f.Parse[i]);
                }
            }
            std::vector<int32_t> targetOf(NFiles, -1);
            for (int32_t target = 0; target < int32_t(FilesOf.size()); ++target) {
                for (int32_t t : FilesOf[target]) {
                    targetOf[t] = target;
                }
            }
            b->RebuiltOf.resize(FilesOf.size());
            for (int32_t k = 0; k < int32_t(Rebuilt.size()); ++k) {
                for (int32_t t : Rebuilt[k]) {
                    auto& of = b->RebuiltOf[targetOf[t]];
                    if (of.empty() || of.back().first != k) {
                        of.emplace_back(k, std::vector<int32_t>());
                    }
                    of.back().second.push_back(t);
                }
            }
            ByFile = b;
        }
        return *ByFile;
    }

    TModel::TModel(std::shared_ptr<const TModelCore> core, double q)
        : Q(q)
        , Core_(std::move(core))
        , Units_(std::make_shared<std::vector<int32_t>>(AllUnits(int32_t(Core_->Headers.size()))))
    {
    }

    const TUnitFiles& TModel::Files(int32_t unit) const {
        if (!Sparse_) {
            return Core_->Files[unit];
        }
        auto it = Sparse_->find(unit);
        return it == Sparse_->end() ? NO_FILES : it->second;
    }

    const std::vector<std::vector<int32_t>>& TModel::Rebuilt() const {
        return Rebuilt_ ? *Rebuilt_ : Core_->Rebuilt;
    }

    const std::vector<int32_t>& TModel::Units() const {
        return *Units_;
    }

    std::shared_ptr<TModelCore> BuildModel(const TFacts& facts, int maxGroupFiles) {
        auto core = std::make_shared<TModelCore>();
        int32_t n = int32_t(facts.HeaderNames.size());
        std::vector<uint64_t> edges(facts.HeaderEdges.begin(), facts.HeaderEdges.end());
        std::sort(edges.begin(), edges.end());
        std::vector<std::vector<int32_t>> children(n);
        for (uint64_t x : edges) {
            children[x >> 32].push_back(int32_t(x & 0xFFFFFFFFu));
        }
        int32_t count = StrongComponents(children, &core->Comp);
        core->Headers.resize(count);
        for (int32_t h = 0; h < n; ++h) {
            core->Headers[core->Comp[h]].push_back(h);
        }
        std::vector<std::set<int32_t>> unitChildren(count);
        for (uint64_t x : edges) {
            int32_t a = core->Comp[x >> 32];
            int32_t b = core->Comp[x & 0xFFFFFFFFu];
            if (a != b) {
                unitChildren[a].insert(b);
            }
        }
        core->Children.resize(count);
        core->Parents.resize(count);
        for (int32_t u = 0; u < count; ++u) {
            core->Children[u].assign(unitChildren[u].begin(), unitChildren[u].end());
            for (int32_t v : unitChildren[u]) {
                core->Parents[v].push_back(u);
            }
        }

        core->Files.resize(count);
        core->Build.resize(count);
        core->TotalParse.resize(count);
        for (int32_t u = 0; u < count; ++u) {
            const auto& hs = core->Headers[u];
            TUnitFiles& f = core->Files[u];
            if (hs.size() == 1) {
                f.Files = facts.HeaderFiles[hs[0]];
                f.Own = facts.HeaderOwn[hs[0]];
                f.Parse = facts.HeaderParse[hs[0]];
            } else {
                std::map<int32_t, std::pair<int64_t, int64_t>> merged;
                for (int32_t h : hs) {
                    for (size_t i = 0; i < facts.HeaderFiles[h].size(); ++i) {
                        auto& m = merged[facts.HeaderFiles[h][i]];
                        m.first += facts.HeaderOwn[h][i];
                        m.second = std::max(m.second, facts.HeaderParse[h][i]);
                    }
                }
                for (const auto& [t, op] : merged) {
                    f.Files.push_back(t);
                    f.Own.push_back(op.first);
                    f.Parse.push_back(op.second);
                }
            }
            int64_t build = 0;
            for (int32_t h : hs) {
                build += Median(facts.HeaderOwn[h]);
            }
            core->Build[u] = build;
            core->TotalParse[u] = std::accumulate(f.Parse.begin(), f.Parse.end(), int64_t(0));
        }

        core->Commits.resize(count);
        for (int32_t k = 0; k < int32_t(facts.CommitFiles.size()); ++k) {
            for (const std::string& file : facts.CommitFiles[k]) {
                auto it = facts.HeaderId.find(file);
                if (it != facts.HeaderId.end()) {
                    auto& c = core->Commits[core->Comp[it->second]];
                    if (c.empty() || c.back() != k) {
                        c.push_back(k);
                    }
                }
            }
        }
        core->Ready.assign(count, 0);
        for (int32_t u = 0; u < count; ++u) {
            for (int32_t h : core->Headers[u]) {
                auto it = facts.ReadyOf.find(facts.HeaderNames[h]);
                if (it != facts.ReadyOf.end()) {
                    core->Ready[u] = std::max(core->Ready[u], it->second);
                }
            }
        }

        core->NFiles = int32_t(facts.Objs.size());
        core->NTotal.assign(core->NFiles, 0);
        for (const auto& rebuilt : facts.Rebuilt) {
            for (int32_t t : rebuilt) {
                ++core->NTotal[t];
            }
        }
        core->Rebuilt = facts.Rebuilt;
        core->Compile = facts.CompileUs;
        core->FilesOf.assign(facts.TargetNames.size(), {});
        for (int32_t t = 0; t < core->NFiles; ++t) {
            core->FilesOf[facts.TargetOf[t]].push_back(t);
        }
        // a unit can be a root of a PCH if a header of it compiles alone
        core->Rootable.assign(count, 1);
        if (!facts.SelfContained.empty()) {
            for (int32_t u = 0; u < count; ++u) {
                core->Rootable[u] = std::any_of(core->Headers[u].begin(), core->Headers[u].end(), [&](int32_t h) {
                    return facts.SelfContained[h] != 0;
                });
            }
        }
        // of each unit, the units it includes that can be a root: a selection starts from a copy
        core->Pending.assign(count, 0);
        for (int32_t u = 0; u < count; ++u) {
            for (int32_t v : core->Children[u]) {
                core->Pending[u] += core->Rootable[v];
            }
        }
        core->MaxGroupFiles = maxGroupFiles;
        return core;
    }

    TSelection::TSelection(const TModel& m, const std::set<int32_t>& members, const std::vector<char>* banned, bool run)
        : M(m)
        , Members(members)
    {
        const TModelCore& core = M.Core();
        Banned = banned ? *banned : std::vector<char>(M.NUnits(), 0);
        Cons.assign(core.NFiles, 0);
        for (int32_t target : Members) {
            for (int32_t t : core.FilesOf[target]) {
                Cons[t] = 1;
                ++NCons;
                CCons += core.Compile[t];
            }
        }
        const auto& rebuilt = M.Rebuilt();
        RebuiltC_.assign(rebuilt.size(), 0);
        RebuiltN_.assign(rebuilt.size(), 0);
        for (size_t k = 0; k < rebuilt.size(); ++k) {
            for (int32_t t : rebuilt[k]) {
                if (Cons[t]) {
                    RebuiltC_[k] += core.Compile[t];
                    ++RebuiltN_[k];
                }
            }
            NonhitN += RebuiltN_[k];
        }
        Nonhit_ = core.NTotal;
        P.assign(core.NFiles, 0);
        Selected.assign(M.NUnits(), 0);
        Pending_ = core.Pending;
        if (run) {
            Run();
        }
    }

    TSelection::TValue TSelection::Evaluate(double parse, double build, double hit, double hitC, double nonhitP,
                                            double nonhitN, double ready) const
    {
        double saved = parse - LOAD_SHARE * build * NCons;
        double cold = saved - build - CORES * (ready + build);
        double warm = (hit * (saved - CCons - build) + hitC + nonhitP - LOAD_SHARE * build * nonhitN) /
                      std::max(M.NCommits(), 1);
        return {M.Q * cold + (1 - M.Q) * warm, cold, warm};
    }

    TSelection::TValue TSelection::Current() const {
        return Evaluate(Parse, Build, Hit.size(), HitC, NonhitP, NonhitN, Ready);
    }

    const TSelection::TSums& TSelection::Sums(int32_t unit) {
        TSums& s = Sums_[unit];
        if (s.Epoch != HitEpoch_) {
            s = TSums();
            const TUnitFiles& f = M.Files(unit);
            for (size_t i = 0; i < f.Files.size(); ++i) {
                int32_t t = f.Files[i];
                if (Cons[t]) {
                    s.S1 += f.Own[i];
                    s.S2 += f.Own[i] * Nonhit_[t];
                    s.Pot += f.Parse[i];
                }
            }
            s.Epoch = HitEpoch_;
        }
        return s;
    }

    int64_t TSelection::CommitP(int32_t commit) {
        auto it = CommitP_.find(commit);
        if (it != CommitP_.end() && it->second.second == PEpoch_) {
            return it->second.first;
        }
        int64_t sum = 0;
        for (int32_t t : M.Rebuilt()[commit]) {
            if (Cons[t]) {
                sum += P[t];
            }
        }
        CommitP_[commit] = {sum, PEpoch_};
        return sum;
    }

    double TSelection::Gain(const std::vector<int32_t>& units, bool withCommits) {
        const TModelCore& core = M.Core();
        int64_t s1 = 0;
        int64_t s2 = 0;
        int64_t build = 0;
        int64_t ready = Ready;
        std::set<int32_t> fresh;
        for (int32_t v : units) {
            const TSums& s = Sums(v);
            s1 += s.S1;
            s2 += s.S2;
            build += core.Build[v];
            ready = std::max(ready, core.Ready[v]);
            if (withCommits) {
                for (int32_t k : core.Commits[v]) {
                    if (!Hit.count(k)) {
                        fresh.insert(k);
                    }
                }
            }
        }
        int64_t hitC = HitC;
        int64_t nonhitP = NonhitP;
        int64_t nonhitN = NonhitN;
        for (int32_t k : fresh) {
            hitC += RebuiltC_[k];
            nonhitP -= CommitP(k);
            nonhitN -= RebuiltN_[k];
        }
        nonhitP += s2 - std::min<int64_t>(s2, int64_t(fresh.size()) * s1);
        TValue v = Evaluate(Parse + s1, Build + build, Hit.size() + fresh.size(), hitC, nonhitP, nonhitN, ready);
        return v.G - Value;
    }

    std::vector<int32_t> TSelection::Accept(const std::vector<int32_t>& units) {
        const TModelCore& core = M.Core();
        std::set<int32_t> fresh;
        for (int32_t v : units) {
            for (int32_t k : core.Commits[v]) {
                if (!Hit.count(k)) {
                    fresh.insert(k);
                }
            }
        }
        int64_t oldP = 0;
        for (int32_t k : fresh) {
            oldP += CommitP(k);
        }
        for (int32_t k : fresh) {
            for (int32_t t : M.Rebuilt()[k]) {
                if (Cons[t]) {
                    --Nonhit_[t];
                }
            }
            HitC += RebuiltC_[k];
            NonhitN -= RebuiltN_[k];
        }
        if (!fresh.empty()) {
            Hit.insert(fresh.begin(), fresh.end());
            ++HitEpoch_;
        }
        int64_t added = 0;
        std::vector<int32_t> eligible;
        for (int32_t v : units) {
            Parse += Sums(v).S1;
            Build += core.Build[v];
            Ready = std::max(Ready, core.Ready[v]);
            const TUnitFiles& f = M.Files(v);
            for (size_t i = 0; i < f.Files.size(); ++i) {
                int32_t t = f.Files[i];
                P[t] += f.Own[i];
                if (Cons[t]) {
                    added += f.Own[i] * Nonhit_[t];
                }
            }
            Selected[v] = 1;
        }
        for (int32_t v : units) {
            if (!core.Rootable[v]) {
                continue;
            }
            for (int32_t u : core.Parents[v]) {
                if (!--Pending_[u] && !Selected[u]) {
                    eligible.push_back(u);
                }
            }
        }
        NonhitP += added - oldP;
        ++PEpoch_;
        Value = Current().G;
        return eligible;
    }

    std::vector<int32_t> TSelection::Closure(int32_t unit) const {
        const TModelCore& core = M.Core();
        std::set<int32_t> seen = {unit};
        std::vector<int32_t> stack = {unit};
        while (!stack.empty()) {
            int32_t u = stack.back();
            stack.pop_back();
            for (int32_t v : core.Children[u]) {
                if (!Selected[v] && seen.insert(v).second) {
                    stack.push_back(v);
                }
            }
        }
        return {seen.begin(), seen.end()};
    }

    std::vector<int32_t> TSelection::WithChildren(int32_t unit) const {
        const TModelCore& core = M.Core();
        bool plain = std::all_of(core.Children[unit].begin(), core.Children[unit].end(), [&](int32_t v) {
            return core.Rootable[v] || Selected[v];
        });
        return plain ? std::vector<int32_t>{unit} : Closure(unit);
    }

    void TSelection::Run() {
        const TModelCore& core = M.Core();
        // (-gain, unit, epoch): the most gain first, then the least unit
        using TEntry = std::tuple<double, int32_t, int64_t>;
        std::priority_queue<TEntry, std::vector<TEntry>, std::greater<TEntry>> heap;
        auto push = [&](const std::vector<int32_t>& units) {
            for (int32_t u : units) {
                if (core.Rootable[u] && !Banned[u] && Sums(u).S1 > 0) {
                    heap.emplace(-Gain(WithChildren(u)), u, PEpoch_);
                }
            }
        };
        const std::vector<int32_t>& units = M.Units();
        std::vector<int32_t> first;
        for (int32_t u : units) {
            if (!Pending_[u]) {
                first.push_back(u);
            }
        }
        push(first);
        std::vector<int32_t> candidates = ByTotalParse(core, units, SEED_CANDIDATES);
        while (true) {
            while (!heap.empty()) {
                auto [g, u, epoch] = heap.top();
                heap.pop();
                if (Selected[u]) {
                    continue;
                }
                if (epoch != PEpoch_) {
                    heap.emplace(-Gain(WithChildren(u)), u, PEpoch_);
                    continue;
                }
                if (-g <= 0) {
                    heap.emplace(g, u, epoch);
                    break;
                }
                push(Accept(WithChildren(u)));
            }
            std::vector<int32_t> open;
            for (int32_t u : candidates) {
                if (!Selected[u] && !Banned[u] && core.Rootable[u]) {
                    open.push_back(u);
                }
            }
            for (int32_t u : open) {
                Sums(u);
            }
            std::stable_sort(open.begin(), open.end(), [&](int32_t a, int32_t b) {
                return Sums_[a].Pot > Sums_[b].Pot;
            });
            double best = 0;
            std::vector<int32_t> bestUnits;
            bool found = false;
            for (size_t i = 0; i < open.size() && i < size_t(PACKAGES); ++i) {
                std::vector<int32_t> closure = Closure(open[i]);
                double g = Gain(closure);
                if (g > best) {
                    best = g;
                    bestUnits = std::move(closure);
                    found = true;
                }
            }
            if (!found) {
                break;
            }
            push(Accept(bestUnits));
        }
    }

    std::unordered_map<int32_t, double> TSelection::Contributions(const std::set<int32_t>& targets) const {
        const TModelCore& core = M.Core();
        std::unordered_map<int32_t, int32_t> hits;
        for (int32_t k : Hit) {
            for (int32_t t : M.Rebuilt()[k]) {
                ++hits[t];
            }
        }
        double ncommits = std::max(M.NCommits(), 1);
        double nhit = Hit.size();
        double load = LOAD_SHARE * Build;
        std::unordered_map<int32_t, double> out;
        for (int32_t target : targets) {
            double total = 0;
            for (int32_t t : core.FilesOf[target]) {
                double s = P[t] - load;
                auto it = hits.find(t);
                double h = it == hits.end() ? 0 : it->second;
                total += M.Q * s + (1 - M.Q) * (s * (nhit + core.NTotal[t] - h) - core.Compile[t] * (nhit - h)) / ncommits;
            }
            out[target] = total;
        }
        return out;
    }

    int TSelection::Reason(int32_t unit) {
        if (Banned[unit]) {
            return 4;
        }
        bool changes = false;
        for (int32_t k : M.Core().Commits[unit]) {
            if (!Hit.count(k)) {
                changes = true;
                break;
            }
        }
        double g = Gain({unit}, false);
        if (changes && g > 0) {
            return 1;
        }
        if (g <= 0) {
            return 2;
        }
        return 3;
    }

    std::vector<char> BannedUnits(const TModel& m, const std::vector<int32_t>& units) {
        std::vector<char> out(m.NUnits(), 0);
        std::vector<int32_t> stack(units.begin(), units.end());
        while (!stack.empty()) {
            int32_t u = stack.back();
            stack.pop_back();
            if (!out[u]) {
                out[u] = 1;
                stack.insert(stack.end(), m.Core().Parents[u].begin(), m.Core().Parents[u].end());
            }
        }
        return out;
    }

    std::vector<char> BannedHeaders(const TModel& m, const std::set<int32_t>& headers) {
        std::set<int32_t> units;
        for (int32_t h : headers) {
            units.insert(m.Core().Comp[h]);
        }
        return BannedUnits(m, {units.begin(), units.end()});
    }

    std::unique_ptr<TSelection> FormGroup(const TModel& m, const std::set<int32_t>& pool, const std::vector<char>* banned) {
        std::set<int32_t> members = pool;
        while (true) {
            auto sel = std::make_unique<TSelection>(m, members, banned);
            std::set<int32_t> fresh = Gaining(*sel, members);
            if (fresh == members || fresh.empty()) {
                return sel;
            }
            members = std::move(fresh);
        }
    }

    std::unique_ptr<TSelection> Shrunk(const TModel& m, const std::set<int32_t>& pool, const std::set<int32_t>& headers,
                                       const std::vector<char>& banned)
    {
        const TModelCore& core = m.Core();
        std::set<int32_t> units;
        for (int32_t h : headers) {
            units.insert(core.Comp[h]);
        }
        std::vector<int32_t> kept;
        for (int32_t u : units) {
            bool whole = std::all_of(core.Headers[u].begin(), core.Headers[u].end(), [&](int32_t h) {
                return headers.count(h) > 0;
            });
            if (!banned[u] && whole) {
                kept.push_back(u);
            }
        }
        std::set<int32_t> members = pool;
        while (true) {
            std::vector<char> files(core.NFiles, 0);
            for (int32_t target : members) {
                for (int32_t t : core.FilesOf[target]) {
                    files[t] = 1;
                }
            }
            auto sel = std::make_unique<TSelection>(m, members, &banned, false);
            std::vector<int32_t> taken;
            for (int32_t u : kept) {
                const TUnitFiles& f = m.Files(u);
                if (std::any_of(f.Files.begin(), f.Files.end(), [&](int32_t t) {
                        return files[t] != 0;
                    }))
                {
                    taken.push_back(u);
                }
            }
            sel->Accept(taken);
            std::set<int32_t> fresh = Gaining(*sel, members);
            if (fresh == members || fresh.empty()) {
                return sel;
            }
            members = std::move(fresh);
        }
    }

    std::vector<std::unique_ptr<TSelection>> GroupsOfClass(const TModel& m, const std::vector<int32_t>& targets,
                                                           const std::vector<char>* banned)
    {
        const TModelCore& core = m.Core();
        std::set<int32_t> pool(targets.begin(), targets.end());
        std::vector<std::unique_ptr<TSelection>> out;
        while (!pool.empty()) {
            auto sel = FormGroup(m, pool, banned);
            if (sel->Value < MIN_GROUP_G) {
                break;
            }
            for (int32_t t : sel->Members) {
                pool.erase(t);
            }
            out.push_back(std::move(sel));
        }
        size_t i = 0;
        while (i < out.size()) {
            TSelection& sel = *out[i];
            double bestValue = 0;
            std::vector<std::unique_ptr<TSelection>> best;
            bool found = false;
            for (int32_t u : SplitSeeds(m, sel)) {
                std::vector<char> files(core.NFiles, 0);
                for (int32_t t : m.Files(u).Files) {
                    files[t] = 1;
                }
                std::set<int32_t> sub;
                std::set<int32_t> rest;
                for (int32_t target : sel.Members) {
                    const auto& fs = core.FilesOf[target];
                    size_t in = std::count_if(fs.begin(), fs.end(), [&](int32_t t) {
                        return files[t] != 0;
                    });
                    (2 * in >= fs.size() ? sub : rest).insert(target);
                }
                if (sub.empty() || rest.empty()) {
                    continue;
                }
                std::vector<std::unique_ptr<TSelection>> parts;
                parts.push_back(FormGroup(m, sub, banned));
                parts.push_back(FormGroup(m, rest, banned));
                double value = 0;
                for (const auto& p : parts) {
                    if (p->Value >= MIN_GROUP_G) {
                        value += p->Value;
                    }
                }
                if (value > sel.Value && (!found || value > bestValue)) {
                    found = true;
                    bestValue = value;
                    best.clear();
                    for (auto& p : parts) {
                        if (p->Value >= MIN_GROUP_G) {
                            best.push_back(std::move(p));
                        }
                    }
                }
            }
            if (found) {
                out.erase(out.begin() + i);
                for (size_t j = 0; j < best.size(); ++j) {
                    out.insert(out.begin() + i + j, std::move(best[j]));
                }
            } else {
                ++i;
            }
        }
        return out;
    }

    std::vector<std::vector<int32_t>> DirParts(const TFacts& facts, const TModel& m, const std::vector<int32_t>& targets, int cap) {
        const TModelCore& core = m.Core();
        struct TNode {
            std::map<std::string, TNode> Children;
            std::vector<int32_t> Leaves;
        };
        TNode root;
        for (int32_t t : targets) {
            TNode* node = &root;
            for (std::string_view part : Split(DirOf(facts.TargetNames[t]), '/')) {
                node = &node->Children[std::string(part)];
            }
            node->Leaves.push_back(t);
        }
        std::function<int64_t(const TNode&)> size = [&](const TNode& node) {
            int64_t n = 0;
            for (int32_t t : node.Leaves) {
                n += core.FilesOf[t].size();
            }
            for (const auto& [_, child] : node.Children) {
                n += size(child);
            }
            return n;
        };
        std::function<void(const TNode&, std::vector<int32_t>*)> all = [&](const TNode& node, std::vector<int32_t>* out) {
            out->insert(out->end(), node.Leaves.begin(), node.Leaves.end());
            for (const auto& [_, child] : node.Children) {
                all(child, out);
            }
        };
        using TParts = std::vector<std::pair<int64_t, std::vector<int32_t>>>;
        std::function<TParts(const TNode&)> pack = [&](const TNode& node) {
            int64_t n = size(node);
            if (n <= cap) {
                std::vector<int32_t> ts;
                all(node, &ts);
                return TParts{{n, ts}};
            }
            std::vector<int32_t> leaves = node.Leaves;
            std::stable_sort(leaves.begin(), leaves.end(), [&](int32_t a, int32_t b) {
                return core.FilesOf[a].size() > core.FilesOf[b].size();
            });
            TParts parts;
            for (int32_t t : leaves) {
                parts.push_back({int64_t(core.FilesOf[t].size()), {t}});
            }
            for (const auto& [_, child] : node.Children) {
                TParts sub = pack(child);
                parts.insert(parts.end(), sub.begin(), sub.end());
            }
            TParts out;
            for (auto& [k, ts] : parts) {
                if (!out.empty() && out.back().first + k <= cap) {
                    out.back().first += k;
                    out.back().second.insert(out.back().second.end(), ts.begin(), ts.end());
                } else {
                    out.push_back({k, ts});
                }
            }
            return out;
        };
        std::vector<std::vector<int32_t>> out;
        for (auto& [_, ts] : pack(root)) {
            out.push_back(std::move(ts));
        }
        return out;
    }

    std::vector<std::unique_ptr<TSelection>> ClassGroups(const TFacts& facts, const TModel& m, const std::vector<int32_t>& targets,
                                                         const std::vector<char>* banned, bool contrib)
    {
        int cap = m.Core().MaxGroupFiles;
        std::vector<std::unique_ptr<TSelection>> out;
        if (!cap) {
            if (!contrib) {
                return GroupsOfClass(m, targets, banned);
            }
            auto sel = FormGroup(m, {targets.begin(), targets.end()}, banned);
            if (sel->Value >= MIN_GROUP_G) {
                out.push_back(std::move(sel));
            }
            return out;
        }
        for (const auto& part : DirParts(facts, m, targets, cap)) {
            std::set<int32_t> pool(part.begin(), part.end());
            while (!pool.empty()) {
                auto sel = FormGroup(PoolModel(m, {pool.begin(), pool.end()}, m.Q), pool, banned);
                if (sel->Value < MIN_GROUP_G) {
                    break;
                }
                for (int32_t t : sel->Members) {
                    pool.erase(t);
                }
                out.push_back(std::move(sel));
            }
        }
        return out;
    }

    TModel PoolModel(const TModel& m, const std::vector<int32_t>& targets, double q) {
        const TModelCore& core = m.Core();
        TModel sub(m.Core_, q);
        int64_t files = 0;
        for (int32_t t : targets) {
            files += core.FilesOf[t].size();
        }
        // a selection over the files of the pool is the same as over all,
        // made faster -- but for half the files or more
        if (files * 2 > core.NFiles) {
            return sub;
        }
        const auto& byFile = core.GetByFile();
        auto sparse = std::make_shared<std::unordered_map<int32_t, TUnitFiles>>();
        for (int32_t target : targets) {
            for (int32_t t : core.FilesOf[target]) {
                for (size_t i = 0; i < byFile.Units[t].size(); ++i) {
                    TUnitFiles& f = (*sparse)[byFile.Units[t][i]];
                    f.Files.push_back(t);
                    f.Own.push_back(byFile.Own[t][i]);
                    f.Parse.push_back(byFile.Parse[t][i]);
                }
            }
        }
        auto rebuilt = std::make_shared<std::vector<std::vector<int32_t>>>(m.NCommits());
        for (int32_t target : targets) {
            for (const auto& [k, ts] : byFile.RebuiltOf[target]) {
                (*rebuilt)[k].insert((*rebuilt)[k].end(), ts.begin(), ts.end());
            }
        }
        auto units = std::make_shared<std::vector<int32_t>>();
        for (const auto& [u, _] : *sparse) {
            units->push_back(u);
        }
        std::sort(units->begin(), units->end());
        sub.Sparse_ = sparse;
        sub.Rebuilt_ = rebuilt;
        sub.Units_ = units;
        return sub;
    }

    std::unique_ptr<TSelection> TargetSelection(const TModel& m, int32_t target, const std::vector<char>* banned) {
        const TModelCore& core = m.Core();
        const auto& byFile = core.GetByFile();
        double bound = 0;
        for (int32_t t : core.FilesOf[target]) {
            int64_t own = std::accumulate(byFile.Own[t].begin(), byFile.Own[t].end(), int64_t(0));
            bound += double(own) * core.NTotal[t];
        }
        bound /= std::max(m.NCommits(), 1);
        if (bound < MIN_TARGET_G) {
            return nullptr;
        }
        auto sel = std::make_unique<TSelection>(PoolModel(m, {target}, Q_PER_TARGET), std::set<int32_t>{target}, banned);
        if (sel->Value < MIN_TARGET_G) {
            return nullptr;
        }
        return sel;
    }

    std::vector<int32_t> SelectionRoots(const TFacts& facts, const TSelection& sel) {
        const TModelCore& core = sel.M.Core();
        std::vector<int32_t> out;
        for (int32_t u = 0; u < sel.M.NUnits(); ++u) {
            if (!sel.Selected[u]) {
                continue;
            }
            bool nested = std::any_of(core.Parents[u].begin(), core.Parents[u].end(), [&](int32_t q) {
                return sel.Selected[q] != 0;
            });
            if (nested) {
                continue;
            }
            for (int32_t h : core.Headers[u]) {
                if (facts.SelfContained.empty() || facts.SelfContained[h]) {
                    out.push_back(h);
                }
            }
        }
        return out;
    }

    std::vector<int32_t> SelectedHeaders(const TFacts& facts, const TSelection& sel) {
        std::vector<int32_t> out;
        for (int32_t h = 0; h < int32_t(facts.HeaderNames.size()); ++h) {
            if (sel.Selected[sel.M.Core().Comp[h]]) {
                out.push_back(h);
            }
        }
        return out;
    }

} // namespace NPchUpdate
