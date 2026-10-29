#pragma once

#include "facts.h"

#include <cstdint>
#include <memory>
#include <set>
#include <unordered_map>
#include <vector>

namespace NPchUpdate {

    inline constexpr int CORES = 52;                    // of a CI machine
    inline constexpr double LOAD_SHARE = 0.02;          // a file loading a PCH spends this share of the PCH compile
    inline constexpr int64_t MIN_GROUP_G = 60000000;    // us of G: a group saving less has no PCH
    inline constexpr int SPLIT_SEEDS = 3;               // headers tried for splitting a group
    inline constexpr int SEED_CANDIDATES = 300;         // headers of the most parse the seeds are taken from
    inline constexpr int PACKAGES = 30;                 // headers tried with the headers they include at once
    inline constexpr double Q_PER_TARGET = 0.0;         // --per-target: a developer's incremental build
    inline constexpr int64_t MIN_TARGET_G = 1000000;    // us of G of a target's own PCH

    struct TUnitFiles {
        std::vector<int32_t> Files;
        std::vector<int64_t> Own;
        std::vector<int64_t> Parse;
    };

    // The headers as units -- a header or the headers including each other --
    // with what a PCH having a unit costs and saves; the files and commits.
    struct TModelCore {
        std::vector<int32_t> Comp;
        std::vector<std::vector<int32_t>> Headers;
        std::vector<std::vector<int32_t>> Children;
        std::vector<std::vector<int32_t>> Parents;
        std::vector<char> Rootable;
        std::vector<int32_t> Pending;
        std::vector<TUnitFiles> Files;
        std::vector<int64_t> Build;
        std::vector<int64_t> TotalParse;
        std::vector<std::vector<int32_t>> Commits;
        std::vector<int64_t> Ready;
        int32_t NFiles = 0;
        std::vector<int32_t> NTotal;
        std::vector<int64_t> Compile;
        std::vector<std::vector<int32_t>> Rebuilt;
        // target -> its files
        std::vector<std::vector<int32_t>> FilesOf;
        int MaxGroupFiles = 0;

        // Of each file the units it parses; of each target, commit -> its
        // files the commit rebuilds. Made on the first pool model.
        struct TByFile {
            std::vector<std::vector<int32_t>> Units;
            std::vector<std::vector<int64_t>> Own;
            std::vector<std::vector<int64_t>> Parse;
            std::vector<std::vector<std::pair<int32_t, std::vector<int32_t>>>> RebuiltOf;
        };
        mutable std::shared_ptr<TByFile> ByFile;
        const TByFile& GetByFile() const;
    };

    // A model, or the model of the files of some targets (the units they
    // parse, the commits rebuilding them).
    class TModel {
    public:
        TModel() = default;
        explicit TModel(std::shared_ptr<const TModelCore> core, double q);

        const TModelCore& Core() const {
            return *Core_;
        }

        const TUnitFiles& Files(int32_t unit) const;
        const std::vector<std::vector<int32_t>>& Rebuilt() const;
        // The units to select from: all, or the ones of a pool.
        const std::vector<int32_t>& Units() const;
        int32_t NUnits() const {
            return int32_t(Core_->Headers.size());
        }
        int32_t NCommits() const {
            return int32_t(Core_->Rebuilt.size());
        }

        double Q = 0;

    private:
        friend TModel PoolModel(const TModel& m, const std::vector<int32_t>& targets, double q);

        std::shared_ptr<const TModelCore> Core_;
        std::shared_ptr<const std::unordered_map<int32_t, TUnitFiles>> Sparse_;
        std::shared_ptr<const std::vector<std::vector<int32_t>>> Rebuilt_;
        std::shared_ptr<const std::vector<int32_t>> Units_;
    };

    std::shared_ptr<TModelCore> BuildModel(const TFacts& facts, int maxGroupFiles);

    // The headers of the PCH of a group of files: greedily the units adding
    // most to G, each after the units it includes; then a unit with all it
    // includes at once, for the ones held back by what they include. A unit
    // that cannot be a root comes only with a unit including it.
    class TSelection {
    public:
        struct TValue {
            double G = 0;
            double Cold = 0;
            double Warm = 0;
        };

        // `run`: select; else nothing selected, units come with Accept().
        TSelection(const TModel& m, const std::set<int32_t>& members, const std::vector<char>* banned, bool run = true);

        TValue Evaluate(double parse, double build, double hit, double hitC, double nonhitP, double nonhitN, double ready) const;
        TValue Current() const;
        double Gain(const std::vector<int32_t>& units, bool withCommits = true);
        std::vector<int32_t> Accept(const std::vector<int32_t>& units);
        // target -> what it adds to G
        std::unordered_map<int32_t, double> Contributions(const std::set<int32_t>& targets) const;
        // Why a unit with parse in the files of the group is not in its PCH: an index of REASONS.
        int Reason(int32_t unit);
        std::vector<int32_t> Closure(int32_t unit) const;

        TModel M;
        std::set<int32_t> Members;
        std::vector<char> Banned;
        std::vector<char> Cons;
        int64_t NCons = 0;
        int64_t CCons = 0;
        std::vector<char> Selected;
        double Value = 0;
        int64_t Parse = 0;
        int64_t Build = 0;
        int64_t Ready = 0;
        std::set<int32_t> Hit;
        int64_t HitC = 0;
        int64_t NonhitP = 0;
        int64_t NonhitN = 0;
        std::vector<int64_t> P;

    private:
        struct TSums {
            int64_t S1 = 0;
            int64_t S2 = 0;
            int64_t Pot = 0;
            int64_t Epoch = -1;
        };

        const TSums& Sums(int32_t unit);
        int64_t CommitP(int32_t commit);
        std::vector<int32_t> WithChildren(int32_t unit) const;
        void Run();

        std::vector<int64_t> RebuiltC_;
        std::vector<int64_t> RebuiltN_;
        std::vector<int32_t> Nonhit_;
        std::vector<int32_t> Pending_;
        std::unordered_map<int32_t, TSums> Sums_;
        std::unordered_map<int32_t, std::pair<int64_t, int64_t>> CommitP_;
        int64_t HitEpoch_ = 0;
        int64_t PEpoch_ = 0;
    };

    inline constexpr const char* REASONS[] = {
        "files without PCH", "changes often", "few files for the load cost", "includes such headers",
        "dropped by the checks",
    };

    std::vector<char> BannedUnits(const TModel& m, const std::vector<int32_t>& units);
    std::vector<char> BannedHeaders(const TModel& m, const std::set<int32_t>& headers);
    std::unique_ptr<TSelection> FormGroup(const TModel& m, const std::set<int32_t>& pool, const std::vector<char>* banned);
    // A selection of the units of `headers` having no other header, none
    // banned, some file of its targets including them, for the targets of
    // `pool` gaining from it: no header added.
    std::unique_ptr<TSelection> Shrunk(const TModel& m, const std::set<int32_t>& pool, const std::set<int32_t>& headers,
                                       const std::vector<char>& banned);
    std::vector<std::unique_ptr<TSelection>> GroupsOfClass(const TModel& m, const std::vector<int32_t>& targets,
                                                           const std::vector<char>* banned);
    std::vector<std::vector<int32_t>> DirParts(const TFacts& facts, const TModel& m, const std::vector<int32_t>& targets, int cap);
    std::vector<std::unique_ptr<TSelection>> ClassGroups(const TFacts& facts, const TModel& m, const std::vector<int32_t>& targets,
                                                         const std::vector<char>* banned, bool contrib);
    TModel PoolModel(const TModel& m, const std::vector<int32_t>& targets, double q);
    // --per-target: the PCH of a target of its own if it gains MIN_TARGET_G.
    std::unique_ptr<TSelection> TargetSelection(const TModel& m, int32_t target, const std::vector<char>* banned);
    // The headers a PCH includes: of the units no selected unit includes, the
    // headers compiling alone.
    std::vector<int32_t> SelectionRoots(const TFacts& facts, const TSelection& sel);
    std::vector<int32_t> SelectedHeaders(const TFacts& facts, const TSelection& sel);

} // namespace NPchUpdate
