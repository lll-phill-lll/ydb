#pragma once

#include "facts.h"
#include "jobs.h"
#include "model.h"

#include <cstdint>
#include <memory>
#include <set>
#include <string>
#include <utility>
#include <vector>

namespace NPchUpdate {

    // The command lines of the steps.
    struct TTools {
        std::string Gn = "gn";
        std::string Ninja = "ninja";
    };

    // A group of targets with a PCH: its flag class (an index of
    // TFacts::Classes), its selection; the targets the checks keep in it.
    struct TGroup {
        int32_t Class = 0;
        std::unique_ptr<TSelection> Sel;
    };

    // What the steps from the groups on decide.
    struct TPlan {
        std::shared_ptr<TModelCore> Core;
        TModel Model;
        std::vector<TGroup> Groups;
        // The targets not depending on //util, the headers they cannot have.
        std::set<int32_t> Contrib;
        std::set<int32_t> ContribBans;
        bool PerTarget = false;
        // Of the groups written: what the checks built the PCH with (their
        // -D and -U, include dirs as gn spells them), for the gn gen check.
        std::vector<std::pair<std::string, std::pair<std::vector<std::string>, std::vector<std::string>>>> WrittenInputs;
    };

    // The inputs of the PCH of a group: the files, the roots in the order
    // the PCH includes them, the -D and -U all the files have, the include
    // dirs of the files (of the most files first).
    struct TGroupInputs {
        std::vector<int32_t> Files;
        std::vector<int32_t> Roots;
        std::vector<std::string> Defines;
        std::vector<std::pair<std::string, std::string>> IncludeDirs;
    };

    TGroupInputs GroupInputs(const TFacts& facts, const TSelection& sel);
    bool IsContrib(const TPlan& plan, const TSelection& sel);
    // An include dir of a command of the baseline as gn spells it.
    std::string GnDir(const TFacts& facts, const std::string& dir);
    // The targets `start` depend on (them too), not through an executable:
    // with pch_mode = "global" the tools are of tool_toolchain.
    std::set<std::string> Reachable(const TFacts& facts, const std::vector<std::string>& start);
    // pch_only_cflags of pch_flags.gni.
    std::vector<std::string> PchOnlyArgv(const TFacts& facts);

    // The steps: each reports its lines; false stops the run, nothing written.
    // check, gn labels, traces: the baseline as built, the facts of its files.
    bool CheckBaseline(const TOptions& options, const TTools& tools, TWorkDir& work, TFacts& facts);
    bool ReadTraces(const TOptions& options, TWorkDir& work, TFacts& facts);
    bool ReadHistory(const TOptions& options, TFacts& facts);
    bool FlagClasses(const TOptions& options, TFacts& facts);
    bool HeaderFacts(const TOptions& options, TWorkDir& work, TFacts& facts);

    bool MakeGroups(const TOptions& options, TFacts& facts, TPlan& plan);
    bool Checks(const TOptions& options, TWorkDir& work, TFacts& facts, TPlan& plan);
    void ReportGroups(const TFacts& facts, const TPlan& plan, const std::string& title);

    // Writing //build/gn/pch: a copy first, put back unless all passed.
    class TPchDirBackup {
    public:
        explicit TPchDirBackup(const TFacts& facts);
        ~TPchDirBackup();
        void Keep() {
            Keep_ = true;
        }

    private:
        std::string Root_;
        std::string Copy_;
        bool Keep_ = false;
    };

    bool Write(const TOptions& options, TFacts& facts, TPlan& plan);
    bool WritePerTarget(const TOptions& options, TFacts& facts, TPlan& plan);
    // gn gen of the tree as written, in a dir of the work dir: the PCH of
    // the build as the checks built them.
    bool GnGenCheck(const TOptions& options, const TTools& tools, TWorkDir& work, TFacts& facts, TPlan& plan);

} // namespace NPchUpdate
