#include "ninja.h"
#include "steps.h"

#include <algorithm>
#include <cstdio>
#include <cstdlib>
#include <ctime>
#include <thread>

namespace NPchUpdate {

    namespace {

        constexpr int KEEP_LOGS = 10;

        const char* USAGE =
            "pch_update update --baseline DIR [options]: the PCH of build/gn/pch from a build of the baseline\n"
            "  --baseline DIR            build dir with pch_mode = \"none\", time_trace = true\n"
            "  --targets T...            what was built there (all tests)\n"
            "  --per-target              a PCH of its own for each target gaining from one\n"
            "  --q Q                     share of the builds from scratch in G (0)\n"
            "  --max-group-files N       C++ files of a group at most, 0: no limit (500)\n"
            "  --max-missing-traces X    share of the C++ objects that may lack a trace (0.01)\n"
            "  -j N                      jobs of the work dir's ninja (min(24, cpus))\n"
            "  --bisect-ways N           roots tried at once searching the root breaking a file (4)\n"
            "  --main-ref REF            main, for the merge base of HEAD (origin/main)\n"
            "  --root DIR                the source tree written (the one of .gn above the current dir)\n"
            "  --work-dir DIR            the jobs, their results, the logs (~/.cache/pch_update-cpp)\n"
            "  --gn PATH, --ninja PATH   the tools (gn, ninja)\n"
            "pch_update job KIND RESULT: a job of the work dir's manifest\n";

        [[noreturn]] void Usage(const std::string& error) {
            fprintf(stderr, "pch_update: %s\n%s", error.c_str(), USAGE);
            exit(2);
        }

        // The dir of .gn at or above `dir`, as gn finds its root.
        std::string GnRoot(std::string dir) {
            dir = RealPath(dir);
            while (!Exists(JoinPath(dir, ".gn"))) {
                if (dir == "/") {
                    Fail("no .gn at or above the current dir: --root");
                }
                dir = DirName(dir);
            }
            return dir;
        }

        void StartLog(const std::string& workDir) {
            std::string dir = JoinPath(workDir, "logs");
            MakeDirs(dir);
            char name[64];
            time_t now = time(nullptr);
            strftime(name, sizeof(name), "%Y%m%d-%H%M%S.log", localtime(&now));
            std::string path = JoinPath(dir, name);
            OpenRunLog(path);
            std::vector<std::string> logs = ListDir(dir);
            std::sort(logs.begin(), logs.end());
            for (size_t i = 0; i + KEEP_LOGS < logs.size(); ++i) {
                RemoveFile(JoinPath(dir, logs[i]));
            }
            Log("log: " + path);
        }

        struct TArgs {
            TOptions Options;
            TTools Tools;
            std::string Root;
        };

        TArgs ParseUpdate(int argc, char** argv) {
            TArgs a;
            TOptions& o = a.Options;
            o.Jobs = int(std::min(24u, std::max(1u, std::thread::hardware_concurrency())));
            const char* cache = getenv("XDG_CACHE_HOME");
            const char* home = getenv("HOME");
            o.WorkDir = cache && *cache ? JoinPath(cache, "pch_update-cpp")
                                        : JoinPath(home ? home : "/", ".cache/pch_update-cpp");
            bool targets = false;
            for (int i = 2; i < argc; ++i) {
                std::string arg = argv[i];
                auto value = [&]() -> std::string {
                    if (i + 1 >= argc) {
                        Usage(arg + ": a value expected");
                    }
                    return argv[++i];
                };
                auto number = [&](double* out) {
                    std::string v = value();
                    char* end = nullptr;
                    *out = strtod(v.c_str(), &end);
                    if (v.empty() || *end) {
                        Usage(arg + ": not a number: " + v);
                    }
                };
                auto integer = [&](int* out) {
                    double v = 0;
                    number(&v);
                    if (v != double(int(v))) {
                        Usage(arg + ": not an integer");
                    }
                    *out = int(v);
                };
                if (arg == "--baseline") {
                    o.Baseline = value();
                } else if (arg == "--targets") {
                    o.Targets.clear();
                    targets = true;
                    while (i + 1 < argc && argv[i + 1][0] != '-') {
                        o.Targets.push_back(argv[++i]);
                    }
                } else if (arg == "--per-target") {
                    o.PerTarget = true;
                } else if (arg == "--q") {
                    number(&o.Q);
                } else if (arg == "--max-group-files") {
                    integer(&o.MaxGroupFiles);
                } else if (arg == "--max-missing-traces") {
                    number(&o.MaxMissingTraces);
                } else if (arg == "-j" || arg == "--jobs") {
                    integer(&o.Jobs);
                } else if (arg == "--bisect-ways") {
                    integer(&o.BisectWays);
                } else if (arg == "--main-ref") {
                    o.MainRef = value();
                } else if (arg == "--root") {
                    a.Root = value();
                } else if (arg == "--work-dir") {
                    o.WorkDir = value();
                } else if (arg == "--gn") {
                    a.Tools.Gn = value();
                } else if (arg == "--ninja") {
                    a.Tools.Ninja = value();
                } else if (arg == "-h" || arg == "--help") {
                    fputs(USAGE, stdout);
                    exit(0);
                } else {
                    Usage("unknown option " + arg);
                }
            }
            if (o.Baseline.empty()) {
                Usage("--baseline is required");
            }
            if (targets && o.Targets.empty()) {
                Usage("--targets: a target expected");
            }
            if (o.Jobs < 1 || o.BisectWays < 1 || o.MaxGroupFiles < 0 || o.Q < 0 || o.Q > 1) {
                Usage("-j, --bisect-ways: 1 at least; --max-group-files: 0 at least; --q: of 0..1");
            }
            if (o.PerTarget && o.Q != 0) {
                Usage("--per-target: G of the incremental builds only, no --q");
            }
            o.Ninja = a.Tools.Ninja;
            o.Baseline = AbsPath(o.Baseline);
            o.WorkDir = AbsPath(o.WorkDir);
            a.Root = GnRoot(a.Root.empty() ? AbsPath(".") : a.Root);
            return a;
        }

        bool Update(const TArgs& a) {
            const TOptions& o = a.Options;
            TWorkDir work(o.WorkDir, o.Ninja, o.Jobs);
            StartLog(o.WorkDir);
            TFacts facts;
            facts.Root = a.Root;
            if (!CheckBaseline(o, a.Tools, work, facts) || !ReadTraces(o, work, facts) || !ReadHistory(o, facts) ||
                !FlagClasses(o, facts) || !HeaderFacts(o, work, facts)) {
                return false;
            }
            TPlan plan;
            if (!MakeGroups(o, facts, plan) || !Checks(o, work, facts, plan)) {
                return false;
            }
            // the tree as it was unless all passed, the gn gen of it too
            TPchDirBackup backup(facts);
            if (!(o.PerTarget ? WritePerTarget(o, facts, plan) : Write(o, facts, plan))) {
                return false;
            }
            if (!GnGenCheck(o, a.Tools, work, facts, plan)) {
                return false;
            }
            backup.Keep();
            return true;
        }

    } // namespace

} // namespace NPchUpdate

int main(int argc, char** argv) {
    using namespace NPchUpdate;
    if (argc < 2) {
        Usage("a command expected");
    }
    std::string command = argv[1];
    try {
        if (command == "job") {
            return RunJob(std::vector<std::string>(argv + 2, argv + argc));
        }
        if (command != "update") {
            Usage("unknown command " + command);
        }
        TArgs args = ParseUpdate(argc, argv);
        bool ok = Update(args);
        Report(ok ? "OK" : "FAILED");
        return ok ? 0 : 1;
    } catch (const TError& e) {
        fprintf(stderr, "pch_update: %s\n", e.what());
        if (command != "job") {
            Report("FAILED");
        }
        return 1;
    }
}
