#include "gni.h"
#include "jobs.h"
#include "ninja.h"
#include "steps.h"

#include <rapidjson/document.h>
#include <rapidjson/error/en.h>

#include <sys/stat.h>
#include <unistd.h>

#include <algorithm>
#include <cerrno>
#include <cstdlib>
#include <cstring>
#include <deque>
#include <initializer_list>
#include <map>
#include <set>
#include <unordered_map>
#include <unordered_set>

namespace NPchUpdate {

    namespace {

        constexpr uint64_t GN_RESULT_TAG = 0x70636875'676e7201ull + JOBS_VERSION;
        constexpr size_t THREADS = 8;
        // A trace is written right after its object; older than it by more is of another compile.
        constexpr int64_t TRACE_SLACK_NS = 60'000'000'000;
        // The seed of MurmurHash64A in BuildLog::LogEntry::HashCommand of ninja.
        constexpr uint64_t COMMAND_HASH_SEED = 0xDECAFBADDECAFBADull;
        constexpr size_t TOP_TARGETS = 10;

        // =====================================================================
        // the gn job
        // =====================================================================

        // A target of gn gen: its type and deps as the json of --ide=json has
        // them, the paths ninja_outputs.json lists for it.
        struct TGnTarget {
            std::string Label;
            std::string Type;
            std::vector<std::string> Deps;
            std::vector<std::string> Outputs;
        };

        void SaveGnResult(const std::string& path, const std::vector<TGnTarget>& targets) {
            TBinWriter out;
            out.U64(GN_RESULT_TAG);
            out.U64(targets.size());
            for (const TGnTarget& t : targets) {
                out.Str(t.Label);
                out.Str(t.Type);
                out.StrList(t.Deps);
                out.StrList(t.Outputs);
            }
            WriteFileIfChanged(path, out.Data());
        }

        std::vector<TGnTarget> LoadGnResult(const std::string& path) {
            std::string data = ReadFile(path);
            TBinReader in(data);
            if (in.U64() != GN_RESULT_TAG) {
                Fail(path + ": not a gn result of this version");
            }
            std::vector<TGnTarget> targets(in.U64());
            for (TGnTarget& t : targets) {
                t.Label = in.Str();
                t.Type = in.Str();
                t.Deps = in.StrList();
                t.Outputs = in.StrList();
            }
            return targets;
        }

        void ParseJson(const std::string& path, const std::string& text, rapidjson::Document* doc) {
            doc->Parse(text.data(), text.size());
            if (doc->HasParseError() || !doc->IsObject()) {
                Fail(Format("%s: %s at %zu", path.c_str(), rapidjson::GetParseError_En(doc->GetParseError()), doc->GetErrorOffset()));
            }
        }

        std::string JsonString(const rapidjson::Value& value) {
            return value.IsString() ? std::string(value.GetString(), value.GetStringLength()) : std::string();
        }

        std::vector<std::string> JsonStrings(const rapidjson::Value& object, const char* name) {
            std::vector<std::string> out;
            auto it = object.FindMember(name);
            if (it != object.MemberEnd() && it->value.IsArray()) {
                for (const auto& item : it->value.GetArray()) {
                    if (item.IsString()) {
                        out.push_back(JsonString(item));
                    }
                }
            }
            return out;
        }

        // A dir removed when done with.
        class TTempDir {
        public:
            explicit TTempDir(const std::string& path)
                : Path_(path)
            {
                RemoveTree(Path_);
                MakeDirs(Path_);
            }

            ~TTempDir() {
                try {
                    RemoveTree(Path_);
                } catch (const std::exception&) {
                }
            }

            const std::string& Path() const {
                return Path_;
            }

        private:
            std::string Path_;
        };

        // An executable as Run finds it: by PATH if its name has no slash.
        std::string FindProgram(const std::string& name) {
            if (name.find('/') != std::string::npos) {
                return AbsPath(name);
            }
            const char* path = getenv("PATH");
            for (std::string_view dir : Split(path ? path : "", ':')) {
                std::string candidate = JoinPath(dir.empty() ? "." : dir, name);
                if (access(candidate.c_str(), X_OK) == 0) {
                    return AbsPath(candidate);
                }
            }
            Fail(name + ": not found in PATH");
        }


        // =====================================================================
        // the dirty check of ninja 1.12
        // =====================================================================

        // MurmurHash64A, as BuildLog::LogEntry::HashCommand of ninja.
        uint64_t HashCommand(std::string_view command) {
            const uint64_t m = 0xc6a4a7935bd1e995ull;
            const int r = 47;
            size_t len = command.size();
            uint64_t h = COMMAND_HASH_SEED ^ (len * m);
            const unsigned char* data = reinterpret_cast<const unsigned char*>(command.data());
            while (len >= 8) {
                uint64_t k;
                memcpy(&k, data, sizeof(k));
                k *= m;
                k ^= k >> r;
                k *= m;
                h ^= k;
                h *= m;
                data += 8;
                len -= 8;
            }
            switch (len & 7) {
                case 7:
                    h ^= uint64_t(data[6]) << 48;
                    [[fallthrough]];
                case 6:
                    h ^= uint64_t(data[5]) << 40;
                    [[fallthrough]];
                case 5:
                    h ^= uint64_t(data[4]) << 32;
                    [[fallthrough]];
                case 4:
                    h ^= uint64_t(data[3]) << 24;
                    [[fallthrough]];
                case 3:
                    h ^= uint64_t(data[2]) << 16;
                    [[fallthrough]];
                case 2:
                    h ^= uint64_t(data[1]) << 8;
                    [[fallthrough]];
                case 1:
                    h ^= uint64_t(data[0]);
                    h *= m;
            }
            h ^= h >> r;
            h *= m;
            h ^= h >> r;
            return h;
        }

        // The mtime as RealDiskInterface::Stat of ninja: 0 if missing.
        int64_t NinjaStat(const std::string& path) {
            struct stat st;
            if (stat(path.c_str(), &st) != 0) {
                if (errno == ENOENT || errno == ENOTDIR) {
                    return 0;
                }
                Fail(Format("stat %s: %s", path.c_str(), strerror(errno)));
            }
            if (st.st_mtime == 0) {
                return 1;
            }
            return int64_t(st.st_mtim.tv_sec) * 1'000'000'000 + st.st_mtim.tv_nsec;
        }

        // DependencyScan::RecomputeDirty and Plan::AddTarget of ninja 1.12
        // over the manifest of a build dir, its .ninja_log and .ninja_deps:
        // the commands `ninja <targets>` would run. Dyndep is not supported.
        class TDirtyScan {
        public:
            TDirtyScan(const TNinjaManifest& manifest, const std::string& buildDir, const TNinjaLog& log, TNinjaDepsLog deps)
                : Manifest_(manifest)
                , BuildDir_(buildDir)
                , Log_(log)
                , Deps_(std::move(deps))
            {
                const auto& edges = Manifest_.Edges();
                Edges_.resize(edges.size());
                for (size_t e = 0; e < edges.size(); ++e) {
                    const TNinjaManifest::TEdge& me = edges[e];
                    TEdgeState& s = Edges_[e];
                    s.Phony = me.Rule == "phony";
                    s.HasInputs = !me.Inputs.empty() || !me.ImplicitInputs.empty() || !me.OrderOnly.empty();
                    for (const std::string& p : me.Outputs) {
                        int32_t n = Intern(p);
                        Nodes_[n].InEdge = int32_t(e);
                        s.Out.push_back(n);
                    }
                    for (const std::string& p : me.Inputs) {
                        s.In.push_back(Intern(p));
                    }
                    for (const std::string& p : me.ImplicitInputs) {
                        s.In.push_back(Intern(p));
                    }
                    for (const std::string& p : me.OrderOnly) {
                        s.Order.push_back(Intern(p));
                    }
                    for (const std::string& p : me.Validations) {
                        s.Valid.push_back(Intern(p));
                    }
                }
                DepsNode_.resize(Deps_.Paths.size());
                for (size_t k = 0; k < Deps_.Paths.size(); ++k) {
                    DepsNode_[k] = Intern(Deps_.Paths[k]);
                }
                RecordOfNode_.assign(Nodes_.size(), -1);
                for (size_t k = 0; k < Deps_.Paths.size(); ++k) {
                    if (Deps_.RecordOf[k] >= 0) {
                        RecordOfNode_[DepsNode_[k]] = Deps_.RecordOf[k];
                    }
                }
            }

            bool Has(std::string_view path) const {
                return NodeId_.count(path) != 0;
            }

            // The edges ninja would run for `targets` (dirty, not phony), in
            // no order.
            std::vector<int32_t> Wanted(const std::vector<std::string>& targets) {
                std::vector<int32_t> roots;
                for (const std::string& t : targets) {
                    auto it = NodeId_.find(NormPath(t));
                    if (it == NodeId_.end()) {
                        Fail("ninja: unknown target '" + t + "'");
                    }
                    roots.push_back(it->second);
                }
                Reach(roots);
                for (int32_t r : roots) {
                    Visit(r);
                }
                // RecomputeDirty: the validations of the edges seen are roots too
                std::vector<int32_t> validations = Validations(roots);
                for (int32_t v : validations) {
                    Visit(v);
                }
                std::vector<int32_t> planRoots = roots;
                for (int32_t v : validations) {
                    int32_t e = Nodes_[v].InEdge;
                    if (e >= 0 && !Edges_[e].Ready) {
                        planRoots.push_back(v);
                    }
                }
                return Plan(planRoots);
            }

        private:
            struct TNode {
                std::string_view Path;
                int32_t InEdge = -1;
                // ns; 0: missing.
                int64_t Mtime = 0;
                uint32_t Seen = 0;
                bool Reached = false;
                bool LeafDone = false;
                bool Dirty = false;
                // An input of a depfile or of .ninja_deps with no edge (ninja
                // makes a phony edge of no inputs for it).
                bool FromDeps = false;
            };

            struct TEdgeState {
                std::vector<int32_t> Out;
                // Explicit and implicit inputs; Deps: the ones of the depfile or .ninja_deps.
                std::vector<int32_t> In;
                std::vector<int32_t> Order;
                std::vector<int32_t> Valid;
                std::vector<int32_t> Deps;
                std::string Depfile;
                uint64_t Hash = 0;
                uint32_t Seen = 0;
                bool Phony = false;
                bool HasInputs = false;
                bool Reached = false;
                bool Restat = false;
                bool Generator = false;
                bool DepsMissing = false;
                // 0: not visited, 1: on the stack, 2: done.
                int8_t Mark = 0;
                bool Dirty = false;
                bool Ready = true;
            };

            int32_t Intern(std::string_view path) {
                auto [it, added] = NodeId_.emplace(path, int32_t(Nodes_.size()));
                if (added) {
                    Nodes_.emplace_back().Path = path;
                }
                return it->second;
            }

            // The nodes `stack` lead to, through the deps of the edges too:
            // stat, the edges prepared.
            void Reach(std::vector<int32_t> stack) {
                while (!stack.empty()) {
                    std::vector<int32_t> nodes;
                    std::vector<int32_t> edges;
                    while (!stack.empty()) {
                        int32_t n = stack.back();
                        stack.pop_back();
                        if (Nodes_[n].Reached) {
                            continue;
                        }
                        Nodes_[n].Reached = true;
                        nodes.push_back(n);
                        int32_t e = Nodes_[n].InEdge;
                        if (e < 0 || Edges_[e].Reached) {
                            continue;
                        }
                        TEdgeState& s = Edges_[e];
                        s.Reached = true;
                        edges.push_back(e);
                        stack.insert(stack.end(), s.Out.begin(), s.Out.end());
                        stack.insert(stack.end(), s.In.begin(), s.In.end());
                        stack.insert(stack.end(), s.Order.begin(), s.Order.end());
                        stack.insert(stack.end(), s.Valid.begin(), s.Valid.end());
                    }
                    ParallelFor(nodes.size(), THREADS, [&](size_t i) {
                        TNode& node = Nodes_[nodes[i]];
                        node.Mtime = NinjaStat(JoinPath(BuildDir_, node.Path));
                    });
                    ParallelFor(edges.size(), THREADS, [&](size_t i) {
                        Prepare(edges[i]);
                    });
                    for (int32_t e : edges) {
                        TEdgeState& s = Edges_[e];
                        if (!s.Depfile.empty()) {
                            LoadDepfile(e);
                        }
                        for (int32_t d : s.Deps) {
                            if (Nodes_[d].InEdge < 0) {
                                Nodes_[d].FromDeps = true;
                            }
                            if (!Nodes_[d].Reached) {
                                stack.push_back(d);
                            }
                        }
                    }
                }
            }

            // The bindings of an edge, its deps of .ninja_deps (as
            // ImplicitDepLoader::LoadDepsFromLog); its outputs are stat.
            void Prepare(int32_t e) {
                TEdgeState& s = Edges_[e];
                if (s.Phony) {
                    return;
                }
                s.Restat = !Manifest_.Binding(e, "restat").empty();
                s.Generator = !Manifest_.Binding(e, "generator").empty();
                if (!Manifest_.Binding(e, "dyndep").empty()) {
                    Fail("ninja manifest: dyndep is not supported (" + Manifest_.Edges()[e].Outputs[0] + ")");
                }
                // EvaluateCommand(incl_rsp_file = true)
                std::string command = Manifest_.Command(e);
                std::string rspfile = Manifest_.Binding(e, "rspfile_content");
                if (!rspfile.empty()) {
                    command += ";rspfile=" + rspfile;
                }
                s.Hash = HashCommand(command);
                if (!Manifest_.Binding(e, "deps").empty()) {
                    int32_t out = s.Out[0];
                    int32_t record = RecordOfNode_[out];
                    if (record < 0 || Nodes_[out].Mtime > Deps_.Mtimes[record]) {
                        // missing, or older than the output: rebuilt to regenerate them
                        s.DepsMissing = true;
                        return;
                    }
                    const std::vector<int32_t>& ids = Deps_.Records[record];
                    s.Deps.reserve(ids.size());
                    for (int32_t id : ids) {
                        s.Deps.push_back(DepsNode_[id]);
                    }
                    return;
                }
                s.Depfile = Manifest_.Binding(e, "depfile");
            }

            // ImplicitDepLoader::LoadDepFile: a depfile missing or empty, the
            // edge is dirty.
            void LoadDepfile(int32_t e) {
                TEdgeState& s = Edges_[e];
                std::string text;
                if (!TryReadFile(JoinPath(BuildDir_, s.Depfile), &text) || text.empty()) {
                    s.DepsMissing = true;
                    return;
                }
                for (const std::string& dep : ReadDepfile(text)) {
                    std::string path = NormPath(dep);
                    auto it = NodeId_.find(path);
                    int32_t n = it != NodeId_.end() ? it->second : Intern(Extra_.emplace_back(std::move(path)));
                    if (RecordOfNode_.size() < Nodes_.size()) {
                        RecordOfNode_.resize(Nodes_.size(), -1);
                    }
                    s.Deps.push_back(n);
                }
            }

            // DependencyScan::RecomputeNodeDirty.
            void Visit(int32_t n) {
                TNode& node = Nodes_[n];
                int32_t e = node.InEdge;
                if (e < 0) {
                    if (!node.LeafDone) {
                        node.LeafDone = true;
                        node.Dirty = node.Mtime == 0;
                    }
                    return;
                }
                TEdgeState& s = Edges_[e];
                if (s.Mark == 2) {
                    return;
                }
                if (s.Mark == 1) {
                    Fail("ninja: dependency cycle at " + std::string(node.Path));
                }
                s.Mark = 1;
                bool dirty = s.DepsMissing;
                bool ready = true;
                bool hasRecent = false;
                int64_t recent = 0;
                auto input = [&](int32_t i, bool orderOnly) {
                    Visit(i);
                    int32_t ie = Nodes_[i].InEdge;
                    if (ie >= 0 && !Edges_[ie].Ready) {
                        ready = false;
                    }
                    if (orderOnly) {
                        return;
                    }
                    if (Nodes_[i].Dirty) {
                        dirty = true;
                    } else if (!hasRecent || Nodes_[i].Mtime > recent) {
                        hasRecent = true;
                        recent = Nodes_[i].Mtime;
                    }
                };
                for (int32_t i : s.In) {
                    input(i, false);
                }
                for (int32_t i : s.Deps) {
                    input(i, false);
                }
                for (int32_t i : s.Order) {
                    input(i, true);
                }
                if (!dirty) {
                    dirty = OutputsDirty(s, hasRecent, recent);
                }
                if (dirty) {
                    for (int32_t o : s.Out) {
                        Nodes_[o].Dirty = true;
                    }
                }
                bool noInputs = !s.HasInputs && s.Deps.empty();
                if (dirty && !(s.Phony && noInputs)) {
                    ready = false;
                }
                s.Dirty = dirty;
                s.Ready = ready;
                s.Mark = 2;
            }

            // DependencyScan::RecomputeOutputsDirty.
            bool OutputsDirty(const TEdgeState& s, bool hasRecent, int64_t recent) {
                if (s.Phony) {
                    bool noInputs = !s.HasInputs && s.Deps.empty();
                    for (int32_t o : s.Out) {
                        // phony outputs are dirty only with no inputs and missing
                        if (noInputs && Nodes_[o].Mtime == 0) {
                            return true;
                        }
                        // the newest input's mtime, for the edges taking the phony output
                        if (hasRecent) {
                            Nodes_[o].Mtime = std::max(Nodes_[o].Mtime, recent);
                        }
                    }
                    return false;
                }
                for (int32_t o : s.Out) {
                    const TNode& out = Nodes_[o];
                    if (out.Mtime == 0) {
                        return true;
                    }
                    auto it = Log_.Entries.find(std::string(out.Path));
                    const TNinjaLogEntry* entry = it == Log_.Entries.end() ? nullptr : &it->second;
                    // restat: the mtime of the log, not of the output
                    bool usedRestat = s.Restat && entry;
                    if (!usedRestat && hasRecent && out.Mtime < recent) {
                        return true;
                    }
                    if (entry) {
                        if (!s.Generator && s.Hash != entry->CommandHash) {
                            return true;
                        }
                        if (hasRecent && entry->MtimeNs < recent) {
                            return true;
                        }
                    } else if (!s.Generator) {
                        return true;
                    }
                }
                return false;
            }

            // The validations of the edges `roots` lead to, theirs too.
            std::vector<int32_t> Validations(const std::vector<int32_t>& roots) {
                ++Generation_;
                std::vector<int32_t> out;
                std::vector<int32_t> stack = roots;
                while (!stack.empty()) {
                    int32_t n = stack.back();
                    stack.pop_back();
                    if (Nodes_[n].Seen == Generation_) {
                        continue;
                    }
                    Nodes_[n].Seen = Generation_;
                    int32_t e = Nodes_[n].InEdge;
                    if (e < 0 || Edges_[e].Seen == Generation_) {
                        continue;
                    }
                    TEdgeState& s = Edges_[e];
                    s.Seen = Generation_;
                    for (int32_t v : s.Valid) {
                        out.push_back(v);
                        stack.push_back(v);
                    }
                    stack.insert(stack.end(), s.In.begin(), s.In.end());
                    stack.insert(stack.end(), s.Deps.begin(), s.Deps.end());
                    stack.insert(stack.end(), s.Order.begin(), s.Order.end());
                }
                std::sort(out.begin(), out.end());
                out.erase(std::unique(out.begin(), out.end()), out.end());
                return out;
            }

            // Plan::AddSubTarget: the edges not ready the roots lead to
            // through edges not ready; the dirty ones are run.
            std::vector<int32_t> Plan(const std::vector<int32_t>& roots) {
                ++Generation_;
                std::vector<int32_t> wanted;
                std::vector<std::pair<int32_t, int32_t>> stack;
                for (int32_t r : roots) {
                    stack.emplace_back(r, -1);
                }
                while (!stack.empty()) {
                    auto [n, dependent] = stack.back();
                    stack.pop_back();
                    const TNode& node = Nodes_[n];
                    int32_t e = node.InEdge;
                    if (e < 0) {
                        if (node.Dirty && !node.FromDeps) {
                            if (dependent < 0) {
                                Fail("ninja: '" + std::string(node.Path) + "' missing and no known rule to make it");
                            }
                            Fail("ninja: '" + std::string(node.Path) + "', needed by '" + std::string(Nodes_[dependent].Path) +
                                 "', missing and no known rule to make it");
                        }
                        continue;
                    }
                    TEdgeState& s = Edges_[e];
                    if (s.Ready || s.Seen == Generation_) {
                        continue;
                    }
                    s.Seen = Generation_;
                    if (s.Dirty && !s.Phony) {
                        wanted.push_back(e);
                    }
                    for (const auto* list : {&s.In, &s.Deps, &s.Order}) {
                        for (int32_t i : *list) {
                            stack.emplace_back(i, n);
                        }
                    }
                }
                return wanted;
            }

            const TNinjaManifest& Manifest_;
            std::string BuildDir_;
            const TNinjaLog& Log_;
            TNinjaDepsLog Deps_;
            std::deque<std::string> Extra_;
            std::unordered_map<std::string_view, int32_t> NodeId_;
            std::deque<TNode> Nodes_;
            std::vector<TEdgeState> Edges_;
            std::vector<int32_t> DepsNode_;
            std::vector<int32_t> RecordOfNode_;
            uint32_t Generation_ = 0;
        };

        // =====================================================================
        // check
        // =====================================================================

        // The command gn regenerates build.ninja with: its source root, its
        // switches but --root and --regeneration.
        struct TGnCommand {
            std::string Root;
            std::vector<std::string> Switches;
        };

        TGnCommand GnCommand(const TNinjaManifest& manifest) {
            const auto& edges = manifest.Edges();
            std::vector<std::string> stack = {"build.ninja"};
            std::set<std::string> seen;
            std::set<int64_t> generators;
            while (!stack.empty()) {
                std::string path = std::move(stack.back());
                stack.pop_back();
                if (!seen.insert(path).second) {
                    continue;
                }
                int64_t e = manifest.Producer(path);
                if (e < 0) {
                    continue;
                }
                const TNinjaManifest::TEdge& edge = edges[e];
                if (edge.Rule == "phony") {
                    stack.insert(stack.end(), edge.Inputs.begin(), edge.Inputs.end());
                    stack.insert(stack.end(), edge.ImplicitInputs.begin(), edge.ImplicitInputs.end());
                } else if (!manifest.Binding(e, "generator").empty()) {
                    generators.insert(e);
                }
            }
            if (generators.size() != 1) {
                Fail(Format("build.ninja: %zu generator edges make it, one of gn expected", generators.size()));
            }
            std::string command = manifest.Command(size_t(*generators.begin()));
            std::vector<std::string> argv = ShellSplit(command);
            TGnCommand out;
            bool gen = false;
            for (size_t i = 1; i < argv.size(); ++i) {
                if (argv[i] == "gen") {
                    gen = true;
                    break;
                }
                if (StartsWith(argv[i], "--root=")) {
                    out.Root = argv[i].substr(7);
                } else if (argv[i] != "--regeneration") {
                    out.Switches.push_back(argv[i]);
                }
            }
            if (!gen || out.Root.empty()) {
                Fail("build.ninja: not made by gn gen --root=...: " + command);
            }
            return out;
        }

        // Pids of the ninja processes building in `buildDir`.
        std::vector<int> RunningNinja(const std::string& buildDir) {
            std::vector<int> out;
            for (const std::string& pid : ListDir("/proc")) {
                if (pid.empty() || !std::all_of(pid.begin(), pid.end(), [](char c) {
                        return c >= '0' && c <= '9';
                    }))
                {
                    continue;
                }
                std::string cmdline;
                if (!TryReadFile("/proc/" + pid + "/cmdline", &cmdline) || cmdline.empty()) {
                    continue;
                }
                std::vector<std::string> argv;
                for (std::string_view a : Split(cmdline, '\0')) {
                    argv.emplace_back(a);
                }
                if (BaseName(argv[0]) != "ninja") {
                    continue;
                }
                char buffer[4096];
                ssize_t size = readlink(("/proc/" + pid + "/cwd").c_str(), buffer, sizeof(buffer) - 1);
                if (size < 0) {
                    continue;
                }
                std::string dir(buffer, size);
                for (size_t k = 0; k < argv.size(); ++k) {
                    if (argv[k] == "-C" && k + 1 < argv.size()) {
                        dir = JoinPath(dir, argv[k + 1]);
                    } else if (StartsWith(argv[k], "-C") && argv[k].size() > 2) {
                        dir = JoinPath(dir, argv[k].substr(2));
                    }
                }
                if (RealPath(dir) == buildDir) {
                    out.push_back(atoi(pid.c_str()));
                }
            }
            return out;
        }

        // A value of args.gn as written there.
        std::string GnValueText(const TGnValue* value, const std::string& absent) {
            if (!value) {
                return absent;
            }
            switch (value->Kind) {
                case TGnValue::String:
                    return GnQuote(value->Text);
                case TGnValue::Bool:
                    return value->BoolValue ? "true" : "false";
                case TGnValue::Integer:
                    return std::to_string(value->IntValue);
                default:
                    return value->Text;
            }
        }

        // The path with the extension of its file name replaced, as
        // llvm::sys::path::replace_extension.
        std::string ReplaceExtension(const std::string& path, std::string_view extension) {
            size_t slash = path.rfind('/');
            size_t start = slash == std::string::npos ? 0 : slash + 1;
            size_t dot = path.rfind('.');
            std::string stem = path;
            if (dot != std::string::npos && dot >= start && path.substr(start) != "." && path.substr(start) != "..") {
                stem = path.substr(0, dot);
            }
            return stem + std::string(extension);
        }

        // Of a C++ compile: whether it writes a trace, where, whether it
        // takes a PCH.
        struct TCompileInfo {
            bool TimeTrace = false;
            bool IncludePch = false;
            // Relative to the build dir or absolute; empty if unknown.
            std::string Trace;
            int64_t ObjMtime = 0;
            int64_t TraceMtime = 0;
            int64_t TraceSize = 0;
        };

        // The trace path as clang's driver infers it (Driver.cpp,
        // handleTimeTrace): -ftime-trace=<file or dir>, or the output of -o
        // (or -dumpdir with the input's name) with the extension .json.
        TCompileInfo CompileInfo(const std::vector<std::string>& argv, size_t compiler) {
            TCompileInfo info;
            std::string value;
            bool bare = false;
            std::string output;
            std::string dumpDir;
            std::string input;
            for (size_t i = compiler + 1; i < argv.size(); ++i) {
                const std::string& a = argv[i];
                if (a == "-ftime-trace") {
                    info.TimeTrace = bare = true;
                    value.clear();
                } else if (StartsWith(a, "-ftime-trace=")) {
                    info.TimeTrace = true;
                    bare = false;
                    value = a.substr(13);
                } else if (a == "-include-pch") {
                    info.IncludePch = true;
                } else if (a == "-o" && i + 1 < argv.size()) {
                    output = argv[++i];
                } else if (a == "-dumpdir" && i + 1 < argv.size()) {
                    dumpDir = argv[++i];
                } else if (a == "-c" && i + 1 < argv.size() && !StartsWith(argv[i + 1], "-")) {
                    input = argv[i + 1];
                }
            }
            if (!info.TimeTrace) {
                return info;
            }
            if (!bare) {
                if (EndsWith(value, "/") || IsDir(value)) {
                    if (!output.empty()) {
                        info.Trace = JoinPath(value, BaseName(ReplaceExtension(output, ".json")));
                    }
                } else {
                    info.Trace = value;
                }
            } else if (!dumpDir.empty()) {
                if (!input.empty()) {
                    info.Trace = ReplaceExtension(dumpDir + BaseName(input), ".json");
                }
            } else if (!output.empty()) {
                info.Trace = ReplaceExtension(output, ".json");
            }
            return info;
        }

        bool StatFile(const std::string& path, int64_t* mtime, int64_t* size) {
            struct stat st;
            if (stat(path.c_str(), &st) != 0) {
                return false;
            }
            *mtime = int64_t(st.st_mtim.tv_sec) * 1'000'000'000 + st.st_mtim.tv_nsec;
            *size = int64_t(st.st_size);
            return true;
        }

        std::string ByTarget(const std::map<std::string, std::vector<std::string>>& objs) {
            std::vector<std::pair<std::string, const std::vector<std::string>*>> top;
            for (const auto& [label, list] : objs) {
                top.emplace_back(label, &list);
            }
            std::sort(top.begin(), top.end(), [](const auto& a, const auto& b) {
                if (a.second->size() != b.second->size()) {
                    return a.second->size() > b.second->size();
                }
                return a.first < b.first;
            });
            std::string out;
            for (size_t i = 0; i < top.size() && i < TOP_TARGETS; ++i) {
                if (i) {
                    out += "\n";
                }
                out += Format("      %-70s %zu, e.g. %s", top[i].first.c_str(), top[i].second->size(), (*top[i].second)[0].c_str());
            }
            return out;
        }

        size_t Total(const std::map<std::string, std::vector<std::string>>& objs) {
            size_t n = 0;
            for (const auto& [label, list] : objs) {
                n += list.size();
            }
            return n;
        }

        bool IsQualified(std::string_view label) {
            return label.find('(') != std::string_view::npos;
        }

        // LabelOfOutput, OutputsOfLabel, TypeOf, DepsOf of the targets of the
        // default toolchain. ninja_outputs.json lists for a target the inputs
        // of its .inputdeps phony too: of the targets listing a path written
        // by an edge, the one not having it as an input of an edge it writes.
        void TakeLabels(const std::vector<TGnTarget>& targets, TFacts& facts) {
            const TNinjaManifest& manifest = *facts.Manifest;
            const auto& edges = manifest.Edges();
            std::unordered_map<std::string_view, std::vector<int32_t>> listedBy;
            for (size_t i = 0; i < targets.size(); ++i) {
                for (const std::string& p : targets[i].Outputs) {
                    if (manifest.Producer(p) < 0) {
                        continue;
                    }
                    std::vector<int32_t>& labels = listedBy[p];
                    if (labels.empty() || labels.back() != int32_t(i)) {
                        labels.push_back(int32_t(i));
                    }
                }
            }
            std::unordered_set<std::string_view> multi;
            for (const auto& [p, labels] : listedBy) {
                if (labels.size() > 1) {
                    multi.insert(p);
                }
            }
            std::set<std::pair<std::string_view, int32_t>> inputOf;
            if (!multi.empty()) {
                for (size_t i = 0; i < targets.size(); ++i) {
                    std::unordered_set<int64_t> seen;
                    for (const std::string& q : targets[i].Outputs) {
                        int64_t e = manifest.Producer(q);
                        if (e < 0 || !seen.insert(e).second) {
                            continue;
                        }
                        const TNinjaManifest::TEdge& edge = edges[e];
                        for (const auto* list : {&edge.Inputs, &edge.ImplicitInputs, &edge.OrderOnly}) {
                            for (const std::string& p : *list) {
                                auto it = multi.find(p);
                                if (it != multi.end()) {
                                    inputOf.emplace(*it, int32_t(i));
                                }
                            }
                        }
                    }
                }
            }
            facts.LabelOfOutput.clear();
            facts.OutputsOfLabel.clear();
            for (const auto& [p, labels] : listedBy) {
                int32_t owner = labels[0];
                if (labels.size() > 1) {
                    owner = -1;
                    for (int32_t i : labels) {
                        if (inputOf.count({p, i})) {
                            continue;
                        }
                        if (owner >= 0) {
                            Fail(Format("ninja_outputs.json: %s is of %s and %s", std::string(p).c_str(), targets[owner].Label.c_str(),
                                        targets[i].Label.c_str()));
                        }
                        owner = i;
                    }
                    if (owner < 0) {
                        Fail(Format("ninja_outputs.json: %s is an input of all its %zu targets", std::string(p).c_str(), labels.size()));
                    }
                }
                const std::string& label = targets[owner].Label;
                if (!IsQualified(label)) {
                    facts.LabelOfOutput.emplace(std::string(p), label);
                    facts.OutputsOfLabel[label].emplace_back(p);
                }
            }
            for (auto& [label, outputs] : facts.OutputsOfLabel) {
                std::sort(outputs.begin(), outputs.end());
            }
            facts.TypeOf.clear();
            facts.DepsOf.clear();
            for (const TGnTarget& t : targets) {
                if (IsQualified(t.Label) || t.Type.empty()) {
                    continue;
                }
                facts.TypeOf[t.Label] = t.Type;
                std::vector<std::string>& deps = facts.DepsOf[t.Label];
                for (const std::string& d : t.Deps) {
                    if (!IsQualified(d)) {
                        deps.push_back(d);
                    }
                }
            }
        }

    } // namespace

    // =========================================================================
    // paths
    // =========================================================================

    TPaths::TPaths(const std::string& buildDir, const std::string& sourceRoot, const std::string& sysroot)
        : BuildDir(RealPath(buildDir))
        , SourceRoot(sourceRoot.empty() ? std::string() : RealPath(sourceRoot))
        , Sysroot(sysroot.empty() ? std::string() : RealPath(sysroot))
    {
        if (!sourceRoot.empty()) {
            SourceRoots.push_back(SourceRoot);
            std::string named = NormPath(JoinPath(BuildDir, sourceRoot));
            if (named != SourceRoot) {
                SourceRoots.push_back(named);
            }
        }
    }

    std::string TPaths::Normalize(std::string_view path) const {
        std::string p = NormPath(IsAbs(path) ? std::string(path) : JoinPath(BuildDir, path));
        auto under = [&](const std::string& dir) {
            return !dir.empty() && p.size() > dir.size() && p[dir.size()] == '/' && StartsWith(p, dir);
        };
        for (const std::string& root : SourceRoots) {
            if (under(root)) {
                return p.substr(root.size() + 1);
            }
        }
        if (under(BuildDir)) {
            return "<out>/" + p.substr(BuildDir.size() + 1);
        }
        if (under(Sysroot)) {
            return "<sysroot>/" + p.substr(Sysroot.size() + 1);
        }
        return p;
    }

    std::string TPaths::Real(std::string_view name) const {
        if (StartsWith(name, "<out>/")) {
            return JoinPath(BuildDir, name.substr(6));
        }
        if (StartsWith(name, "<sysroot>/")) {
            return JoinPath(Sysroot, name.substr(10));
        }
        return IsAbs(name) ? std::string(name) : JoinPath(SourceRoot, name);
    }

    // =========================================================================
    // the gn job
    // =========================================================================

    // gn <gn> <source root> <args.gn> <switch...>: gn gen with a copy of
    // args.gn in a dir of its own, with --ninja-outputs-file and --ide=json;
    // of each target of either (labels as gn writes them: the default
    // toolchain's without one) its type and deps, the paths of
    // ninja_outputs.json (ninja's form: relative to the out dir). Its
    // depfile: the files gn read (build.ninja.d), args.gn, gn.
    void GnJob(const std::string& result, const std::vector<std::string>& args) {
        if (args.size() < 3) {
            Fail("gn: bad arguments");
        }
        const std::string& gn = args[0];
        const std::string& root = args[1];
        const std::string& argsGn = args[2];
        TTempDir tmp(result + ".tmp");
        std::string out = JoinPath(tmp.Path(), "out");
        MakeDirs(out);
        WriteFile(JoinPath(out, "args.gn"), ReadFile(argsGn));

        std::vector<std::string> argv = {gn, "--root=" + root};
        argv.insert(argv.end(), args.begin() + 3, args.end());
        argv.insert(argv.end(), {"gen", out, "--ninja-outputs-file=ninja_outputs.json", "--ide=json"});
        TRunOptions options;
        options.Cwd = tmp.Path();
        options.MergeStderr = true;
        TRunResult run = Run(argv, options);
        if (run.ExitCode != 0) {
            std::string tail = run.Stdout.size() <= 4000 ? run.Stdout : run.Stdout.substr(run.Stdout.size() - 4000);
            Fail(Format("gn gen failed with %d:\n%s", run.ExitCode, tail.c_str()));
        }

        std::map<std::string, TGnTarget> targets;
        {
            std::string path = JoinPath(out, "project.json");
            std::string text = ReadFile(path);
            rapidjson::Document doc;
            ParseJson(path, text, &doc);
            auto it = doc.FindMember("targets");
            if (it == doc.MemberEnd() || !it->value.IsObject()) {
                Fail(path + ": no targets");
            }
            for (auto t = it->value.MemberBegin(); t != it->value.MemberEnd(); ++t) {
                std::string label = JsonString(t->name);
                TGnTarget& target = targets[label];
                target.Label = label;
                if (t->value.IsObject()) {
                    auto type = t->value.FindMember("type");
                    if (type != t->value.MemberEnd()) {
                        target.Type = JsonString(type->value);
                    }
                    target.Deps = JsonStrings(t->value, "deps");
                }
            }
        }
        {
            std::string path = JoinPath(out, "ninja_outputs.json");
            std::string text = ReadFile(path);
            rapidjson::Document doc;
            ParseJson(path, text, &doc);
            for (auto t = doc.MemberBegin(); t != doc.MemberEnd(); ++t) {
                if (!t->value.IsArray()) {
                    continue;
                }
                std::string label = JsonString(t->name);
                TGnTarget& target = targets[label];
                target.Label = label;
                for (const auto& p : t->value.GetArray()) {
                    if (p.IsString()) {
                        target.Outputs.push_back(NormPath(JsonString(p)));
                    }
                }
            }
        }
        std::vector<TGnTarget> list;
        list.reserve(targets.size());
        for (auto& [label, target] : targets) {
            list.push_back(std::move(target));
        }
        SaveGnResult(result, list);

        std::set<std::string> deps;
        for (const std::string& dep : ReadDepfile(ReadFile(JoinPath(out, "build.ninja.d")))) {
            deps.insert(NormPath(IsAbs(dep) ? dep : JoinPath(out, dep)));
        }
        deps.insert(NormPath(argsGn));
        deps.insert(RealPath(gn));
        WriteFile(result + ".d", WriteDepfile(result, {deps.begin(), deps.end()}));
    }

    // =========================================================================
    // check
    // =========================================================================

    bool CheckBaseline(const TOptions& options, const TTools& tools, TWorkDir& work, TFacts& facts) {
        std::string buildDir = RealPath(options.Baseline);
        std::vector<std::string> errors;
        std::vector<std::string> warnings;
        if (!Exists(JoinPath(buildDir, "build.ninja"))) {
            Fail(buildDir + " is not a gn build dir (no build.ninja)");
        }
        facts.BuildDir = buildDir;

        Log("reading the ninja manifest...");
        facts.Manifest = std::make_unique<TNinjaManifest>(buildDir);
        const TNinjaManifest& manifest = *facts.Manifest;
        const auto& edges = manifest.Edges();

        TGnCommand gnCommand = GnCommand(manifest);
        std::string sourceRoot = NormPath(JoinPath(buildDir, gnCommand.Root));
        std::string realRoot = RealPath(sourceRoot);
        if (facts.Root.empty()) {
            facts.Root = realRoot;
        } else if (realRoot != RealPath(facts.Root)) {
            errors.push_back(Format("a build of %s, not of %s", realRoot.c_str(), facts.Root.c_str()));
        }

        std::string argsGn = JoinPath(buildDir, "args.gn");
        std::vector<std::pair<std::string, TGnValue>> gnArgs = ParseGnFile(ReadFile(argsGn));
        const TGnValue* pchMode = FindGnValue(gnArgs, "pch_mode");
        const TGnValue* timeTrace = FindGnValue(gnArgs, "time_trace");
        std::string pchModeText = GnValueText(pchMode, "\"none\"");
        if (pchMode && !(pchMode->Kind == TGnValue::String && pchMode->Text == "none")) {
            errors.push_back("args.gn: pch_mode = " + pchModeText + ", a build without PCH is needed (\"none\")");
        }
        if (!timeTrace || timeTrace->Kind != TGnValue::Bool || !timeTrace->BoolValue) {
            errors.push_back("args.gn: time_trace = " + GnValueText(timeTrace, "false (default)") + ", true is needed");
        }

        std::vector<int> busy = RunningNinja(buildDir);
        if (!busy.empty()) {
            std::vector<std::string> pids;
            for (int pid : busy) {
                pids.push_back(std::to_string(pid));
            }
            errors.push_back("ninja is running there (pid " + Join(pids, ", ") + ")");
        }

        // the dirty check: what ninja would run
        TNinjaLog ninjaLog;
        std::string logPath = JoinPath(buildDir, ".ninja_log");
        bool hasLog = Exists(logPath);
        if (hasLog) {
            ninjaLog = ReadNinjaLog(logPath);
        } else {
            errors.push_back("no .ninja_log: no command of the build is known to have run");
        }
        std::string depsPath = JoinPath(buildDir, ".ninja_deps");
        bool hasDeps = Exists(depsPath);
        if (!hasDeps) {
            warnings.push_back("no .ninja_deps: the header dependencies of the objects are unknown");
        }
        std::string targetsText = Join(options.Targets, " ");
        bool stale = false;
        std::vector<int32_t> commands;
        {
            Log("the dirty check of " + targetsText + "...");
            TDirtyScan scan(manifest, buildDir, ninjaLog, hasDeps ? ReadNinjaDepsLog(depsPath) : TNinjaDepsLog());
            if (scan.Has("build.ninja")) {
                stale = !scan.Wanted({"build.ninja"}).empty();
            }
            if (stale) {
                errors.push_back("the ninja files are stale (gn files changed since the build): "
                                 "ninja would regenerate them and rebuild what changed");
            } else {
                commands = scan.Wanted(options.Targets);
            }
        }
        std::set<std::string> toCompile;
        for (int32_t e : commands) {
            if (edges[e].Rule == "cxx") {
                toCompile.insert(edges[e].Outputs[0]);
            }
        }

        // the labels: gn gen of the args of the baseline
        TWorkDir::TJob job;
        job.Kind = "gn";
        job.Args = {FindProgram(tools.Gn), realRoot, argsGn};
        job.Args.insert(job.Args.end(), gnCommand.Switches.begin(), gnCommand.Switches.end());
        job.Inputs = {argsGn};
        job.Restat = true;
        std::string gnResult = work.Add(job);
        Log("gn gen of the args of the baseline...");
        work.Build({gnResult}, "gn");
        TakeLabels(LoadGnResult(gnResult), facts);
        auto labelOf = [&](size_t e) -> const std::string* {
            const auto& outs = edges[e].Outputs;
            auto it = outs.empty() ? facts.LabelOfOutput.end() : facts.LabelOfOutput.find(outs[0]);
            return it == facts.LabelOfOutput.end() ? nullptr : &it->second;
        };

        std::vector<size_t> cxx;
        size_t pchEdges = 0;
        std::map<std::string, size_t> otherToolchains;
        std::string pchPrefix = "//" + std::string(PCH_DIR);
        for (size_t e = 0; e < edges.size(); ++e) {
            const TNinjaManifest::TEdge& edge = edges[e];
            if (edge.Rule == "cxx") {
                cxx.push_back(e);
            } else if (EndsWith(edge.Rule, "_cxx")) {
                ++otherToolchains[edge.Rule];
            }
            const std::string* label = labelOf(e);
            bool pch = label && (StartsWith(*label, pchPrefix + "/") || StartsWith(*label, pchPrefix + ":"));
            for (const std::string& o : edge.Outputs) {
                pch = pch || EndsWith(o, ".pch") || EndsWith(o, ".gch");
            }
            pchEdges += pch;
        }

        // the compiler and the sysroot of the C++ compiles
        facts.Compiler.clear();
        std::string sysroot;
        if (!cxx.empty()) {
            std::vector<std::string> argv = ShellSplit(manifest.Command(cxx[0]));
            for (size_t i = 0; i < argv.size(); ++i) {
                if (facts.Compiler.empty() && StartsWith(BaseName(argv[i]), "clang++")) {
                    facts.Compiler = argv[i];
                } else if (!facts.Compiler.empty() && StartsWith(argv[i], "--sysroot=")) {
                    sysroot = argv[i].substr(10);
                } else if (!facts.Compiler.empty() && argv[i] == "--sysroot" && i + 1 < argv.size()) {
                    sysroot = argv[i + 1];
                }
            }
        }
        facts.CompilerDir = facts.Compiler.empty() ? std::string() : DirName(DirName(RealPath(facts.Compiler)));
        facts.Paths = TPaths(buildDir, sourceRoot, sysroot.empty() ? std::string() : JoinPath(buildDir, sysroot));

        Log(Format("looking at %zu C++ objects and their traces...", cxx.size()));
        std::vector<TCompileInfo> infos(cxx.size());
        std::vector<char> objBuilt(cxx.size(), 0);
        std::vector<char> traceFound(cxx.size(), 0);
        ParallelFor(cxx.size(), THREADS, [&](size_t i) {
            std::vector<std::string> argv = ShellSplit(manifest.Command(cxx[i]));
            size_t compiler = 0;
            while (compiler < argv.size() && argv[compiler] != facts.Compiler) {
                ++compiler;
            }
            TCompileInfo& info = infos[i];
            if (compiler < argv.size()) {
                info = CompileInfo(argv, compiler);
            }
            int64_t size = 0;
            objBuilt[i] = StatFile(JoinPath(buildDir, edges[cxx[i]].Outputs[0]), &info.ObjMtime, &size);
            if (!info.Trace.empty()) {
                info.Trace = NormPath(JoinPath(buildDir, info.Trace));
                traceFound[i] = StatFile(info.Trace, &info.TraceMtime, &info.TraceSize);
            }
        });

        facts.DurationMs.clear();
        for (const auto& [output, entry] : ninjaLog.Entries) {
            facts.DurationMs[output] = entry.EndMs - entry.StartMs;
        }

        std::vector<size_t> noFlag;
        std::vector<std::string> noLog;
        std::vector<std::string> outOfDate;
        std::vector<std::string> staleTraces;
        std::vector<std::string> unlabeled;
        std::map<std::string, std::vector<std::string>> notBuilt;
        std::map<std::string, std::vector<std::string>> noTrace;
        std::set<std::string> labels;
        size_t built = 0;
        size_t withPch = 0;
        int64_t traceBytes = 0;
        facts.Traced.clear();
        for (size_t i = 0; i < cxx.size(); ++i) {
            const TNinjaManifest::TEdge& edge = edges[cxx[i]];
            const std::string& obj = edge.Outputs[0];
            const TCompileInfo& info = infos[i];
            const std::string* found = labelOf(cxx[i]);
            if (!found) {
                unlabeled.push_back(obj);
            }
            std::string label = found ? *found : std::string("?");
            labels.insert(label);
            if (!info.TimeTrace) {
                noFlag.push_back(i);
            }
            withPch += info.IncludePch;
            if (!objBuilt[i]) {
                notBuilt[label].push_back(obj);
                continue;
            }
            ++built;
            if (toCompile.count(obj)) {
                outOfDate.push_back(obj);
            }
            auto entry = ninjaLog.Entries.find(obj);
            if (entry == ninjaLog.Entries.end()) {
                noLog.push_back(obj);
            }
            if (!traceFound[i]) {
                noTrace[label].push_back(obj);
                continue;
            }
            traceBytes += info.TraceSize;
            if (info.TraceSize == 0 || info.TraceMtime < info.ObjMtime - TRACE_SLACK_NS) {
                staleTraces.push_back(obj);
            } else if (entry != ninjaLog.Entries.end()) {
                facts.Traced.emplace_back(obj, info.Trace);
            }
        }
        std::sort(facts.Traced.begin(), facts.Traced.end());

        if (pchEdges || withPch) {
            errors.push_back(Format("the manifest has PCH: %zu commands building one, %zu C++ files using one", pchEdges, withPch));
        }
        if (!otherToolchains.empty()) {
            std::vector<std::string> parts;
            for (const auto& [rule, n] : otherToolchains) {
                parts.push_back(Format("%s %zu", rule.c_str(), n));
            }
            warnings.push_back("C++ files of other toolchains, not looked at: " + Join(parts, ", "));
        }
        if (!unlabeled.empty()) {
            errors.push_back(Format("%zu C++ files of no target of gn (ninja_outputs.json), e.g. %s", unlabeled.size(),
                                    unlabeled[0].c_str()));
        }
        if (!outOfDate.empty()) {
            errors.push_back(Format("%zu C++ objects out of date (ninja would compile them again), e.g. %s", outOfDate.size(),
                                    outOfDate[0].c_str()));
        }
        if (!notBuilt.empty()) {
            warnings.push_back(Format("%zu C++ files not built (failed, not reached or not in %s), left out, in %zu targets:\n%s",
                                      Total(notBuilt), targetsText.c_str(), notBuilt.size(), ByTarget(notBuilt).c_str()));
        }
        size_t others = commands.size() - std::count_if(commands.begin(), commands.end(), [&](int32_t e) {
            return edges[e].Rule == "cxx";
        });
        if (others && !stale) {
            warnings.push_back(Format("ninja would also run %zu other commands (actions, links...) for %s", others, targetsText.c_str()));
        }
        if (!noFlag.empty()) {
            const std::string* label = labelOf(cxx[noFlag[0]]);
            errors.push_back(Format("%zu C++ files compiled without -ftime-trace, e.g. %s (target %s)", noFlag.size(),
                                    edges[cxx[noFlag[0]]].Outputs[0].c_str(), label ? label->c_str() : "?"));
        }
        if (!noLog.empty()) {
            errors.push_back(Format("%zu C++ objects without a .ninja_log entry (no duration), e.g. %s", noLog.size(), noLog[0].c_str()));
        }
        if (!staleTraces.empty()) {
            errors.push_back(Format("%zu traces empty or older than their objects, e.g. %s", staleTraces.size(), staleTraces[0].c_str()));
        }
        size_t missing = Total(noTrace);
        if (missing) {
            std::string text = Format("%zu built C++ objects without a trace (%.2f%%) in %zu targets:\n%s", missing,
                                      100.0 * double(missing) / double(std::max<size_t>(built, 1)), noTrace.size(), ByTarget(noTrace).c_str());
            (double(missing) > options.MaxMissingTraces * double(built) ? errors : warnings).push_back(text);
        }

        std::string head = "?";
        {
            TRunOptions git;
            git.CaptureStderr = false;
            TRunResult run = Run({"git", "-C", facts.Root, "rev-parse", "--short", "HEAD"}, git);
            if (run.ExitCode == 0) {
                head = run.Stdout;
                while (!head.empty() && (head.back() == '\n' || head.back() == '\r')) {
                    head.pop_back();
                }
            }
        }
        size_t cxxCount = cxx.size();
        size_t notBuiltTargets = notBuilt.size();
        Report(Format("baseline %s, source tree %s at %s", buildDir.c_str(), facts.Root.c_str(), head.c_str()));
        Report(Format("  args.gn:          pch_mode = %s, time_trace = %s", pchModeText.c_str(), GnValueText(timeTrace, "false").c_str()));
        Report("  dirty check:      " +
               (stale ? std::string("the ninja files are stale")
                      : Format("%zu commands to run for %s, %zu of them C++", commands.size(), targetsText.c_str(), toCompile.size())));
        Report(Format("  manifest:         %zu targets, %zu of them with C++: %zu C++ files, with PCH %zu", facts.OutputsOfLabel.size(),
                      labels.size(), cxxCount, withPch));
        Report(Format("  built:            %zu of %zu C++ files; all files of %zu of %zu targets", built, cxxCount,
                      labels.size() - notBuiltTargets, labels.size()));
        Report(Format("  -ftime-trace:     %zu of %zu C++ files", cxxCount - noFlag.size(), cxxCount));
        Report(Format("  traces:           %zu of %zu built (%.1f GB), empty or older than the object %zu", built - missing, built,
                      double(traceBytes) / 1e9, staleTraces.size()));
        Report(Format("  .ninja_log:       %zu of %zu built; %s", built - noLog.size(), built,
                      ninjaLog.Runs == 1 ? Format("1 ninja run of %.1f min", double(ninjaLog.LastRunSpanMs) / 60000.0).c_str()
                                         : Format("%zu ninja runs, no build time", ninjaLog.Runs).c_str()));
        for (const std::string& w : warnings) {
            Report("WARNING: " + w);
        }
        for (const std::string& e : errors) {
            Report("ERROR: " + e);
        }
        return errors.empty();
    }

    // =========================================================================
    // the targets
    // =========================================================================

    std::string GnDir(const TFacts& facts, const std::string& dir) {
        std::string n = facts.Paths.Normalize(dir);
        const auto& roots = facts.Paths.SourceRoots;
        if (std::find(roots.begin(), roots.end(), n) != roots.end()) {
            return "//";
        }
        if (StartsWith(n, "<out>/")) {
            return "$root_build_dir/" + n.substr(6);
        }
        return IsAbs(n) || StartsWith(n, "<") ? n : "//" + n;
    }

    std::set<std::string> Reachable(const TFacts& facts, const std::vector<std::string>& start) {
        std::set<std::string> seen(start.begin(), start.end());
        std::vector<std::string> stack = start;
        while (!stack.empty()) {
            std::string label = std::move(stack.back());
            stack.pop_back();
            auto deps = facts.DepsOf.find(label);
            if (deps == facts.DepsOf.end()) {
                continue;
            }
            for (const std::string& dep : deps->second) {
                if (seen.count(dep)) {
                    continue;
                }
                auto type = facts.TypeOf.find(dep);
                if (type != facts.TypeOf.end() && type->second == "executable") {
                    continue;
                }
                seen.insert(dep);
                stack.push_back(dep);
            }
        }
        return seen;
    }

    std::vector<std::string> PchOnlyArgv(const TFacts& facts) {
        std::string path = JoinPath(facts.Root, PCH_FLAGS_GNI);
        std::string text;
        std::vector<std::string> argv;
        if (TryReadFile(path, &text)) {
            std::vector<std::pair<std::string, TGnValue>> values = ParseGnFile(text);
            const TGnValue* value = FindGnValue(values, "pch_only_cflags");
            if (value) {
                argv = value->Strings();
            }
        }
        if (argv.empty()) {
            Fail("no pch_only_cflags in " + std::string(PCH_FLAGS_GNI));
        }
        return argv;
    }

} // namespace NPchUpdate
