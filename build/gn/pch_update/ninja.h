#pragma once

#include <cstdint>
#include <memory>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

namespace NPchUpdate {

    // A manifest of ninja as ninja reads it: build.ninja with its includes
    // and subninjas, the paths relative to the build dir.
    class TNinjaManifest {
    public:
        struct TEdge {
            std::string Rule;
            // Explicit outputs first, then the implicit ones.
            std::vector<std::string> Outputs;
            size_t ExplicitOutputs = 0;
            std::vector<std::string> Inputs;
            std::vector<std::string> ImplicitInputs;
            std::vector<std::string> OrderOnly;
            std::vector<std::string> Validations;
            // The manifest file of the edge, relative to the build dir.
            std::string File;
        };

        explicit TNinjaManifest(const std::string& buildDir);
        ~TNinjaManifest();
        TNinjaManifest(const TNinjaManifest&) = delete;
        TNinjaManifest& operator=(const TNinjaManifest&) = delete;

        const std::vector<TEdge>& Edges() const {
            return Edges_;
        }

        // A variable of an edge as ninja evaluates it: $in, $out, the
        // bindings of the edge, of its rule, of its scopes.
        std::string Binding(size_t edge, std::string_view name) const;
        std::string Command(size_t edge) const {
            return Binding(edge, "command");
        }

        // The edge writing `path`, or -1.
        int64_t Producer(const std::string& path) const;
        // A top-level variable of build.ninja.
        std::string Variable(std::string_view name) const;

    private:
        struct TImpl;
        std::unique_ptr<TImpl> Impl_;
        std::vector<TEdge> Edges_;
    };

    // .ninja_log (v5, v6): the latest entry of each output.
    struct TNinjaLogEntry {
        int64_t StartMs = 0;
        int64_t EndMs = 0;
        int64_t MtimeNs = 0;
        uint64_t CommandHash = 0;
    };

    struct TNinjaLog {
        std::unordered_map<std::string, TNinjaLogEntry> Entries;
        // A run logs its commands as they end: an end earlier than the one
        // before starts a new run.
        size_t Runs = 0;
        int64_t LastRunSpanMs = 0;
    };

    TNinjaLog ReadNinjaLog(const std::string& path);

    // .ninja_deps (v3, v4): the dependencies of each output, as path ids.
    struct TNinjaDeps {
        std::vector<std::string> Paths;
        std::unordered_map<std::string, std::vector<int32_t>> Deps;
    };

    TNinjaDeps ReadNinjaDeps(const std::string& path);

    // DepsLog of ninja: the latest record of each output, the mtime of
    // its output when it was recorded.
    struct TNinjaDepsLog {
        std::vector<std::string> Paths;
        // By path id: its record, or -1.
        std::vector<int32_t> RecordOf;
        std::vector<int64_t> Mtimes;
        std::vector<std::vector<int32_t>> Records;
    };

    TNinjaDepsLog ReadNinjaDepsLog(const std::string& path);

    // A Makefile depfile as clang writes it: the inputs of its first target.
    std::vector<std::string> ReadDepfile(std::string_view text);
    std::string WriteDepfile(const std::string& target, const std::vector<std::string>& inputs);

    // Escapes for writing a manifest.
    std::string NinjaEscapePath(std::string_view path);
    std::string NinjaEscapeValue(std::string_view value);

} // namespace NPchUpdate
