#pragma once

#include <cstdint>
#include <functional>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace NPchUpdate {

    class TError : public std::runtime_error {
    public:
        using std::runtime_error::runtime_error;
    };

    [[noreturn]] void Fail(const std::string& message);

    std::string Format(const char* format, ...) __attribute__((format(printf, 1, 2)));

    // "pch_update: <message>" on stderr and in the log of the run.
    void Log(const std::string& message);
    // A line of the report: stdout and the log of the run.
    void Report(const std::string& line);
    void OpenRunLog(const std::string& path);

    // Strings.
    bool StartsWith(std::string_view text, std::string_view prefix);
    bool EndsWith(std::string_view text, std::string_view suffix);
    std::vector<std::string_view> Split(std::string_view text, char separator);
    std::string Join(const std::vector<std::string>& parts, std::string_view separator);
    std::string Hours(double us);
    std::string Duration(double us);

    // Paths: as os.path of python.
    bool IsAbs(std::string_view path);
    std::string JoinPath(std::string_view a, std::string_view b);
    std::string NormPath(std::string_view path);
    std::string DirName(std::string_view path);
    std::string BaseName(std::string_view path);
    std::string RelPath(std::string_view path, std::string_view start);
    std::string RealPath(const std::string& path);
    std::string AbsPath(const std::string& path);

    // Files.
    std::string ReadFile(const std::string& path);
    bool TryReadFile(const std::string& path, std::string* out);
    // Written whole or not at all: a temporary file renamed over `path`.
    void WriteFile(const std::string& path, std::string_view data);
    // As WriteFile, but nothing done if `path` has `data` already (for ninja's restat).
    bool WriteFileIfChanged(const std::string& path, std::string_view data);
    void MakeDirs(const std::string& path);
    bool Exists(const std::string& path);
    bool IsDir(const std::string& path);
    // Nanoseconds; -1 if missing.
    int64_t MtimeNs(const std::string& path);
    int64_t FileSize(const std::string& path);
    void RemoveFile(const std::string& path);
    void RemoveTree(const std::string& path);
    std::vector<std::string> ListDir(const std::string& path);
    std::string MakeTempDir(const std::string& parent, const std::string& prefix);

    class TMappedFile {
    public:
        explicit TMappedFile(const std::string& path);
        ~TMappedFile();
        TMappedFile(const TMappedFile&) = delete;
        TMappedFile& operator=(const TMappedFile&) = delete;

        std::string_view Data() const {
            return {Data_, Size_};
        }

    private:
        const char* Data_ = nullptr;
        size_t Size_ = 0;
    };

    // Hashing: 128 bits, stable between runs and builds of the tool.
    class THasher {
    public:
        THasher();
        void Update(std::string_view data);
        // A field: its length first, so that ("ab", "c") and ("a", "bc") differ.
        void Field(std::string_view data);
        void Field(int64_t value);
        std::string Hex() const;

    private:
        uint64_t A_;
        uint64_t B_;
        uint64_t Length_ = 0;
    };

    std::string HashHex(std::string_view data);

    // Binary records of the files the tool writes for itself.
    class TBinWriter {
    public:
        void U64(uint64_t value);
        void I64(int64_t value);
        void Str(std::string_view value);
        void StrList(const std::vector<std::string>& values);
        const std::string& Data() const {
            return Data_;
        }

    private:
        std::string Data_;
    };

    class TBinReader {
    public:
        explicit TBinReader(std::string_view data)
            : Data_(data)
        {
        }

        uint64_t U64();
        int64_t I64();
        std::string_view Str();
        std::vector<std::string> StrList();
        bool AtEnd() const {
            return Pos_ >= Data_.size();
        }

    private:
        std::string_view Data_;
        size_t Pos_ = 0;
    };

    // Processes.
    struct TRunOptions {
        std::string Cwd;
        // Extra environment, NAME=value.
        std::vector<std::string> Env;
        bool CaptureStdout = true;
        bool CaptureStderr = true;
        // Stderr into stdout, as 2>&1.
        bool MergeStderr = false;
        // Given stdout as it comes, not kept.
        std::function<void(std::string_view)> OnStdout;
        std::string StdoutFile;
        std::string StderrFile;
    };

    struct TRunResult {
        // The exit code; 128 + signal if killed.
        int ExitCode = 0;
        std::string Stdout;
        std::string Stderr;
    };

    TRunResult Run(const std::vector<std::string>& argv, const TRunOptions& options = {});
    // As Run, failing unless the exit code is 0.
    std::string RunChecked(const std::vector<std::string>& argv, const TRunOptions& options = {});
    std::string SelfExe();

    // fn(i) for i in [0, n) on `threads` threads.
    void ParallelFor(size_t n, size_t threads, const std::function<void(size_t)>& fn);
    size_t CpuCount();

    // The words of a POSIX shell command without expansions, as gn writes
    // compile_commands.json: quotes and backslashes.
    std::vector<std::string> ShellSplit(std::string_view command);
    std::string ShellQuote(std::string_view word);
    std::string ShellJoin(const std::vector<std::string>& argv);

} // namespace NPchUpdate
