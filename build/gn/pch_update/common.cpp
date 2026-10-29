#include "common.h"

#include <algorithm>
#include <atomic>
#include <cerrno>
#include <climits>
#include <cstdarg>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <ctime>
#include <exception>
#include <mutex>
#include <thread>

#include <dirent.h>
#include <fcntl.h>
#include <poll.h>
#include <signal.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>

extern char** environ;

namespace NPchUpdate {

    namespace {
        std::mutex LogMutex;
        FILE* RunLog = nullptr;

        std::string ErrnoText(const std::string& what) {
            return what + ": " + strerror(errno);
        }
    } // namespace

    void Fail(const std::string& message) {
        throw TError(message);
    }

    std::string Format(const char* format, ...) {
        va_list args;
        va_start(args, format);
        va_list copy;
        va_copy(copy, args);
        int size = vsnprintf(nullptr, 0, format, copy);
        va_end(copy);
        std::string out(size > 0 ? size : 0, '\0');
        if (size > 0) {
            vsnprintf(out.data(), size + 1, format, args);
        }
        va_end(args);
        return out;
    }

    void Log(const std::string& message) {
        std::lock_guard<std::mutex> lock(LogMutex);
        fprintf(stderr, "pch_update: %s\n", message.c_str());
        fflush(stderr);
        if (RunLog) {
            fprintf(RunLog, "pch_update: %s\n", message.c_str());
            fflush(RunLog);
        }
    }

    void Report(const std::string& line) {
        std::lock_guard<std::mutex> lock(LogMutex);
        fprintf(stdout, "%s\n", line.c_str());
        fflush(stdout);
        if (RunLog) {
            fprintf(RunLog, "%s\n", line.c_str());
            fflush(RunLog);
        }
    }

    void OpenRunLog(const std::string& path) {
        MakeDirs(DirName(path));
        std::lock_guard<std::mutex> lock(LogMutex);
        RunLog = fopen(path.c_str(), "a");
        if (!RunLog) {
            Fail(ErrnoText("open " + path));
        }
    }

    bool StartsWith(std::string_view text, std::string_view prefix) {
        return text.size() >= prefix.size() && text.compare(0, prefix.size(), prefix) == 0;
    }

    bool EndsWith(std::string_view text, std::string_view suffix) {
        return text.size() >= suffix.size() && text.compare(text.size() - suffix.size(), suffix.size(), suffix) == 0;
    }

    std::vector<std::string_view> Split(std::string_view text, char separator) {
        std::vector<std::string_view> out;
        size_t start = 0;
        while (true) {
            size_t end = text.find(separator, start);
            if (end == std::string_view::npos) {
                out.push_back(text.substr(start));
                return out;
            }
            out.push_back(text.substr(start, end - start));
            start = end + 1;
        }
    }

    std::string Join(const std::vector<std::string>& parts, std::string_view separator) {
        std::string out;
        for (size_t i = 0; i < parts.size(); ++i) {
            if (i) {
                out += separator;
            }
            out += parts[i];
        }
        return out;
    }

    std::string Hours(double us) {
        double h = us / 3.6e9;
        return std::abs(h) >= 1 ? Format("%.1f h", h) : Format("%.0f min", h * 60);
    }

    std::string Duration(double us) {
        double s = us / 1e6;
        return std::abs(s) < 60 ? Format("%.0f s", s) : Format("%.1f min", s / 60);
    }

    bool IsAbs(std::string_view path) {
        return !path.empty() && path[0] == '/';
    }

    std::string JoinPath(std::string_view a, std::string_view b) {
        if (IsAbs(b) || a.empty()) {
            return std::string(b);
        }
        std::string out(a);
        if (out.back() != '/') {
            out += '/';
        }
        out += b;
        return out;
    }

    std::string NormPath(std::string_view path) {
        if (path.empty()) {
            return ".";
        }
        bool abs = IsAbs(path);
        // posixpath.normpath: two leading slashes are kept, more are one
        int leading = abs ? (StartsWith(path, "//") && !StartsWith(path, "///") ? 2 : 1) : 0;
        std::vector<std::string_view> parts;
        for (std::string_view part : Split(path, '/')) {
            if (part.empty() || part == ".") {
                continue;
            }
            if (part == "..") {
                if (!parts.empty() && parts.back() != "..") {
                    parts.pop_back();
                    continue;
                }
                if (abs) {
                    continue;
                }
            }
            parts.push_back(part);
        }
        std::string out(leading, '/');
        for (size_t i = 0; i < parts.size(); ++i) {
            if (i) {
                out += '/';
            }
            out += parts[i];
        }
        return out.empty() ? "." : out;
    }

    std::string DirName(std::string_view path) {
        size_t slash = path.rfind('/');
        if (slash == std::string_view::npos) {
            return "";
        }
        std::string_view head = path.substr(0, slash + 1);
        // trailing slashes off unless all slashes
        size_t end = head.find_last_not_of('/');
        return end == std::string_view::npos ? std::string(head) : std::string(head.substr(0, end + 1));
    }

    std::string BaseName(std::string_view path) {
        size_t slash = path.rfind('/');
        return std::string(slash == std::string_view::npos ? path : path.substr(slash + 1));
    }

    std::string RelPath(std::string_view path, std::string_view start) {
        std::string a = NormPath(AbsPath(std::string(path)));
        std::string b = NormPath(AbsPath(std::string(start)));
        auto as = Split(a, '/');
        auto bs = Split(b, '/');
        size_t common = 0;
        while (common < as.size() && common < bs.size() && as[common] == bs[common]) {
            ++common;
        }
        std::vector<std::string> parts;
        for (size_t i = common; i < bs.size(); ++i) {
            if (!bs[i].empty()) {
                parts.push_back("..");
            }
        }
        for (size_t i = common; i < as.size(); ++i) {
            if (!as[i].empty()) {
                parts.emplace_back(as[i]);
            }
        }
        return parts.empty() ? "." : Join(parts, "/");
    }

    std::string RealPath(const std::string& path) {
        char buffer[PATH_MAX];
        if (!realpath(path.c_str(), buffer)) {
            // as os.path.realpath: a missing path stays as it is, made absolute
            return NormPath(AbsPath(path));
        }
        return buffer;
    }

    std::string AbsPath(const std::string& path) {
        if (IsAbs(path)) {
            return NormPath(path);
        }
        char buffer[PATH_MAX];
        if (!getcwd(buffer, sizeof(buffer))) {
            Fail(ErrnoText("getcwd"));
        }
        return NormPath(JoinPath(buffer, path));
    }

    std::string ReadFile(const std::string& path) {
        std::string out;
        if (!TryReadFile(path, &out)) {
            Fail(ErrnoText("read " + path));
        }
        return out;
    }

    bool TryReadFile(const std::string& path, std::string* out) {
        int fd = open(path.c_str(), O_RDONLY | O_CLOEXEC);
        if (fd < 0) {
            return false;
        }
        out->clear();
        struct stat st;
        if (fstat(fd, &st) == 0 && st.st_size > 0) {
            out->reserve(st.st_size);
        }
        char buffer[1 << 16];
        while (true) {
            ssize_t n = read(fd, buffer, sizeof(buffer));
            if (n < 0) {
                if (errno == EINTR) {
                    continue;
                }
                int saved = errno;
                close(fd);
                errno = saved;
                return false;
            }
            if (n == 0) {
                break;
            }
            out->append(buffer, n);
        }
        close(fd);
        return true;
    }

    void WriteFile(const std::string& path, std::string_view data) {
        std::string dir = DirName(path);
        if (!dir.empty()) {
            MakeDirs(dir);
        }
        std::string tmp = Format("%s.%d.%zu.tmp", path.c_str(), getpid(),
                                 std::hash<std::thread::id>()(std::this_thread::get_id()) % 100000);
        int fd = open(tmp.c_str(), O_WRONLY | O_CREAT | O_TRUNC | O_CLOEXEC, 0644);
        if (fd < 0) {
            Fail(ErrnoText("write " + tmp));
        }
        size_t done = 0;
        while (done < data.size()) {
            ssize_t n = write(fd, data.data() + done, data.size() - done);
            if (n < 0) {
                if (errno == EINTR) {
                    continue;
                }
                int saved = errno;
                close(fd);
                unlink(tmp.c_str());
                errno = saved;
                Fail(ErrnoText("write " + tmp));
            }
            done += n;
        }
        if (close(fd) != 0 || rename(tmp.c_str(), path.c_str()) != 0) {
            int saved = errno;
            unlink(tmp.c_str());
            errno = saved;
            Fail(ErrnoText("write " + path));
        }
    }

    bool WriteFileIfChanged(const std::string& path, std::string_view data) {
        std::string old;
        if (TryReadFile(path, &old) && old == data) {
            return false;
        }
        WriteFile(path, data);
        return true;
    }

    void MakeDirs(const std::string& path) {
        if (path.empty() || IsDir(path)) {
            return;
        }
        std::string parent = DirName(path);
        if (!parent.empty() && parent != path) {
            MakeDirs(parent);
        }
        if (mkdir(path.c_str(), 0755) != 0 && errno != EEXIST) {
            Fail(ErrnoText("mkdir " + path));
        }
    }

    bool Exists(const std::string& path) {
        struct stat st;
        return stat(path.c_str(), &st) == 0;
    }

    bool IsDir(const std::string& path) {
        struct stat st;
        return stat(path.c_str(), &st) == 0 && S_ISDIR(st.st_mode);
    }

    int64_t MtimeNs(const std::string& path) {
        struct stat st;
        if (stat(path.c_str(), &st) != 0) {
            return -1;
        }
        return int64_t(st.st_mtim.tv_sec) * 1000000000 + st.st_mtim.tv_nsec;
    }

    int64_t FileSize(const std::string& path) {
        struct stat st;
        if (stat(path.c_str(), &st) != 0) {
            return -1;
        }
        return st.st_size;
    }

    void RemoveFile(const std::string& path) {
        if (unlink(path.c_str()) != 0 && errno != ENOENT) {
            Fail(ErrnoText("rm " + path));
        }
    }

    void RemoveTree(const std::string& path) {
        struct stat st;
        if (lstat(path.c_str(), &st) != 0) {
            return;
        }
        if (S_ISDIR(st.st_mode)) {
            for (const std::string& name : ListDir(path)) {
                RemoveTree(JoinPath(path, name));
            }
            if (rmdir(path.c_str()) != 0 && errno != ENOENT) {
                Fail(ErrnoText("rmdir " + path));
            }
            return;
        }
        RemoveFile(path);
    }

    std::vector<std::string> ListDir(const std::string& path) {
        std::vector<std::string> out;
        DIR* dir = opendir(path.c_str());
        if (!dir) {
            return out;
        }
        while (dirent* entry = readdir(dir)) {
            std::string_view name = entry->d_name;
            if (name != "." && name != "..") {
                out.emplace_back(name);
            }
        }
        closedir(dir);
        std::sort(out.begin(), out.end());
        return out;
    }

    std::string MakeTempDir(const std::string& parent, const std::string& prefix) {
        MakeDirs(parent);
        std::string pattern = JoinPath(parent, prefix + "XXXXXX");
        if (!mkdtemp(pattern.data())) {
            Fail(ErrnoText("mkdtemp " + pattern));
        }
        return pattern;
    }

    TMappedFile::TMappedFile(const std::string& path) {
        int fd = open(path.c_str(), O_RDONLY | O_CLOEXEC);
        if (fd < 0) {
            Fail(ErrnoText("open " + path));
        }
        struct stat st;
        if (fstat(fd, &st) != 0) {
            int saved = errno;
            close(fd);
            errno = saved;
            Fail(ErrnoText("stat " + path));
        }
        Size_ = st.st_size;
        if (Size_) {
            void* data = mmap(nullptr, Size_, PROT_READ, MAP_PRIVATE, fd, 0);
            if (data == MAP_FAILED) {
                int saved = errno;
                close(fd);
                errno = saved;
                Fail(ErrnoText("mmap " + path));
            }
            Data_ = static_cast<const char*>(data);
        }
        close(fd);
    }

    TMappedFile::~TMappedFile() {
        if (Data_) {
            munmap(const_cast<char*>(Data_), Size_);
        }
    }

    namespace {
        // Two lanes of 64-bit multiply-xorshift mixing, MurmurHash3's finalizer.
        constexpr uint64_t SEED_A = 0x9E3779B97F4A7C15ull;
        constexpr uint64_t SEED_B = 0xC2B2AE3D27D4EB4Full;

        uint64_t Mix(uint64_t x) {
            x ^= x >> 33;
            x *= 0xFF51AFD7ED558CCDull;
            x ^= x >> 33;
            x *= 0xC4CEB9FE1A85EC53ull;
            x ^= x >> 33;
            return x;
        }
    } // namespace

    THasher::THasher()
        : A_(SEED_A)
        , B_(SEED_B)
    {
    }

    void THasher::Update(std::string_view data) {
        size_t i = 0;
        for (; i + 8 <= data.size(); i += 8) {
            uint64_t word;
            memcpy(&word, data.data() + i, 8);
            A_ = Mix(A_ ^ word) + 0x2545F4914F6CDD1Dull;
            B_ = Mix(B_ + word * 0x9FB21C651E98DF25ull) ^ A_;
        }
        if (i < data.size()) {
            uint64_t word = 0;
            memcpy(&word, data.data() + i, data.size() - i);
            word ^= uint64_t(data.size() - i) << 56;
            A_ = Mix(A_ ^ word) + 0x2545F4914F6CDD1Dull;
            B_ = Mix(B_ + word * 0x9FB21C651E98DF25ull) ^ A_;
        }
        Length_ += data.size();
    }

    void THasher::Field(std::string_view data) {
        Field(int64_t(data.size()));
        Update(data);
    }

    void THasher::Field(int64_t value) {
        char bytes[8];
        memcpy(bytes, &value, 8);
        Update(std::string_view(bytes, 8));
    }

    std::string THasher::Hex() const {
        uint64_t a = Mix(A_ ^ Length_ ^ B_);
        uint64_t b = Mix(B_ + Length_ * SEED_A + a);
        return Format("%016llx%016llx", (unsigned long long)a, (unsigned long long)b);
    }

    std::string HashHex(std::string_view data) {
        THasher h;
        h.Update(data);
        return h.Hex();
    }

    void TBinWriter::U64(uint64_t value) {
        while (value >= 0x80) {
            Data_ += char(value | 0x80);
            value >>= 7;
        }
        Data_ += char(value);
    }

    void TBinWriter::I64(int64_t value) {
        U64((uint64_t(value) << 1) ^ uint64_t(value >> 63));
    }

    void TBinWriter::Str(std::string_view value) {
        U64(value.size());
        Data_ += value;
    }

    void TBinWriter::StrList(const std::vector<std::string>& values) {
        U64(values.size());
        for (const std::string& v : values) {
            Str(v);
        }
    }

    uint64_t TBinReader::U64() {
        uint64_t value = 0;
        for (int shift = 0; shift < 64; shift += 7) {
            if (Pos_ >= Data_.size()) {
                Fail("a truncated record");
            }
            uint8_t byte = Data_[Pos_++];
            value |= uint64_t(byte & 0x7F) << shift;
            if (!(byte & 0x80)) {
                return value;
            }
        }
        Fail("a bad record");
    }

    int64_t TBinReader::I64() {
        uint64_t v = U64();
        return int64_t(v >> 1) ^ -int64_t(v & 1);
    }

    std::string_view TBinReader::Str() {
        uint64_t size = U64();
        if (size > Data_.size() - Pos_) {
            Fail("a truncated record");
        }
        std::string_view out = Data_.substr(Pos_, size);
        Pos_ += size;
        return out;
    }

    std::vector<std::string> TBinReader::StrList() {
        std::vector<std::string> out(U64());
        for (std::string& s : out) {
            s = std::string(Str());
        }
        return out;
    }

    namespace {
        void ReadAll(int fd, std::string* out, const std::function<void(std::string_view)>* onData) {
            char buffer[1 << 16];
            while (true) {
                ssize_t n = read(fd, buffer, sizeof(buffer));
                if (n < 0 && errno == EINTR) {
                    continue;
                }
                if (n <= 0) {
                    return;
                }
                if (onData && *onData) {
                    (*onData)(std::string_view(buffer, n));
                } else if (out) {
                    out->append(buffer, n);
                }
            }
        }
    } // namespace

    TRunResult Run(const std::vector<std::string>& argv, const TRunOptions& options) {
        if (argv.empty()) {
            Fail("run: no command");
        }
        int outPipe[2] = {-1, -1};
        int errPipe[2] = {-1, -1};
        bool pipeOut = options.CaptureStdout && options.StdoutFile.empty();
        bool pipeErr = options.CaptureStderr && options.StderrFile.empty() && !options.MergeStderr;
        if ((pipeOut && pipe2(outPipe, O_CLOEXEC) != 0) || (pipeErr && pipe2(errPipe, O_CLOEXEC) != 0)) {
            Fail(ErrnoText("pipe"));
        }
        // all prepared before fork(): the child only calls async-signal-safe functions
        std::string program = argv[0];
        if (program.find('/') == std::string::npos) {
            const char* path = getenv("PATH");
            for (std::string_view dir : Split(path ? path : "/usr/bin:/bin", ':')) {
                std::string candidate = JoinPath(dir.empty() ? "." : dir, program);
                if (access(candidate.c_str(), X_OK) == 0) {
                    program = candidate;
                    break;
                }
            }
        }
        std::vector<char*> args;
        for (const std::string& a : argv) {
            args.push_back(const_cast<char*>(a.c_str()));
        }
        args.push_back(nullptr);
        std::vector<std::string> envStrings;
        for (char** e = environ; *e; ++e) {
            std::string_view entry = *e;
            bool replaced = false;
            for (const std::string& extra : options.Env) {
                size_t eq = extra.find('=');
                if (entry.substr(0, entry.find('=') + 1) == std::string_view(extra).substr(0, eq + 1)) {
                    replaced = true;
                }
            }
            if (!replaced) {
                envStrings.emplace_back(entry);
            }
        }
        envStrings.insert(envStrings.end(), options.Env.begin(), options.Env.end());
        std::vector<char*> env;
        for (std::string& e : envStrings) {
            env.push_back(e.data());
        }
        env.push_back(nullptr);
        int failPipe[2];
        if (pipe2(failPipe, O_CLOEXEC) != 0) {
            Fail(ErrnoText("pipe"));
        }
        pid_t pid = fork();
        if (pid < 0) {
            Fail(ErrnoText("fork"));
        }
        if (pid == 0) {
            int in = open("/dev/null", O_RDONLY);
            dup2(in, 0);
            if (!options.StdoutFile.empty()) {
                int fd = open(options.StdoutFile.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
                dup2(fd, 1);
            } else if (pipeOut) {
                dup2(outPipe[1], 1);
            }
            if (options.MergeStderr) {
                dup2(1, 2);
            } else if (!options.StderrFile.empty()) {
                int fd = open(options.StderrFile.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
                dup2(fd, 2);
            } else if (pipeErr) {
                dup2(errPipe[1], 2);
            }
            if (!options.Cwd.empty() && chdir(options.Cwd.c_str()) != 0) {
                int e = errno;
                ssize_t ignored = write(failPipe[1], &e, sizeof(e));
                (void)ignored;
                _exit(127);
            }
            execve(program.c_str(), args.data(), env.data());
            int e = errno;
            ssize_t ignored = write(failPipe[1], &e, sizeof(e));
            (void)ignored;
            _exit(127);
        }
        close(failPipe[1]);
        int childErrno = 0;
        ssize_t got;
        while ((got = read(failPipe[0], &childErrno, sizeof(childErrno))) < 0 && errno == EINTR) {
        }
        close(failPipe[0]);
        int rc = got == sizeof(childErrno) ? childErrno : 0;
        if (pipeOut) {
            close(outPipe[1]);
        }
        if (pipeErr) {
            close(errPipe[1]);
        }
        if (rc != 0) {
            if (pipeOut) {
                close(outPipe[0]);
            }
            if (pipeErr) {
                close(errPipe[0]);
            }
            int status;
            waitpid(pid, &status, 0);
            errno = rc;
            Fail(ErrnoText("run " + argv[0]));
        }
        TRunResult result;
        // stderr on a thread of its own: neither pipe may fill while the other is read
        std::thread errReader;
        if (pipeErr) {
            errReader = std::thread([&] {
                ReadAll(errPipe[0], &result.Stderr, nullptr);
            });
        }
        if (pipeOut) {
            ReadAll(outPipe[0], &result.Stdout, &options.OnStdout);
            close(outPipe[0]);
        }
        if (pipeErr) {
            errReader.join();
            close(errPipe[0]);
        }
        int status = 0;
        while (waitpid(pid, &status, 0) < 0) {
            if (errno != EINTR) {
                Fail(ErrnoText("waitpid"));
            }
        }
        result.ExitCode = WIFEXITED(status) ? WEXITSTATUS(status) : 128 + WTERMSIG(status);
        return result;
    }

    std::string RunChecked(const std::vector<std::string>& argv, const TRunOptions& options) {
        TRunResult r = Run(argv, options);
        if (r.ExitCode != 0) {
            std::string err = r.Stderr.size() > 4000 ? r.Stderr.substr(r.Stderr.size() - 4000) : r.Stderr;
            Fail(Format("%s failed (exit %d): %s", ShellJoin(argv).substr(0, 300).c_str(), r.ExitCode, err.c_str()));
        }
        return r.Stdout;
    }

    std::string SelfExe() {
        char buffer[PATH_MAX];
        ssize_t n = readlink("/proc/self/exe", buffer, sizeof(buffer) - 1);
        if (n <= 0) {
            Fail(ErrnoText("readlink /proc/self/exe"));
        }
        return std::string(buffer, n);
    }

    void ParallelFor(size_t n, size_t threads, const std::function<void(size_t)>& fn) {
        threads = std::max<size_t>(1, std::min(threads, n));
        if (threads == 1) {
            for (size_t i = 0; i < n; ++i) {
                fn(i);
            }
            return;
        }
        std::atomic<size_t> next{0};
        std::exception_ptr error;
        std::mutex errorMutex;
        std::vector<std::thread> pool;
        for (size_t t = 0; t < threads; ++t) {
            pool.emplace_back([&] {
                while (true) {
                    size_t i = next.fetch_add(1);
                    if (i >= n) {
                        return;
                    }
                    try {
                        fn(i);
                    } catch (...) {
                        std::lock_guard<std::mutex> lock(errorMutex);
                        if (!error) {
                            error = std::current_exception();
                        }
                        next = n;
                    }
                }
            });
        }
        for (std::thread& t : pool) {
            t.join();
        }
        if (error) {
            std::rethrow_exception(error);
        }
    }

    size_t CpuCount() {
        unsigned n = std::thread::hardware_concurrency();
        return n ? n : 1;
    }

    std::vector<std::string> ShellSplit(std::string_view command) {
        std::vector<std::string> out;
        std::string word;
        bool inWord = false;
        size_t i = 0;
        while (i < command.size()) {
            char c = command[i];
            if (c == ' ' || c == '\t' || c == '\n') {
                if (inWord) {
                    out.push_back(word);
                    word.clear();
                    inWord = false;
                }
                ++i;
                continue;
            }
            inWord = true;
            if (c == '\\') {
                if (i + 1 < command.size()) {
                    if (command[i + 1] != '\n') {
                        word += command[i + 1];
                    }
                    i += 2;
                } else {
                    ++i;
                }
                continue;
            }
            if (c == '\'') {
                size_t end = command.find('\'', i + 1);
                if (end == std::string_view::npos) {
                    Fail("an unterminated ' in: " + std::string(command.substr(0, 200)));
                }
                word.append(command.substr(i + 1, end - i - 1));
                i = end + 1;
                continue;
            }
            if (c == '"') {
                ++i;
                while (i < command.size() && command[i] != '"') {
                    if (command[i] == '\\' && i + 1 < command.size() &&
                        (command[i + 1] == '"' || command[i + 1] == '\\' || command[i + 1] == '$' || command[i + 1] == '`'))
                    {
                        word += command[i + 1];
                        i += 2;
                        continue;
                    }
                    if (command[i] == '\\' && i + 1 < command.size() && command[i + 1] == '\n') {
                        i += 2;
                        continue;
                    }
                    word += command[i++];
                }
                if (i >= command.size()) {
                    Fail("an unterminated \" in: " + std::string(command.substr(0, 200)));
                }
                ++i;
                continue;
            }
            word += c;
            ++i;
        }
        if (inWord) {
            out.push_back(word);
        }
        return out;
    }

    std::string ShellQuote(std::string_view word) {
        if (word.empty()) {
            return "''";
        }
        bool plain = std::all_of(word.begin(), word.end(), [](char c) {
            return isalnum(static_cast<unsigned char>(c)) || strchr("@%+=:,./-_", c);
        });
        if (plain) {
            return std::string(word);
        }
        std::string out = "'";
        for (char c : word) {
            if (c == '\'') {
                out += "'\\''";
            } else {
                out += c;
            }
        }
        out += '\'';
        return out;
    }

    std::string ShellJoin(const std::vector<std::string>& argv) {
        std::string out;
        for (size_t i = 0; i < argv.size(); ++i) {
            if (i) {
                out += ' ';
            }
            out += ShellQuote(argv[i]);
        }
        return out;
    }

} // namespace NPchUpdate
