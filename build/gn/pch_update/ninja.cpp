#include "ninja.h"

#include "common.h"

#include <algorithm>
#include <charconv>
#include <cstdlib>
#include <cstring>
#include <deque>
#include <utility>

namespace NPchUpdate {

    namespace {

        enum ECharClass : uint8_t {
            CC_VAR = 1,   // simple_varname of ninja: [a-zA-Z0-9_-]
            CC_IDENT = 2, // varname: simple_varname and '.'
            CC_TEXT = 4,  // a plain char of a value or a path
        };

        struct TCharTable {
            uint8_t Classes[256] = {};

            TCharTable() {
                for (int c = 0; c < 256; ++c) {
                    bool var = (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') ||
                        c == '_' || c == '-';
                    if (var) {
                        Classes[c] |= CC_VAR | CC_IDENT;
                    }
                    if (c == '.') {
                        Classes[c] |= CC_IDENT;
                    }
                    if (c != '$' && c != ' ' && c != ':' && c != '\r' && c != '\n' && c != '|' && c != '\0') {
                        Classes[c] |= CC_TEXT;
                    }
                }
            }

            bool Is(char c, ECharClass cls) const {
                return Classes[static_cast<unsigned char>(c)] & cls;
            }
        };

        const TCharTable CHARS;

        // A string with $-references, as EvalString of ninja.
        class TEvalString {
        public:
            void AddText(const char* text, size_t size) {
                if (!Parts_.empty() && !Parts_.back().Var) {
                    Parts_.back().Size += size;
                } else {
                    Parts_.push_back({Text_.size(), size, false});
                }
                Text_.append(text, size);
            }

            void AddVar(const char* name, size_t size) {
                Parts_.push_back({Text_.size(), size, true});
                Text_.append(name, size);
            }

            bool Empty() const {
                return Parts_.empty();
            }

            void Clear() {
                Text_.clear();
                Parts_.clear();
            }

            // lookup(name, out) appends the value of a variable to out.
            template <class TLookup>
            void Evaluate(std::string* out, TLookup&& lookup) const {
                for (const TPart& part : Parts_) {
                    std::string_view text(Text_.data() + part.Begin, part.Size);
                    if (part.Var) {
                        lookup(text, out);
                    } else {
                        out->append(text);
                    }
                }
            }

        private:
            struct TPart {
                size_t Begin;
                size_t Size;
                bool Var;
            };

            std::string Text_;
            std::vector<TPart> Parts_;
        };

        struct TRule {
            std::string Name;
            std::vector<std::pair<std::string, TEvalString>> Bindings;

            const TEvalString* Find(std::string_view name) const {
                for (const auto& [key, value] : Bindings) {
                    if (key == name) {
                        return &value;
                    }
                }
                return nullptr;
            }

            void Add(std::string_view name, const TEvalString& value) {
                for (auto& [key, old] : Bindings) {
                    if (key == name) {
                        old = value;
                        return;
                    }
                }
                Bindings.emplace_back(std::string(name), value);
            }
        };

        // BindingEnv of ninja: the variables and rules of a file or the bindings of an edge.
        class TScope {
        public:
            explicit TScope(const TScope* parent)
                : Parent_(parent)
            {
            }

            const TScope* Parent() const {
                return Parent_;
            }

            const std::string* FindOwn(std::string_view name) const {
                if (!Index_.empty()) {
                    auto it = Index_.find(std::string(name));
                    return it == Index_.end() ? nullptr : &Vars_[it->second].second;
                }
                for (const auto& [key, value] : Vars_) {
                    if (key == name) {
                        return &value;
                    }
                }
                return nullptr;
            }

            const std::string* Find(std::string_view name) const {
                for (const TScope* scope = this; scope; scope = scope->Parent_) {
                    if (const std::string* value = scope->FindOwn(name)) {
                        return value;
                    }
                }
                return nullptr;
            }

            void Add(std::string_view name, std::string value) {
                if (const std::string* old = FindOwn(name)) {
                    *const_cast<std::string*>(old) = std::move(value);
                    return;
                }
                Vars_.emplace_back(std::string(name), std::move(value));
                if (Vars_.size() > MAX_LINEAR) {
                    if (Index_.empty()) {
                        for (size_t i = 0; i < Vars_.size(); ++i) {
                            Index_.emplace(Vars_[i].first, i);
                        }
                    } else {
                        Index_.emplace(Vars_.back().first, Vars_.size() - 1);
                    }
                }
            }

            const TRule* FindOwnRule(std::string_view name) const {
                auto it = Rules_.find(std::string(name));
                return it == Rules_.end() ? nullptr : it->second;
            }

            const TRule* FindRule(std::string_view name) const {
                for (const TScope* scope = this; scope; scope = scope->Parent_) {
                    if (const TRule* rule = scope->FindOwnRule(name)) {
                        return rule;
                    }
                }
                return nullptr;
            }

            void AddRule(const TRule* rule) {
                Rules_.emplace(rule->Name, rule);
            }

        private:
            static constexpr size_t MAX_LINEAR = 16;

            const TScope* Parent_;
            std::vector<std::pair<std::string, std::string>> Vars_;
            std::unordered_map<std::string, size_t> Index_;
            std::unordered_map<std::string, const TRule*> Rules_;
        };

        auto ScopeLookup(const TScope* scope) {
            return [scope](std::string_view name, std::string* out) {
                if (const std::string* value = scope->Find(name)) {
                    out->append(*value);
                }
            };
        }

        // CanonicalizePath of ninja (POSIX): no "." and empty components, "x/.." folded, leading ".." kept.
        void CanonicalizePath(std::string* path) {
            if (path->empty()) {
                return;
            }
            char* start = path->data();
            char* dst = start;
            const char* src = start;
            const char* end = start + path->size();
            if (*src == '/') {
                ++src;
                ++dst;
            }
            char* dstStart = dst;
            size_t components = 0;
            while (src < end) {
                const char* sep = static_cast<const char*>(memchr(src, '/', end - src));
                const char* componentEnd = sep ? sep : end;
                const char* next = sep ? sep + 1 : end;
                size_t size = componentEnd - src;
                if (size == 0 || (size == 1 && src[0] == '.')) {
                    src = next;
                    continue;
                }
                if (size == 2 && src[0] == '.' && src[1] == '.') {
                    if (components > 0) {
                        --components;
                        --dst;
                        while (dst > dstStart && dst[-1] != '/') {
                            --dst;
                        }
                    } else {
                        dst[0] = '.';
                        dst[1] = '.';
                        dst += 2;
                        if (sep) {
                            *dst++ = '/';
                        }
                    }
                    src = next;
                    continue;
                }
                ++components;
                size_t copy = next - src;
                if (dst != src) {
                    memmove(dst, src, copy);
                }
                dst += copy;
                src = next;
            }
            if (dst > dstStart && dst[-1] == '/') {
                --dst;
            }
            if (dst == start) {
                *dst++ = '.';
            }
            path->resize(dst - start);
        }

        bool IsShellSafe(char c) {
            return (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') ||
                c == '_' || c == '+' || c == '-' || c == '.' || c == '/';
        }

        // GetShellEscapedString of ninja.
        void AppendShellEscaped(std::string_view text, std::string* out) {
            if (std::all_of(text.begin(), text.end(), IsShellSafe)) {
                out->append(text);
                return;
            }
            out->push_back('\'');
            for (char c : text) {
                if (c == '\'') {
                    out->append("'\\''");
                } else {
                    out->push_back(c);
                }
            }
            out->push_back('\'');
        }

        class TLexer {
        public:
            enum EToken {
                LexError,
                Build,
                Colon,
                Default,
                Equals,
                Ident,
                Include,
                Indent,
                Newline,
                Pipe,
                Pipe2,
                PipeAt,
                Pool,
                Rule,
                Subninja,
                Eof,
            };

            TLexer(std::string_view file, std::string_view data)
                : File_(file)
                , Begin_(data.data())
                , Pos_(data.data())
                , End_(data.data() + data.size())
                , LastToken_(data.data())
            {
            }

            static const char* TokenName(EToken token) {
                switch (token) {
                    case LexError:
                        return "lexing error";
                    case Build:
                        return "'build'";
                    case Colon:
                        return "':'";
                    case Default:
                        return "'default'";
                    case Equals:
                        return "'='";
                    case Ident:
                        return "identifier";
                    case Include:
                        return "'include'";
                    case Indent:
                        return "indent";
                    case Newline:
                        return "newline";
                    case Pipe:
                        return "'|'";
                    case Pipe2:
                        return "'||'";
                    case PipeAt:
                        return "'|@'";
                    case Pool:
                        return "'pool'";
                    case Rule:
                        return "'rule'";
                    case Subninja:
                        return "'subninja'";
                    case Eof:
                        return "eof";
                }
                return "?";
            }

            EToken ReadToken() {
                EToken token = LexError;
                const char* start = Pos_;
                for (;;) {
                    start = Pos_;
                    const char* p = Pos_;
                    while (p < End_ && *p == ' ') {
                        ++p;
                    }
                    if (p < End_ && *p == '#') {
                        const char* eol = static_cast<const char*>(memchr(p, '\n', End_ - p));
                        Pos_ = eol ? eol + 1 : End_;
                        continue;
                    }
                    if (p < End_ && *p == '\n') {
                        Pos_ = p + 1;
                        token = Newline;
                        break;
                    }
                    if (p + 1 < End_ && p[0] == '\r' && p[1] == '\n') {
                        Pos_ = p + 2;
                        token = Newline;
                        break;
                    }
                    if (p > start) {
                        Pos_ = p;
                        token = Indent;
                        break;
                    }
                    if (p == End_ || *p == '\0') {
                        token = Eof;
                        break;
                    }
                    if (CHARS.Is(*p, CC_IDENT)) {
                        const char* identEnd = p;
                        while (identEnd < End_ && CHARS.Is(*identEnd, CC_IDENT)) {
                            ++identEnd;
                        }
                        std::string_view ident(p, identEnd - p);
                        Pos_ = identEnd;
                        token = Ident;
                        if (ident == "build") {
                            token = Build;
                        } else if (ident == "pool") {
                            token = Pool;
                        } else if (ident == "rule") {
                            token = Rule;
                        } else if (ident == "default") {
                            token = Default;
                        } else if (ident == "include") {
                            token = Include;
                        } else if (ident == "subninja") {
                            token = Subninja;
                        }
                        break;
                    }
                    Pos_ = p + 1;
                    if (*p == '=') {
                        token = Equals;
                    } else if (*p == ':') {
                        token = Colon;
                    } else if (*p == '|') {
                        if (p + 1 < End_ && p[1] == '@') {
                            Pos_ = p + 2;
                            token = PipeAt;
                        } else if (p + 1 < End_ && p[1] == '|') {
                            Pos_ = p + 2;
                            token = Pipe2;
                        } else {
                            token = Pipe;
                        }
                    } else {
                        token = LexError;
                    }
                    break;
                }
                LastToken_ = start;
                if (token != Newline && token != Eof) {
                    EatWhitespace();
                }
                return token;
            }

            bool PeekToken(EToken token) {
                if (ReadToken() == token) {
                    return true;
                }
                UnreadToken();
                return false;
            }

            void UnreadToken() {
                Pos_ = LastToken_;
            }

            void ExpectToken(EToken expected) {
                EToken token = ReadToken();
                if (token != expected) {
                    Throw(Format("expected %s, got %s", TokenName(expected), TokenName(token)));
                }
            }

            bool ReadIdent(std::string_view* out) {
                const char* p = Pos_;
                while (p < End_ && CHARS.Is(*p, CC_IDENT)) {
                    ++p;
                }
                LastToken_ = Pos_;
                if (p == Pos_) {
                    return false;
                }
                *out = std::string_view(Pos_, p - Pos_);
                Pos_ = p;
                EatWhitespace();
                return true;
            }

            // A path ends at an unescaped space, ':', '|' or newline, left unread;
            // a value ends at the end of the line, read.
            void ReadEvalString(TEvalString* eval, bool path) {
                const char* p = Pos_;
                const char* start = p;
                for (;;) {
                    start = p;
                    if (p == End_ || *p == '\0') {
                        LastToken_ = start;
                        Throw("unexpected EOF");
                    }
                    char c = *p;
                    if (CHARS.Is(c, CC_TEXT)) {
                        ++p;
                        while (p < End_ && CHARS.Is(*p, CC_TEXT)) {
                            ++p;
                        }
                        eval->AddText(start, p - start);
                        continue;
                    }
                    if (c == '\r' && p + 1 < End_ && p[1] == '\n') {
                        if (!path) {
                            p += 2;
                        }
                        break;
                    }
                    if (c == ' ' || c == ':' || c == '|' || c == '\n') {
                        if (path) {
                            break;
                        }
                        ++p;
                        if (c == '\n') {
                            break;
                        }
                        eval->AddText(start, 1);
                        continue;
                    }
                    if (c != '$') {
                        LastToken_ = start;
                        Throw(c == '\t' ? "tabs are not allowed, use spaces" : "lexing error");
                    }
                    char next = p + 1 < End_ ? p[1] : '\0';
                    if (next == '$' || next == ' ' || next == ':') {
                        eval->AddText(p + 1, 1);
                        p += 2;
                    } else if (next == '\n' || (next == '\r' && p + 2 < End_ && p[2] == '\n')) {
                        p += next == '\n' ? 2 : 3;
                        while (p < End_ && *p == ' ') {
                            ++p;
                        }
                    } else if (next == '{') {
                        const char* name = p + 2;
                        const char* nameEnd = name;
                        while (nameEnd < End_ && CHARS.Is(*nameEnd, CC_IDENT)) {
                            ++nameEnd;
                        }
                        if (nameEnd == name || nameEnd == End_ || *nameEnd != '}') {
                            LastToken_ = start;
                            Throw("bad $-escape (literal $ must be written as $$)");
                        }
                        eval->AddVar(name, nameEnd - name);
                        p = nameEnd + 1;
                    } else if (CHARS.Is(next, CC_VAR)) {
                        const char* name = p + 1;
                        const char* nameEnd = name;
                        while (nameEnd < End_ && CHARS.Is(*nameEnd, CC_VAR)) {
                            ++nameEnd;
                        }
                        eval->AddVar(name, nameEnd - name);
                        p = nameEnd;
                    } else {
                        LastToken_ = start;
                        Throw("bad $-escape (literal $ must be written as $$)");
                    }
                }
                LastToken_ = start;
                Pos_ = p;
                if (path) {
                    EatWhitespace();
                }
            }

            std::string DescribeLastError() const {
                return LastToken_ < End_ && *LastToken_ == '\t' ? "tabs are not allowed, use spaces" : "lexing error";
            }

            [[noreturn]] void Throw(const std::string& message) const {
                size_t line = 1 + std::count(Begin_, LastToken_, '\n');
                Fail(Format("ninja manifest %.*s:%zu: %s",
                    static_cast<int>(File_.size()), File_.data(), line, message.c_str()));
            }

        private:
            void EatWhitespace() {
                for (;;) {
                    if (Pos_ < End_ && *Pos_ == ' ') {
                        ++Pos_;
                    } else if (Pos_ + 1 < End_ && Pos_[0] == '$' && Pos_[1] == '\n') {
                        Pos_ += 2;
                    } else if (Pos_ + 2 < End_ && Pos_[0] == '$' && Pos_[1] == '\r' && Pos_[2] == '\n') {
                        Pos_ += 3;
                    } else {
                        break;
                    }
                }
            }

            std::string_view File_;
            const char* Begin_;
            const char* Pos_;
            const char* End_;
            const char* LastToken_;
        };

        struct TEdgeData {
            const TRule* Rule;
            // The bindings of the edge, or its file scope if it has none: ninja looks up
            // a variable there before the rule.
            const TScope* Env;
        };

        struct TManifestData {
            std::string BuildDir;
            std::deque<TScope> Scopes;
            // The first is phony.
            std::deque<TRule> Rules;
            std::vector<std::string> Pools;
            TScope* Root = nullptr;
            std::vector<TEdgeData> EdgeData;
            std::unordered_map<std::string_view, size_t> Producers;
        };

        bool IsReservedBinding(std::string_view name) {
            return name == "command" || name == "depfile" || name == "dyndep" || name == "description" ||
                name == "deps" || name == "generator" || name == "pool" || name == "restat" ||
                name == "rspfile" || name == "rspfile_content" || name == "msvc_deps_prefix";
        }

    } // namespace

    namespace {

        class TManifestParser {
        public:
            using TEdge = TNinjaManifest::TEdge;

            TManifestParser(TManifestData& impl, std::vector<TEdge>& edges)
                : Impl_(impl)
                , Edges_(edges)
            {
            }

            void ParseFile(const std::string& file, TScope* scope) {
                TMappedFile mapped(JoinPath(Impl_.BuildDir, file));
                TLexer lexer(file, mapped.Data());
                for (;;) {
                    TLexer::EToken token = lexer.ReadToken();
                    switch (token) {
                        case TLexer::Pool:
                            ParsePool(lexer, scope);
                            break;
                        case TLexer::Build:
                            ParseEdge(lexer, scope, file);
                            break;
                        case TLexer::Rule:
                            ParseRule(lexer, scope);
                            break;
                        case TLexer::Default:
                            ParseDefault(lexer);
                            break;
                        case TLexer::Ident: {
                            lexer.UnreadToken();
                            std::string_view name;
                            ParseLet(lexer, &name, &Value_);
                            std::string value;
                            Value_.Evaluate(&value, ScopeLookup(scope));
                            scope->Add(name, std::move(value));
                            break;
                        }
                        case TLexer::Include:
                            ParseInclude(lexer, scope, false);
                            break;
                        case TLexer::Subninja:
                            ParseInclude(lexer, scope, true);
                            break;
                        case TLexer::LexError:
                            lexer.Throw(lexer.DescribeLastError());
                        case TLexer::Eof:
                            return;
                        case TLexer::Newline:
                            break;
                        default:
                            lexer.Throw(std::string("unexpected ") + TLexer::TokenName(token));
                    }
                }
            }

        private:
            void ParseLet(TLexer& lexer, std::string_view* name, TEvalString* value) {
                if (!lexer.ReadIdent(name)) {
                    lexer.Throw("expected variable name");
                }
                lexer.ExpectToken(TLexer::Equals);
                value->Clear();
                lexer.ReadEvalString(value, false);
            }

            void ParseRule(TLexer& lexer, TScope* scope) {
                std::string_view name;
                if (!lexer.ReadIdent(&name)) {
                    lexer.Throw("expected rule name");
                }
                lexer.ExpectToken(TLexer::Newline);
                if (scope->FindOwnRule(name)) {
                    lexer.Throw("duplicate rule '" + std::string(name) + "'");
                }
                TRule& rule = Impl_.Rules.emplace_back();
                rule.Name = name;
                while (lexer.PeekToken(TLexer::Indent)) {
                    std::string_view key;
                    ParseLet(lexer, &key, &Value_);
                    if (!IsReservedBinding(key)) {
                        lexer.Throw("unexpected variable '" + std::string(key) + "'");
                    }
                    rule.Add(key, Value_);
                }
                const TEvalString* rspfile = rule.Find("rspfile");
                const TEvalString* rspfileContent = rule.Find("rspfile_content");
                if ((!rspfile || rspfile->Empty()) != (!rspfileContent || rspfileContent->Empty())) {
                    lexer.Throw("rspfile and rspfile_content need to be both specified");
                }
                const TEvalString* command = rule.Find("command");
                if (!command || command->Empty()) {
                    lexer.Throw("expected 'command =' line");
                }
                scope->AddRule(&rule);
                RuleCacheScope_ = nullptr;
            }

            void ParsePool(TLexer& lexer, TScope* scope) {
                std::string_view name;
                if (!lexer.ReadIdent(&name)) {
                    lexer.Throw("expected pool name");
                }
                lexer.ExpectToken(TLexer::Newline);
                if (std::find(Impl_.Pools.begin(), Impl_.Pools.end(), name) != Impl_.Pools.end()) {
                    lexer.Throw("duplicate pool '" + std::string(name) + "'");
                }
                int64_t depth = -1;
                while (lexer.PeekToken(TLexer::Indent)) {
                    std::string_view key;
                    ParseLet(lexer, &key, &Value_);
                    if (key != "depth") {
                        lexer.Throw("unexpected variable '" + std::string(key) + "'");
                    }
                    std::string value;
                    Value_.Evaluate(&value, ScopeLookup(scope));
                    depth = std::strtoll(value.c_str(), nullptr, 10);
                    if (depth < 0) {
                        lexer.Throw("invalid pool depth");
                    }
                }
                if (depth < 0) {
                    lexer.Throw("expected 'depth =' line");
                }
                Impl_.Pools.emplace_back(name);
            }

            void ParseDefault(TLexer& lexer) {
                Value_.Clear();
                lexer.ReadEvalString(&Value_, true);
                if (Value_.Empty()) {
                    lexer.Throw("expected target name");
                }
                while (!Value_.Empty()) {
                    Value_.Clear();
                    lexer.ReadEvalString(&Value_, true);
                }
                lexer.ExpectToken(TLexer::Newline);
            }

            void ParseInclude(TLexer& lexer, TScope* scope, bool newScope) {
                Value_.Clear();
                lexer.ReadEvalString(&Value_, true);
                std::string file;
                Value_.Evaluate(&file, ScopeLookup(scope));
                if (file.empty()) {
                    lexer.Throw("expected path");
                }
                TScope* fileScope = newScope ? &Impl_.Scopes.emplace_back(scope) : scope;
                ParseFile(file, fileScope);
                lexer.ExpectToken(TLexer::Newline);
            }

            // Reads paths into Paths_ from UsedPaths_ on; their number.
            size_t ReadPaths(TLexer& lexer) {
                size_t count = 0;
                for (;;) {
                    if (UsedPaths_ == Paths_.size()) {
                        Paths_.emplace_back();
                    }
                    TEvalString& path = Paths_[UsedPaths_];
                    path.Clear();
                    lexer.ReadEvalString(&path, true);
                    if (path.Empty()) {
                        return count;
                    }
                    ++UsedPaths_;
                    ++count;
                }
            }

            const TRule* FindRule(const TScope* scope, std::string_view name) {
                if (RuleCacheScope_ != scope || RuleCacheName_ != name) {
                    RuleCacheScope_ = scope;
                    RuleCacheName_ = name;
                    RuleCache_ = scope->FindRule(name);
                }
                return RuleCache_;
            }

            void ParseEdge(TLexer& lexer, TScope* scope, const std::string& file) {
                UsedPaths_ = 0;
                size_t explicitOuts = ReadPaths(lexer);
                size_t implicitOuts = 0;
                if (lexer.PeekToken(TLexer::Pipe)) {
                    implicitOuts = ReadPaths(lexer);
                }
                if (explicitOuts + implicitOuts == 0) {
                    lexer.Throw("expected path");
                }
                lexer.ExpectToken(TLexer::Colon);
                std::string_view ruleName;
                if (!lexer.ReadIdent(&ruleName)) {
                    lexer.Throw("expected build command name");
                }
                const TRule* rule = FindRule(scope, ruleName);
                if (!rule) {
                    lexer.Throw("unknown build rule '" + std::string(ruleName) + "'");
                }
                size_t ins = ReadPaths(lexer);
                size_t implicitIns = 0;
                if (lexer.PeekToken(TLexer::Pipe)) {
                    implicitIns = ReadPaths(lexer);
                }
                size_t orderOnly = 0;
                if (lexer.PeekToken(TLexer::Pipe2)) {
                    orderOnly = ReadPaths(lexer);
                }
                size_t validations = 0;
                if (lexer.PeekToken(TLexer::PipeAt)) {
                    validations = ReadPaths(lexer);
                }
                lexer.ExpectToken(TLexer::Newline);

                // Bindings are evaluated in the file scope, the paths in the edge scope.
                const TScope* env = scope;
                bool hasBinding = lexer.PeekToken(TLexer::Indent);
                if (hasBinding) {
                    TScope& edgeScope = Impl_.Scopes.emplace_back(scope);
                    while (hasBinding) {
                        std::string_view key;
                        ParseLet(lexer, &key, &Value_);
                        std::string value;
                        Value_.Evaluate(&value, ScopeLookup(scope));
                        edgeScope.Add(key, std::move(value));
                        hasBinding = lexer.PeekToken(TLexer::Indent);
                    }
                    env = &edgeScope;
                }

                TEdge& edge = Edges_.emplace_back();
                edge.Rule = rule->Name;
                edge.File = file;
                edge.ExplicitOutputs = explicitOuts;
                size_t next = 0;
                auto evaluate = [&](std::vector<std::string>* out, size_t count) {
                    out->reserve(count);
                    for (size_t i = 0; i < count; ++i) {
                        std::string path;
                        Paths_[next++].Evaluate(&path, ScopeLookup(env));
                        if (path.empty()) {
                            lexer.Throw("empty path");
                        }
                        CanonicalizePath(&path);
                        out->push_back(std::move(path));
                    }
                };
                evaluate(&edge.Outputs, explicitOuts + implicitOuts);
                evaluate(&edge.Inputs, ins);
                evaluate(&edge.ImplicitInputs, implicitIns);
                evaluate(&edge.OrderOnly, orderOnly);
                evaluate(&edge.Validations, validations);

                // As ninja with -w phonycycle=warn, its default: a phony naming itself as an input.
                if (rule == &Impl_.Rules.front() && edge.Outputs.size() == 1 && implicitOuts == 0 && implicitIns == 0) {
                    const std::string& out = edge.Outputs[0];
                    std::erase(edge.Inputs, out);
                    std::erase(edge.OrderOnly, out);
                }
                Impl_.EdgeData.push_back({rule, env});
            }

            TManifestData& Impl_;
            std::vector<TEdge>& Edges_;
            TEvalString Value_;
            std::vector<TEvalString> Paths_;
            size_t UsedPaths_ = 0;
            const TScope* RuleCacheScope_ = nullptr;
            std::string RuleCacheName_;
            const TRule* RuleCache_ = nullptr;
        };

        // EdgeEnv of ninja with shell escaping, as Edge::GetBinding.
        class TEdgeEnv {
        public:
            TEdgeEnv(const TNinjaManifest::TEdge& edge, const TEdgeData& data)
                : Edge_(edge)
                , Data_(data)
            {
            }

            void Append(std::string_view name, std::string* out) {
                if (name == "in" || name == "in_newline") {
                    AppendPaths(Edge_.Inputs.data(), Edge_.Inputs.size(), name == "in" ? ' ' : '\n', out);
                    return;
                }
                if (name == "out") {
                    AppendPaths(Edge_.Outputs.data(), Edge_.ExplicitOutputs, ' ', out);
                    return;
                }
                if (Recursive_) {
                    auto it = std::find(Lookups_.begin(), Lookups_.end(), name);
                    if (it != Lookups_.end()) {
                        std::string cycle;
                        for (; it != Lookups_.end(); ++it) {
                            cycle.append(*it).append(" -> ");
                        }
                        cycle.append(name);
                        Fail("ninja manifest: cycle in rule variables: " + cycle);
                    }
                }
                const TEvalString* eval = Data_.Rule->Find(name);
                bool record = Recursive_ && eval;
                if (record) {
                    Lookups_.push_back(name);
                }
                // Rule variables never use other rule variables in practice: the check starts
                // after the first lookup, as in ninja.
                Recursive_ = true;
                if (const std::string* own = Data_.Env->FindOwn(name)) {
                    out->append(*own);
                } else if (eval) {
                    eval->Evaluate(out, [this](std::string_view var, std::string* to) {
                        Append(var, to);
                    });
                } else if (Data_.Env->Parent()) {
                    if (const std::string* value = Data_.Env->Parent()->Find(name)) {
                        out->append(*value);
                    }
                }
                if (record) {
                    Lookups_.pop_back();
                }
            }

        private:
            static void AppendPaths(const std::string* paths, size_t count, char separator, std::string* out) {
                for (size_t i = 0; i < count; ++i) {
                    if (i) {
                        out->push_back(separator);
                    }
                    AppendShellEscaped(paths[i], out);
                }
            }

            const TNinjaManifest::TEdge& Edge_;
            const TEdgeData& Data_;
            std::vector<std::string_view> Lookups_;
            bool Recursive_ = false;
        };

    } // namespace

    struct TNinjaManifest::TImpl : TManifestData {
    };

    TNinjaManifest::TNinjaManifest(const std::string& buildDir)
        : Impl_(std::make_unique<TImpl>())
    {
        Impl_->BuildDir = buildDir;
        Impl_->Root = &Impl_->Scopes.emplace_back(nullptr);
        TRule& phony = Impl_->Rules.emplace_back();
        phony.Name = "phony";
        Impl_->Root->AddRule(&phony);
        Impl_->Pools.emplace_back("console");

        TManifestParser parser(*Impl_, Edges_);
        parser.ParseFile("build.ninja", Impl_->Root);

        Impl_->Producers.reserve(Edges_.size() * 2);
        for (size_t i = 0; i < Edges_.size(); ++i) {
            for (const std::string& out : Edges_[i].Outputs) {
                auto [it, inserted] = Impl_->Producers.emplace(out, i);
                if (!inserted) {
                    Fail(it->second == i
                        ? "ninja manifest: " + out + " is defined as an output multiple times"
                        : "ninja manifest: multiple rules generate " + out);
                }
            }
        }
    }

    TNinjaManifest::~TNinjaManifest() = default;

    std::string TNinjaManifest::Binding(size_t edge, std::string_view name) const {
        TEdgeEnv env(Edges_.at(edge), Impl_->EdgeData.at(edge));
        std::string out;
        env.Append(name, &out);
        return out;
    }

    int64_t TNinjaManifest::Producer(const std::string& path) const {
        auto it = Impl_->Producers.find(path);
        return it == Impl_->Producers.end() ? -1 : static_cast<int64_t>(it->second);
    }

    std::string TNinjaManifest::Variable(std::string_view name) const {
        const std::string* value = Impl_->Root->FindOwn(name);
        return value ? *value : std::string();
    }

    namespace {

        template <class T>
        bool ParseNumber(std::string_view text, T* value, int base = 10) {
            auto [end, error] = std::from_chars(text.data(), text.data() + text.size(), *value, base);
            return error == std::errc() && end == text.data() + text.size();
        }

    } // namespace

    TNinjaLog ReadNinjaLog(const std::string& path) {
        TMappedFile mapped(path);
        std::string_view data = mapped.Data();
        size_t eol = data.find('\n');
        std::string_view header = data.substr(0, eol);
        constexpr std::string_view signature = "# ninja log v";
        int version = 0;
        if (!StartsWith(header, signature) || !ParseNumber(header.substr(signature.size()), &version)) {
            Fail("unknown .ninja_log format: " + path);
        }
        if (version != 5 && version != 6) {
            Fail(Format("unsupported .ninja_log version %d: %s", version, path.c_str()));
        }

        TNinjaLog log;
        bool started = false;
        int64_t start = 0;
        int64_t prevEnd = 0;
        size_t pos = eol == std::string_view::npos ? data.size() : eol + 1;
        size_t lineNo = 1;
        while (pos < data.size()) {
            size_t end = data.find('\n', pos);
            if (end == std::string_view::npos) {
                end = data.size();
            }
            std::string_view line = data.substr(pos, end - pos);
            pos = end + 1;
            ++lineNo;

            std::string_view fields[5];
            size_t count = 0;
            size_t fieldStart = 0;
            for (;;) {
                size_t tab = line.find('\t', fieldStart);
                if (count == 4 || tab == std::string_view::npos) {
                    fields[count++] = line.substr(fieldStart);
                    break;
                }
                fields[count++] = line.substr(fieldStart, tab - fieldStart);
                fieldStart = tab + 1;
            }
            if (count < 4) {
                continue;
            }
            TNinjaLogEntry entry;
            if (!ParseNumber(fields[0], &entry.StartMs) || !ParseNumber(fields[1], &entry.EndMs) ||
                !ParseNumber(fields[2], &entry.MtimeNs) ||
                (count == 5 && !ParseNumber(fields[4].substr(0, fields[4].find('\t')), &entry.CommandHash, 16)))
            {
                Fail(Format("bad line %zu of %s", lineNo, path.c_str()));
            }
            if (!started || entry.EndMs < prevEnd) {
                started = true;
                ++log.Runs;
                start = entry.StartMs;
            }
            start = std::min(start, entry.StartMs);
            prevEnd = entry.EndMs;
            log.Entries[NormPath(fields[3])] = entry;
        }
        log.LastRunSpanMs = log.Runs ? prevEnd - start : 0;
        return log;
    }

    // ninja stops at a broken record and takes what is before it; so do these.
    TNinjaDepsLog ReadNinjaDepsLog(const std::string& path) {
        TMappedFile mapped(path);
        std::string_view data = mapped.Data();
        constexpr std::string_view signature = "# ninjadeps\n";
        if (!StartsWith(data, signature) || data.size() < signature.size() + 4) {
            Fail("unknown .ninja_deps format: " + path);
        }
        int32_t version;
        memcpy(&version, data.data() + signature.size(), 4);
        if (version != 3 && version != 4) {
            Fail(Format("unsupported .ninja_deps version %d: %s", version, path.c_str()));
        }
        const size_t mtimeSize = version == 4 ? 8 : 4;
        // kMaxRecordSize of ninja.
        const uint32_t maxRecordSize = (1u << 19) - 1;

        TNinjaDepsLog log;
        size_t offset = signature.size() + 4;
        while (offset + 4 <= data.size()) {
            uint32_t size;
            memcpy(&size, data.data() + offset, 4);
            offset += 4;
            bool isDeps = size >> 31;
            size &= 0x7FFFFFFF;
            if (size > maxRecordSize || offset + size > data.size()) {
                break;
            }
            const char* record = data.data() + offset;
            if (isDeps) {
                if (size < 4 + mtimeSize || (size - 4 - mtimeSize) % 4) {
                    break;
                }
                int32_t output;
                memcpy(&output, record, 4);
                if (output < 0 || static_cast<size_t>(output) >= log.Paths.size()) {
                    break;
                }
                int64_t mtime = 0;
                if (mtimeSize == 8) {
                    memcpy(&mtime, record + 4, 8);
                } else {
                    uint32_t seconds;
                    memcpy(&seconds, record + 4, 4);
                    mtime = int64_t(seconds);
                }
                std::vector<int32_t> ids((size - 4 - mtimeSize) / 4);
                if (!ids.empty()) {
                    memcpy(ids.data(), record + 4 + mtimeSize, ids.size() * 4);
                }
                bool bad = false;
                for (int32_t id : ids) {
                    if (id < 0 || static_cast<size_t>(id) >= log.Paths.size()) {
                        bad = true;
                        break;
                    }
                }
                if (bad) {
                    break;
                }
                int32_t& slot = log.RecordOf[output];
                if (slot < 0) {
                    slot = int32_t(log.Records.size());
                    log.Records.emplace_back();
                    log.Mtimes.push_back(0);
                }
                log.Records[slot] = std::move(ids);
                log.Mtimes[slot] = mtime;
            } else {
                if (size < 4) {
                    break;
                }
                size_t pathSize = size - 4;
                for (int i = 0; i < 3 && pathSize > 0 && record[pathSize - 1] == '\0'; ++i) {
                    --pathSize;
                }
                uint32_t checksum;
                memcpy(&checksum, record + size - 4, 4);
                if (static_cast<int32_t>(~checksum) != static_cast<int32_t>(log.Paths.size())) {
                    break;
                }
                log.Paths.emplace_back(record, pathSize);
                log.RecordOf.push_back(-1);
            }
            offset += size;
        }
        return log;
    }

    TNinjaDeps ReadNinjaDeps(const std::string& path) {
        TNinjaDepsLog log = ReadNinjaDepsLog(path);
        TNinjaDeps deps;
        deps.Deps.reserve(log.Records.size());
        for (size_t i = 0; i < log.Paths.size(); ++i) {
            if (log.RecordOf[i] >= 0) {
                deps.Deps[log.Paths[i]] = std::move(log.Records[log.RecordOf[i]]);
            }
        }
        deps.Paths = std::move(log.Paths);
        return deps;
    }

    std::vector<std::string> ReadDepfile(std::string_view text) {
        std::vector<std::string> inputs;
        std::string word;
        bool targets = true;
        bool seenTarget = false;
        auto finishWord = [&]() {
            if (word.empty()) {
                return;
            }
            if (targets) {
                seenTarget = true;
            } else {
                inputs.push_back(word);
            }
            word.clear();
        };
        auto isBlank = [&](size_t i) {
            return i >= text.size() || text[i] == ' ' || text[i] == '\t' || text[i] == '\n' || text[i] == '\r';
        };
        size_t i = 0;
        while (i < text.size()) {
            char c = text[i];
            if (c == '\n' || (c == '\r' && i + 1 < text.size() && text[i + 1] == '\n')) {
                finishWord();
                if (!targets) {
                    break;
                }
                // A target list without ':' is not a rule.
                seenTarget = false;
                i += c == '\n' ? 1 : 2;
                continue;
            }
            if (c == ' ' || c == '\t' || c == '\r') {
                finishWord();
                ++i;
                continue;
            }
            if (c == '\\') {
                size_t run = 0;
                while (i + run < text.size() && text[i + run] == '\\') {
                    ++run;
                }
                size_t after = i + run;
                char next = after < text.size() ? text[after] : '\0';
                if (next == '\n' || (next == '\r' && after + 1 < text.size() && text[after + 1] == '\n')) {
                    // A line continuation.
                    word.append(run - 1, '\\');
                    finishWord();
                    i = after + (next == '\n' ? 1 : 2);
                } else if (next == ' ') {
                    // 2N+1 backslashes and a space: N backslashes and a space; 2N: the end of a word.
                    if (run % 2) {
                        word.append(run / 2, '\\');
                        word.push_back(' ');
                    } else {
                        word.append(run, '\\');
                        finishWord();
                    }
                    i = after + 1;
                } else if (next == '#' || (next == ':' && !isBlank(after + 1))) {
                    word.append(run - 1, '\\');
                    word.push_back(next);
                    i = after + 1;
                } else {
                    word.append(run, '\\');
                    i = after;
                }
                continue;
            }
            if (c == '$' && i + 1 < text.size() && text[i + 1] == '$') {
                word.push_back('$');
                i += 2;
                continue;
            }
            if (c == ':' && targets && isBlank(i + 1)) {
                finishWord();
                if (seenTarget) {
                    targets = false;
                }
                ++i;
                continue;
            }
            word.push_back(c);
            ++i;
        }
        finishWord();
        return inputs;
    }

    namespace {

        // PrintFilename of clang for Makefile depfiles.
        void AppendDepfileName(std::string_view name, std::string* out) {
            for (size_t i = 0; i < name.size(); ++i) {
                char c = name[i];
                if (c == '\n' || c == '\r') {
                    Fail("a newline in a depfile name: " + std::string(name));
                }
                if (c == '#') {
                    out->push_back('\\');
                } else if (c == ' ') {
                    out->push_back('\\');
                    for (size_t j = i; j > 0 && name[j - 1] == '\\'; --j) {
                        out->push_back('\\');
                    }
                } else if (c == '$') {
                    out->push_back('$');
                }
                out->push_back(c);
            }
        }

        void CheckNinjaText(std::string_view text) {
            for (char c : text) {
                if (c == '\n' || c == '\r' || c == '\0') {
                    Fail("a newline in a ninja manifest string: " + std::string(text));
                }
            }
        }

    } // namespace

    std::string WriteDepfile(const std::string& target, const std::vector<std::string>& inputs) {
        std::string out;
        AppendDepfileName(target, &out);
        out.push_back(':');
        for (const std::string& input : inputs) {
            out.append(" \\\n  ");
            AppendDepfileName(input, &out);
        }
        out.push_back('\n');
        return out;
    }

    std::string NinjaEscapePath(std::string_view path) {
        CheckNinjaText(path);
        std::string out;
        out.reserve(path.size());
        for (char c : path) {
            if (c == '|') {
                // Ends a path and has no escape.
                Fail("'|' in a ninja path: " + std::string(path));
            }
            if (c == '$' || c == ' ' || c == ':') {
                out.push_back('$');
            }
            out.push_back(c);
        }
        return out;
    }

    std::string NinjaEscapeValue(std::string_view value) {
        CheckNinjaText(value);
        std::string out;
        out.reserve(value.size());
        // Leading spaces of a value are eaten after '='.
        bool leading = true;
        for (char c : value) {
            leading = leading && c == ' ';
            if (c == '$' || leading) {
                out.push_back('$');
            }
            out.push_back(c);
        }
        return out;
    }

} // namespace NPchUpdate
