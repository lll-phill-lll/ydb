#include "gni.h"

#include "common.h"

#include <limits>

namespace NPchUpdate {

    namespace {
        bool IsIdentifierStart(char c) {
            return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || c == '_';
        }

        bool IsDigit(char c) {
            return c >= '0' && c <= '9';
        }

        bool IsIdentifierChar(char c) {
            return IsIdentifierStart(c) || IsDigit(c);
        }

        using TGnMembers = std::vector<std::pair<std::string, TGnValue>>;

        // A recursive descent over the grammar of gn, keeping only literals.
        class TGnParser {
        public:
            explicit TGnParser(std::string_view text)
                : Text_(text)
            {
            }

            TGnMembers ParseFile() {
                TGnMembers out;
                ParseStatements(&out, '\0');
                return out;
            }

        private:
            [[noreturn]] void Error(const std::string& what) const {
                int line = 1;
                for (size_t i = 0; i < Pos_ && i < Text_.size(); ++i) {
                    line += Text_[i] == '\n';
                }
                Fail(Format("gn: line %d: %s", line, what.c_str()));
            }

            char Peek(size_t offset = 0) const {
                return Pos_ + offset < Text_.size() ? Text_[Pos_ + offset] : '\0';
            }

            bool AtEnd() const {
                return Pos_ >= Text_.size();
            }

            void SkipSpace() {
                while (!AtEnd()) {
                    char c = Peek();
                    if (c == '#') {
                        while (!AtEnd() && Peek() != '\n') {
                            ++Pos_;
                        }
                    } else if (c == ' ' || c == '\t' || c == '\n' || c == '\r') {
                        ++Pos_;
                    } else {
                        break;
                    }
                }
            }

            void Expect(char c) {
                SkipSpace();
                if (Peek() != c) {
                    Error(Format("expected '%c'", c));
                }
                ++Pos_;
            }

            std::string_view PeekIdentifier() const {
                if (!IsIdentifierStart(Peek())) {
                    return {};
                }
                size_t end = Pos_ + 1;
                while (end < Text_.size() && IsIdentifierChar(Text_[end])) {
                    ++end;
                }
                return Text_.substr(Pos_, end - Pos_);
            }

            std::string_view ParseIdentifier() {
                std::string_view out = PeekIdentifier();
                if (out.empty()) {
                    Error("expected a name");
                }
                Pos_ += out.size();
                return out;
            }

            // From an opening bracket at Pos_ past its closing one, strings and
            // comments seen as such.
            void SkipBalanced() {
                std::string open;
                while (true) {
                    if (AtEnd()) {
                        Error("unbalanced brackets");
                    }
                    char c = Text_[Pos_++];
                    if (c == '(' || c == '[' || c == '{') {
                        open += c;
                    } else if (c == ')' || c == ']' || c == '}') {
                        char expected = c == ')' ? '(' : c == ']' ? '[' : '{';
                        if (open.empty() || open.back() != expected) {
                            --Pos_;
                            Error(Format("unexpected '%c'", c));
                        }
                        open.pop_back();
                        if (open.empty()) {
                            return;
                        }
                    } else if (c == '"') {
                        --Pos_;
                        ParseString();
                    } else if (c == '#') {
                        while (!AtEnd() && Peek() != '\n') {
                            ++Pos_;
                        }
                    }
                }
            }

            // Assignments into `out` up to `close` ('\0': the end of the text);
            // conditions and calls skipped.
            void ParseStatements(TGnMembers* out, char close) {
                while (true) {
                    SkipSpace();
                    if (AtEnd()) {
                        if (close != '\0') {
                            Error(Format("expected '%c'", close));
                        }
                        return;
                    }
                    if (close != '\0' && Peek() == close) {
                        ++Pos_;
                        return;
                    }
                    ParseStatement(out);
                }
            }

            void ParseStatement(TGnMembers* out) {
                std::string_view name = ParseIdentifier();
                SkipSpace();
                if (name == "if") {
                    SkipCondition();
                    return;
                }
                if (Peek() == '(') {
                    // A call: import("..."), template("...") { ... }.
                    SkipBalanced();
                    SkipSpace();
                    if (Peek() == '{') {
                        SkipBalanced();
                    }
                    return;
                }
                // `+=` and `-=` give an entry as `=` does, of the operand.
                if (Peek() == '=' && Peek(1) != '=') {
                    Pos_ += 1;
                } else if ((Peek() == '+' || Peek() == '-') && Peek(1) == '=') {
                    Pos_ += 2;
                } else {
                    Error(Format("expected '=' after %.*s", int(name.size()), name.data()));
                }
                out->emplace_back(std::string(name), ParseExpression());
            }

            // After `if`: (...) { ... } [else if (...) { ... }] [else { ... }].
            void SkipCondition() {
                while (true) {
                    SkipSpace();
                    if (Peek() != '(') {
                        Error("expected '(' after if");
                    }
                    SkipBalanced();
                    SkipSpace();
                    if (Peek() != '{') {
                        Error("expected '{' after the condition");
                    }
                    SkipBalanced();
                    SkipSpace();
                    if (PeekIdentifier() != "else") {
                        return;
                    }
                    Pos_ += 4;
                    SkipSpace();
                    if (PeekIdentifier() == "if") {
                        Pos_ += 2;
                        continue;
                    }
                    if (Peek() != '{') {
                        Error("expected '{' after else");
                    }
                    SkipBalanced();
                    return;
                }
            }

            bool ParseBinaryOperator() {
                SkipSpace();
                char c = Peek();
                char next = Peek(1);
                if ((c == '=' || c == '!' || c == '<' || c == '>') && next == '=') {
                    Pos_ += 2;
                    return true;
                }
                if ((c == '&' && next == '&') || (c == '|' && next == '|')) {
                    Pos_ += 2;
                    return true;
                }
                if ((c == '+' || c == '-') && next != '=') {
                    Pos_ += 1;
                    return true;
                }
                if (c == '<' || c == '>') {
                    Pos_ += 1;
                    return true;
                }
                return false;
            }

            // A literal, or Other with the text of the expression.
            TGnValue ParseExpression() {
                SkipSpace();
                size_t start = Pos_;
                bool literal = true;
                TGnValue out = ParseUnary(&literal);
                size_t end = Pos_;
                while (ParseBinaryOperator()) {
                    literal = false;
                    ParseUnary(&literal);
                    end = Pos_;
                }
                Pos_ = end;
                if (!literal) {
                    out = {};
                    out.Kind = TGnValue::Other;
                    out.Text = std::string(Text_.substr(start, end - start));
                }
                return out;
            }

            TGnValue ParseUnary(bool* literal) {
                SkipSpace();
                if (Peek() == '!') {
                    ++Pos_;
                    *literal = false;
                    return ParseUnary(literal);
                }
                return ParsePrimary(literal);
            }

            TGnValue ParsePrimary(bool* literal) {
                SkipSpace();
                TGnValue out;
                char c = Peek();
                if (c == '"') {
                    out.Kind = TGnValue::String;
                    out.Text = ParseString();
                } else if (IsDigit(c) || (c == '-' && IsDigit(Peek(1)))) {
                    out.Kind = TGnValue::Integer;
                    out.IntValue = ParseInteger();
                } else if (c == '[') {
                    out.Kind = TGnValue::List;
                    ++Pos_;
                    ParseListItems(&out.Items);
                } else if (c == '{') {
                    out.Kind = TGnValue::Scope;
                    ++Pos_;
                    ParseStatements(&out.Members, '}');
                } else if (c == '(') {
                    SkipBalanced();
                    *literal = false;
                } else if (IsIdentifierStart(c)) {
                    std::string_view name = ParseIdentifier();
                    if (name == "true" || name == "false") {
                        out.Kind = TGnValue::Bool;
                        out.BoolValue = name == "true";
                    } else {
                        SkipAccessors();
                        *literal = false;
                    }
                } else if (AtEnd()) {
                    Error("expected a value");
                } else {
                    Error(Format("unexpected '%c'", c));
                }
                return out;
            }

            // After a name: calls, .member and [index] (written without spaces, as
            // gn format does).
            void SkipAccessors() {
                while (true) {
                    if (Peek() == '(' || Peek() == '[') {
                        SkipBalanced();
                    } else if (Peek() == '.' && IsIdentifierStart(Peek(1))) {
                        ++Pos_;
                        ParseIdentifier();
                    } else {
                        return;
                    }
                }
            }

            void ParseListItems(std::vector<TGnValue>* items) {
                while (true) {
                    SkipSpace();
                    if (Peek() == ']') {
                        ++Pos_;
                        return;
                    }
                    items->push_back(ParseExpression());
                    SkipSpace();
                    if (Peek() == ',') {
                        ++Pos_;
                    } else if (Peek() != ']') {
                        Error("expected ',' or ']'");
                    }
                }
            }

            int64_t ParseInteger() {
                bool negative = Peek() == '-';
                if (negative) {
                    ++Pos_;
                }
                // The magnitude in uint64_t: -2^63 fits.
                uint64_t limit = uint64_t(std::numeric_limits<int64_t>::max()) + (negative ? 1 : 0);
                uint64_t value = 0;
                while (IsDigit(Peek())) {
                    uint64_t digit = Peek() - '0';
                    if (value > (limit - digit) / 10) {
                        Error("an integer out of range");
                    }
                    value = value * 10 + digit;
                    ++Pos_;
                }
                if (IsIdentifierChar(Peek())) {
                    Error("a bad integer");
                }
                return negative ? int64_t(0 - value) : int64_t(value);
            }

            // The escapes \" \$ \\ undone; a $ of an interpolation ($name,
            // ${name}, $0xFF) kept as written; any other \ is itself, as in gn.
            std::string ParseString() {
                ++Pos_;
                std::string out;
                while (true) {
                    if (AtEnd()) {
                        Error("an unterminated string");
                    }
                    char c = Text_[Pos_++];
                    if (c == '"') {
                        return out;
                    }
                    if (c == '\\' && (Peek() == '"' || Peek() == '$' || Peek() == '\\')) {
                        c = Text_[Pos_++];
                    }
                    out += c;
                }
            }

        private:
            std::string_view Text_;
            size_t Pos_ = 0;
        };
    } // namespace

    const TGnValue* TGnValue::Member(std::string_view name) const {
        if (Kind != Scope) {
            return nullptr;
        }
        return FindGnValue(Members, name);
    }

    std::vector<std::string> TGnValue::Strings() const {
        if (Kind != List) {
            Fail("gn: a list of strings expected");
        }
        std::vector<std::string> out;
        out.reserve(Items.size());
        for (const TGnValue& item : Items) {
            if (item.Kind != String) {
                Fail("gn: a list of strings expected, an item is not a string: " + item.Text);
            }
            out.push_back(item.Text);
        }
        return out;
    }

    std::vector<std::pair<std::string, TGnValue>> ParseGnFile(std::string_view text) {
        return TGnParser(text).ParseFile();
    }

    // The first: a name assigned twice (or with += after =) has its first value.
    const TGnValue* FindGnValue(const std::vector<std::pair<std::string, TGnValue>>& values, std::string_view name) {
        for (const auto& [key, value] : values) {
            if (key == name) {
                return &value;
            }
        }
        return nullptr;
    }

    std::string GnQuote(std::string_view text, bool expand) {
        std::string out = "\"";
        for (char c : text) {
            if (c == '\\' || c == '"' || (c == '$' && !expand)) {
                out += '\\';
            }
            out += c;
        }
        out += '"';
        return out;
    }

    std::string GnList(std::string_view name, const std::vector<std::string>& items, int indent, bool expand) {
        std::string pad(indent, ' ');
        std::string out = pad;
        out += name;
        if (items.empty()) {
            out += " = []\n";
            return out;
        }
        if (items.size() == 1) {
            out += " = [ " + GnQuote(items[0], expand) + " ]\n";
            return out;
        }
        out += " = [\n";
        for (const std::string& item : items) {
            out += pad + "  " + GnQuote(item, expand) + ",\n";
        }
        out += pad + "]\n";
        return out;
    }

    bool LabelMatches(std::string_view label, const std::vector<std::string>& patterns) {
        // As str.partition(":").
        auto partition = [](std::string_view text) {
            size_t colon = text.find(':');
            if (colon == std::string_view::npos) {
                return std::make_pair(text, std::string_view());
            }
            return std::make_pair(text.substr(0, colon), text.substr(colon + 1));
        };
        auto [dir, name] = partition(label);
        for (const std::string& pattern : patterns) {
            auto [patternDir, patternName] = partition(pattern);
            if (EndsWith(patternDir, "/*") && patternName.empty()) {
                std::string_view base = patternDir.substr(0, patternDir.size() - 1);
                if (dir == base.substr(0, base.size() - 1) || StartsWith(dir, base)) {
                    return true;
                }
            } else if (patternDir == dir && (patternName == "*" || patternName == name)) {
                return true;
            }
        }
        return false;
    }

} // namespace NPchUpdate
