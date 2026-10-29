#pragma once

#include <cstdint>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace NPchUpdate {

    // A value of a gn file of literals: groups.gni, targets.gni, args.gn...
    struct TGnValue {
        enum EKind {
            None,
            String,
            Bool,
            Integer,
            List,
            Scope,
            // An expression that is no literal (a variable, a call): as written.
            Other,
        };

        EKind Kind = None;
        // String: unescaped; Other: the text.
        std::string Text;
        bool BoolValue = false;
        int64_t IntValue = 0;
        std::vector<TGnValue> Items;
        // Scope: assignments in order.
        std::vector<std::pair<std::string, TGnValue>> Members;

        const TGnValue* Member(std::string_view name) const;
        std::vector<std::string> Strings() const;
    };

    // The top-level `name = value` of a gn file in order; `import`,
    // conditions and calls are skipped. Throws TError on bad syntax.
    std::vector<std::pair<std::string, TGnValue>> ParseGnFile(std::string_view text);
    const TGnValue* FindGnValue(const std::vector<std::pair<std::string, TGnValue>>& values, std::string_view name);

    // A string as gn reads it back as is: "..." with \ " $ escaped; with
    // `expand`, $ is left for gn to expand ("$root_build_dir/...").
    std::string GnQuote(std::string_view text, bool expand = false);
    // A list as gn format writes it, at `indent` spaces.
    std::string GnList(std::string_view name, const std::vector<std::string>& items, int indent, bool expand = false);

    // label_matches() of gn for a label without a toolchain.
    bool LabelMatches(std::string_view label, const std::vector<std::string>& patterns);

} // namespace NPchUpdate
