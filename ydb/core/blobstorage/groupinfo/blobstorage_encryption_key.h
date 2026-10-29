#pragma once

#include <ydb/core/blobstorage/crypto/crypto.h>

#include <util/generic/string.h>
#include <util/string/builder.h>
#include <util/string/escape.h>

namespace NKikimr {

// encryption key for the group
struct TEncryptionKey {
    TCypherKey Key;
    ui64 Version = 0;
    TString Id;

    TString ToString() const {
        return TStringBuilder() << "{Id# '" << EscapeC(Id) << "' Version# " << Version << "}";
    }

    explicit operator bool() const { // returns true if the key is set
        return Key.GetIsKeySet();
    }

    explicit operator const TCypherKey&() const {
        return Key;
    }
};

} // NKikimr
