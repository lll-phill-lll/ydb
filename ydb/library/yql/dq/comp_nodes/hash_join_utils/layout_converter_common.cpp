#include "layout_converter_common.h"
#include "tuple.h"

namespace NKikimr::NMiniKQL {

i64 TPackResult::AllocatedBytes() const {
    return PackedTuples.size() + Overflow.size();
}

} // namespace NKikimr::NMiniKQL
