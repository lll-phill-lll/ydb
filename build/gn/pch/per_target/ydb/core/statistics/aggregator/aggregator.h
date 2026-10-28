// written by build/gn/scripts/pch_update.py
#pragma once

#include <stack>
#include "contrib/libs/protobuf/src/google/protobuf/type.pb.h"
#include "contrib/restricted/abseil-cpp-tstring/y_absl/status/internal/status_internal.h"
#include "library/cpp/monlib/dynamic_counters/percentile/percentile_base.h"
#include "ydb/core/scheme/scheme_types_proto.h"
#include "ydb/core/statistics/aggregator/aggregator_impl.h"
#include "ydb/core/tablet/tablet_counters_protobuf.h"
#include "ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/driver/driver.h"
#include "ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/topic/codecs.h"
#include "ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/topic/write_events.h"
#include "yql/essentials/core/histogram/eq_width_histogram.h"
#include "yql/essentials/public/decimal/yql_decimal.h"
#include "ydb/public/api/protos/ydb_coordination.pb.h"
