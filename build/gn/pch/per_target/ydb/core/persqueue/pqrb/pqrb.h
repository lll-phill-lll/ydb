// written by build/gn/scripts/pch_update.py
#pragma once

#include <future>
#include <stack>
#include "contrib/libs/fmt/include/fmt/format.h"
#include "contrib/restricted/abseil-cpp-tstring/y_absl/status/internal/status_internal.h"
#include "contrib/restricted/abseil-cpp/absl/container/btree_map.h"
#include "library/cpp/codecs/codecs.h"
#include "library/cpp/messagebus/config/netaddr.h"
#include "tools/enum_parser/enum_serialization_runtime/dispatch_methods.h"
#include "ydb/core/engine/minikql/flat_local_tx_factory.h"
#include "ydb/core/persqueue/common/logging.h"
#include "ydb/core/persqueue/events/global.h"
#include "ydb/core/persqueue/events/internal.h"
#include "ydb/core/persqueue/pqrb/partition_scale_manager_graph_cmp.h"
#include "ydb/core/tablet/tablet_counters_protobuf.h"
#include "ydb/core/tx/scheme_cache/scheme_cache.h"
#include "ydb/core/tx/schemeshard/schemeshard_info_types.h"
#include "ydb/library/actors/http/http_proxy.h"
#include "ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/topic/read_events.h"
#include "ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/topic/write_events.h"
#include "ydb/core/protos/msgbus_kv.pb.h"
#include "ydb/core/protos/tx_proxy.pb.h"
#include "ydb/public/api/protos/ydb_persqueue_cluster_discovery.pb.h"
