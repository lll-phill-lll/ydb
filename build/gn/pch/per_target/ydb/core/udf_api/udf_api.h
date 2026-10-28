// written by build/gn/scripts/pch_update.py
#pragma once

#include "library/cpp/yt/memory/intrusive_ptr.h"
#include "ydb/core/base/auth.h"
#include "ydb/core/grpc_services/local_rate_limiter.h"
#include "ydb/core/tx/scheme_cache/scheme_cache.h"
#include "ydb/services/metadata/abstract/decoder.h"
#include "ydb/services/metadata/manager/object.h"
#include "ydb/library/ydb_issue/proto/issue_id.pb.h"
#include "ydb/public/api/protos/ydb_clickhouse_internal.pb.h"
#include "ydb/public/api/protos/ydb_coordination.pb.h"
#include "ydb/public/api/protos/ydb_federation_discovery.pb.h"
#include "ydb/public/api/protos/ydb_monitoring.pb.h"
#include "ydb/public/api/protos/ydb_persqueue_cluster_discovery.pb.h"
#include "ydb/public/api/protos/ydb_persqueue_v1.pb.h"
