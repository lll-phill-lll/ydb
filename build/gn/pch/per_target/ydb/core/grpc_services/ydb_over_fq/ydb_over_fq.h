// written by build/gn/scripts/pch_update.py
#pragma once

#include "ydb/core/fq/libs/common/util.h"
#include "ydb/core/fq/libs/quota_manager/events/events.h"
#include "ydb/core/grpc_services/rpc_deferrable.h"
#include "ydb/core/grpc_services/ydb_over_fq/fq_local_grpc_events.h"
#include "ydb/core/grpc_services/ydb_over_fq/service.h"
#include "ydb/library/actors/interconnect/events_local.h"
#include "ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/proto/accessor.h"
#include "ydb/core/fq/libs/protos/fq_private.pb.h"
#include "ydb/library/yql/dq/proto/dq_tasks.pb.h"
