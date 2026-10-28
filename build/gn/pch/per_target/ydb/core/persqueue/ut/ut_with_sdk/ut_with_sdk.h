// written by build/gn/scripts/pch_update.py
#pragma once

#include "ydb/core/persqueue/common/actor.h"
#include "ydb/core/persqueue/pqrb/partition_scale_manager_graph_cmp.h"
#include "ydb/core/persqueue/pqrb/partition_scale_request.h"
#include "ydb/core/persqueue/public/describer/describer.h"
#include "ydb/core/tx/schemeshard/schemeshard_info_types.h"
#include "ydb/core/tx/schemeshard/ut_helpers/helpers.h"
#include "ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/query/client.h"
#include "ydb/public/sdk/cpp/src/client/topic/ut/ut_utils/topic_sdk_test_setup.h"
#include "ydb/core/protos/grpc_pq_old.pb.h"
