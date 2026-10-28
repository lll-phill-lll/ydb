// written by build/gn/scripts/pch_update.py
#pragma once

#include "library/cpp/getopt/small/last_getopt_opts.h"
#include "ydb/library/actors/core/interconnect.h"
#include "ydb/library/fyamlcpp/fyamlcpp.h"
#include "ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/coordination/coordination.h"
#include "ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/export/export.h"
#include "ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/import/import.h"
#include "ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/table/table.h"
#include "ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/topic/write_session.h"
#include "ydb/services/workload_manager/metadata_subscription/resource_pool_classifier/behaviour.h"
#include "ydb/services/workload_manager/metadata_subscription/resource_pool_classifier/checker.h"
#include "ydb/services/workload_manager/metadata_subscription/resource_pool_classifier/snapshot.h"
#include "ydb/core/protos/console_config.pb.h"
#include "ydb/public/api/protos/ydb_config.pb.h"
#include "ydb/public/api/protos/ydb_coordination.pb.h"
