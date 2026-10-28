// written by build/gn/scripts/pch_update.py
#pragma once

#include "ydb/core/kqp/common/kqp_yql.h"
#include "ydb/core/kqp/gateway/actors/kqp_ic_gateway_actors.h"
#include "ydb/core/kqp/opt/kqp_opt_generated_columns.h"
#include "ydb/core/kqp/opt/kqp_opt_impl.h"
#include "yql/essentials/core/yql_execution.h"
#include "yql/essentials/core/yql_opt_window.h"
#include "yql/essentials/providers/common/config/yql_dispatch.h"
#include "ydb/core/protos/analyze_operation.pb.h"
