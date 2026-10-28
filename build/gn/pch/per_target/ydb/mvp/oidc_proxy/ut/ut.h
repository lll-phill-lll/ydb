// written by build/gn/scripts/pch_update.py
#pragma once

#include <future>
#include "library/cpp/getopt/small/last_getopt_opt.h"
#include "library/cpp/testing/unittest/registar.h"
#include "library/cpp/testing/unittest/tests_data.h"
#include "util/stream/file.h"
#include "ydb/core/scheme/scheme_pathid.h"
#include "ydb/library/actors/core/thread_context.h"
#include "ydb/library/actors/interconnect/mock/ic_mock.h"
#include "ydb/library/yql/dq/actors/dq_events_ids.h"
#include "ydb/mvp/core/mvp_tokens.h"
#include "ydb/mvp/oidc_proxy/extension.h"
#include "ydb/mvp/oidc_proxy/openid_connect.h"
#include "yql/essentials/public/issue/yql_issue.h"
#include "ydb/core/protos/base.pb.h"
#include "ydb/core/protos/labeled_counters.pb.h"
#include "ydb/core/protos/tablet_pipe.pb.h"
