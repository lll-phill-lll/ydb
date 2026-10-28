// written by build/gn/scripts/pch_update.py
#pragma once

#include "contrib/libs/protobuf/src/google/protobuf/any.pb.h"
#include "contrib/libs/protobuf/src/google/protobuf/duration.pb.h"
#include "ydb/core/blob_depot/s3.h"
#include "ydb/core/blob_depot/s3_router.h"
#include "ydb/core/wrappers/abstract.h"
#include "ydb/core/protos/s3_settings.pb.h"
#include "ydb/public/api/protos/annotations/validation.pb.h"
#include "ydb/public/api/protos/ydb_common.pb.h"
#include "ydb/public/api/protos/ydb_status_codes.pb.h"
