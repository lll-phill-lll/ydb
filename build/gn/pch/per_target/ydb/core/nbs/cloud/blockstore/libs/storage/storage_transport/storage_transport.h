// written by build/gn/scripts/pch_update.py
#pragma once

#include "tools/enum_parser/enum_serialization_runtime/enum_runtime.h"
#include "ydb/core/base/feature_flags.h"
#include "ydb/core/blobstorage/ddisk/defs.h"
#include "ydb/core/blobstorage/groupinfo/blobstorage_groupinfo.h"
#include "ydb/core/blobstorage/vdisk/common/defs.h"
#include "ydb/core/mind/bscontroller/types.h"
#include "ydb/core/nbs/cloud/blockstore/libs/common/block_range/block_range.h"
#include "ydb/core/nbs/cloud/storage/core/libs/common/public.h"
#include "ydb/core/nbs/cloud/storage/core/protos/error.pb.h"
#include "ydb/core/protos/blobstorage_ddisk.pb.h"
