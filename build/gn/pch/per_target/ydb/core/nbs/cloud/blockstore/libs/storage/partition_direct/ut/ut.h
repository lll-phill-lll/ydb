// written by build/gn/scripts/pch_update.py
#pragma once

#include "ydb/core/base/appdata_fwd.h"
#include "ydb/core/base/domain.h"
#include "ydb/core/base/feature_flags.h"
#include "ydb/core/blobstorage/base/blobstorage_vdiskid.h"
#include "ydb/core/blobstorage/crypto/chacha_512/chacha_512.h"
#include "ydb/core/blobstorage/ddisk/defs.h"
#include "ydb/core/blobstorage/groupinfo/defs.h"
#include "ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/base_test_fixture.h"
#include "ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/fast_path_service.h"
#include "ydb/core/util/log_priority_mute_checker.h"
#include "ydb/core/protos/blobstorage.pb.h"
