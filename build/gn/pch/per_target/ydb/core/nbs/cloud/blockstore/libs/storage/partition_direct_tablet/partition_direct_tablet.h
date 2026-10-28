// written by build/gn/scripts/pch_update.py
#pragma once

#include <expected>
#include <thread>
#include "ydb/core/base/bridge.h"
#include "ydb/core/base/feature_flags.h"
#include "ydb/core/base/tablet_pipe.h"
#include "ydb/core/base/tracing.h"
#include "ydb/core/blobstorage/base/blobstorage_host_record.h"
#include "ydb/core/blobstorage/groupinfo/blobstorage_groupinfo_iter.h"
#include "ydb/core/blobstorage/pdisk/defs.h"
#include "ydb/core/engine/minikql/flat_local_tx_factory.h"
#include "ydb/core/nbs/cloud/blockstore/libs/storage/core/tablet.h"
#include "ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/fast_path_service.h"
#include "ydb/library/pdisk_io/sector_map.h"
#include "ydb/core/protos/blobstorage_config.pb.h"
#include "ydb/core/protos/blobstorage_pdisk_config.pb.h"
#include "ydb/core/protos/blockstore_config.pb.h"
