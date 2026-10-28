// written by build/gn/scripts/pch_update.py
#pragma once

#include <thread>
#include "library/cpp/lwtrace/control.h"
#include "util/generic/queue.h"
#include "ydb/core/base/bridge.h"
#include "ydb/core/base/feature_flags.h"
#include "ydb/core/blobstorage/base/blobstorage_host_record.h"
#include "ydb/core/blobstorage/base/bufferwithgaps.h"
#include "ydb/core/blobstorage/groupinfo/blobstorage_groupinfo_iter.h"
#include "ydb/core/blobstorage/vdisk/common/memusage.h"
#include "ydb/core/blobstorage/vdisk/common/vdisk_costmodel.h"
#include "ydb/core/blobstorage/vdisk/common/vdisk_mongroups.h"
#include "ydb/core/blobstorage/vdisk/defrag/defs.h"
#include "ydb/library/actors/core/invoke.h"
#include "ydb/library/actors/wilson/wilson_span.h"
#include "ydb/library/pdisk_io/sector_map.h"
#include "ydb/core/protos/blobstorage_config.pb.h"
#include "ydb/core/protos/blobstorage_pdisk_config.pb.h"
#include "ydb/core/protos/blobstorage_vdisk_config.pb.h"
