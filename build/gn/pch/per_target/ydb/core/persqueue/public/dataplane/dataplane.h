// written by build/gn/scripts/pch_update.py
#pragma once

#include "ydb/core/base/domain.h"
#include "ydb/core/base/runtime_feature_flags.h"
#include "ydb/core/blobstorage/defs.h"
#include "ydb/core/control/lib/dynamic_control_board_impl.h"
#include "ydb/core/persqueue/common/logging.h"
#include "ydb/core/persqueue/public/dataplane/write/partition_writer.h"
#include "ydb/library/pdisk_io/sector_map.h"
