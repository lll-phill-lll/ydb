// written by build/gn/scripts/pch_update.py
#pragma once

#include <thread>
#include "library/cpp/testing/unittest/registar.h"
#include "ydb/core/base/channel_profiles.h"
#include "ydb/core/base/domain.h"
#include "ydb/core/base/statestorage.h"
#include "ydb/core/base/tablet.h"
#include "ydb/core/base/tablet_pipe.h"
#include "ydb/core/blobstorage/backpressure/queue_backpressure_client.h"
#include "ydb/core/blobstorage/pdisk/mock/pdisk_mock.h"
#include "ydb/core/blobstorage/vdisk/common/vdisk_pdiskctx.h"
#include "ydb/core/blobstorage/vdisk/hulldb/base/hullds_glue.h"
#include "ydb/core/cms/console/configs_dispatcher.h"
#include "ydb/core/cms/console/console.h"
#include "ydb/core/control/lib/dynamic_control_board_impl.h"
#include "ydb/library/actors/core/thread_context.h"
#include "ydb/library/pdisk_io/aio.h"
#include "ydb/core/protos/datashard_config.pb.h"
#include "ydb/core/protos/netclassifier.pb.h"
