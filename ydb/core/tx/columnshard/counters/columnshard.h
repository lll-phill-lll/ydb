#pragma once
#include "initialization.h"
#include "tx_progress.h"

#include <ydb/core/tx/data_events/common/signals_flow.h>

#include <ydb/library/signals/owner.h>

#include <library/cpp/monlib/dynamic_counters/counters.h>
#include <util/generic/hash_set.h>

namespace NKikimr::NColumnShard {

enum class EOverloadStatus {
    ShardTxInFly /* "shard_tx" */ = 0,
    ShardWritesInFly /* "shard_writes" */,
    ShardWritesSizeInFly /* "shard_writes_size" */,
    OverloadMetadata /* "overload_metadata" */,
    Disk /* "disk" */,
    None /* "none" */,
    OverloadCompaction /* "overload_compaction" */
};

enum class EWriteFailReason {
    Disabled /* "disabled" */ = 0,
    PutBlob /* "put_blob" */,
    LongTxDuplication /* "long_tx_duplication" */,
    NoTable /* "no_table" */,
    IncorrectSchema /* "incorrect_schema" */,
    Overload /* "overload" */,
    CompactionCriteria /* "compaction_criteria" */
};

class TWriteCounters: public TCommonCountersOwner {
private:
    using TBase = TCommonCountersOwner;
    NMonitoring::TDynamicCounters::TCounterPtr VolumeWriteData;
    NMonitoring::THistogramPtr HistogramBytesWriteDataCount;
    NMonitoring::THistogramPtr HistogramBytesWriteDataBytes;
    NMonitoring::THistogramPtr HistogramDurationQueueWait;
    NMonitoring::THistogramPtr HistogramBatchDataCount;
    NMonitoring::THistogramPtr HistogramBatchDataSize;
    YDB_READONLY_DEF(std::shared_ptr<NEvWrite::TWriteFlowCounters>, WriteFlowCounters);

public:
    const NMonitoring::TDynamicCounters::TCounterPtr QueueWaitSize;
    const NMonitoring::TDynamicCounters::TCounterPtr TimeoutRate;

    void OnWritingTaskDequeue(const TDuration d) {
        HistogramDurationQueueWait->Collect(d.MilliSeconds());
    }

    TWriteCounters(TCommonCountersOwner& owner)
        : TBase(owner, "activity", "writing")
        , WriteFlowCounters(std::make_shared<NEvWrite::TWriteFlowCounters>())
        , QueueWaitSize(TBase::GetValue("Write/Queue/Size"))
        , TimeoutRate(TBase::GetDeriviative("Write/Timeout/Count"))
    {
        VolumeWriteData = TBase::GetDeriviative("Write/Incoming/Bytes");
        HistogramBytesWriteDataCount = TBase::GetHistogram("Write/Incoming/ByBytes/Count", NMonitoring::ExponentialHistogram(18, 2, 100));
        HistogramBytesWriteDataBytes = TBase::GetHistogram("Write/Incoming/ByBytes/Bytes", NMonitoring::ExponentialHistogram(18, 2, 100));
        HistogramDurationQueueWait = TBase::GetHistogram("Write/Queue/Waiting/DurationMs", NMonitoring::ExponentialHistogram(18, 2, 100));
        HistogramBatchDataCount = TBase::GetHistogram("Write/Batch/Size/Count", NMonitoring::ExponentialHistogram(18, 2, 1));
        HistogramBatchDataSize = TBase::GetHistogram("Write/Batch/Size/Bytes", NMonitoring::ExponentialHistogram(18, 2, 128));
    }

    void OnIncomingData(const ui64 dataSize) const {
        VolumeWriteData->Add(dataSize);
        HistogramBytesWriteDataCount->Collect((i64)dataSize, 1);
        HistogramBytesWriteDataBytes->Collect((i64)dataSize, dataSize);
    }

    void OnAggregationWrite(const ui64 count, const ui64 dataSize) const {
        HistogramBatchDataCount->Collect((i64)count, 1);
        HistogramBatchDataSize->Collect((i64)dataSize, 1);
    }
};

class TCSCounters: public TCommonCountersOwner {
private:
    using TBase = TCommonCountersOwner;

    NMonitoring::TDynamicCounters::TCounterPtr StartBackgroundCount;
    NMonitoring::TDynamicCounters::TCounterPtr TooEarlyBackgroundCount;
    NMonitoring::TDynamicCounters::TCounterPtr SetupCompactionCount;
    NMonitoring::TDynamicCounters::TCounterPtr SetupIndexationCount;
    NMonitoring::TDynamicCounters::TCounterPtr SetupTtlCount;
    NMonitoring::TDynamicCounters::TCounterPtr SetupCleanupCount;

    NMonitoring::TDynamicCounters::TCounterPtr SkipIndexationInputDueToGranuleOverloadBytes;
    NMonitoring::TDynamicCounters::TCounterPtr SkipIndexationInputDueToGranuleOverloadCount;
    NMonitoring::TDynamicCounters::TCounterPtr SkipIndexationInputDueToSplitCompactionBytes;
    NMonitoring::TDynamicCounters::TCounterPtr SkipIndexationInputDueToSplitCompactionCount;
    NMonitoring::TDynamicCounters::TCounterPtr FutureIndexationInputBytes;
    NMonitoring::TDynamicCounters::TCounterPtr IndexationInputBytes;

    NMonitoring::TDynamicCounters::TCounterPtr IndexMetadataLimitBytes;

    NMonitoring::TDynamicCounters::TCounterPtr OverloadMetadataBytes;
    NMonitoring::TDynamicCounters::TCounterPtr OverloadMetadataCount;
    NMonitoring::TDynamicCounters::TCounterPtr OverloadCompactionBytes;
    NMonitoring::TDynamicCounters::TCounterPtr OverloadCompactionCount;
    NMonitoring::TDynamicCounters::TCounterPtr OverloadShardTxBytes;
    NMonitoring::TDynamicCounters::TCounterPtr OverloadShardTxCount;
    NMonitoring::TDynamicCounters::TCounterPtr OverloadShardWritesBytes;
    NMonitoring::TDynamicCounters::TCounterPtr OverloadShardWritesCount;
    NMonitoring::TDynamicCounters::TCounterPtr OverloadShardWritesSizeBytes;
    NMonitoring::TDynamicCounters::TCounterPtr OverloadShardWritesSizeCount;

    std::shared_ptr<TValueAggregationClient> InternalCompactionGranuleBytes;
    std::shared_ptr<TValueAggregationClient> InternalCompactionGranulePortionsCount;

    std::shared_ptr<TValueAggregationClient> SplitCompactionGranuleBytes;
    std::shared_ptr<TValueAggregationClient> SplitCompactionGranulePortionsCount;

    NMonitoring::THistogramPtr HistogramSuccessWritePutBlobsDurationMs;
    NMonitoring::THistogramPtr HistogramSuccessWriteMiddle1PutBlobsDurationMs;
    NMonitoring::THistogramPtr HistogramSuccessWriteMiddle2PutBlobsDurationMs;
    NMonitoring::THistogramPtr HistogramSuccessWriteMiddle3PutBlobsDurationMs;
    NMonitoring::THistogramPtr HistogramSuccessWriteMiddle4PutBlobsDurationMs;
    NMonitoring::THistogramPtr HistogramSuccessWriteMiddle5PutBlobsDurationMs;
    NMonitoring::THistogramPtr HistogramSuccessWriteMiddle6PutBlobsDurationMs;
    NMonitoring::THistogramPtr HistogramFailedWritePutBlobsDurationMs;
    NMonitoring::THistogramPtr HistogramWriteTxCompleteDurationMs;

    NMonitoring::TDynamicCounters::TCounterPtr WritePutBlobsCount;
    NMonitoring::TDynamicCounters::TCounterPtr WriteRequests;
    THashMap<EWriteFailReason, NMonitoring::TDynamicCounters::TCounterPtr> FailedWriteRequests;
    NMonitoring::TDynamicCounters::TCounterPtr SuccessWriteRequests;
    std::vector<NMonitoring::TDynamicCounters::TCounterPtr> WaitingOverloads;
    std::vector<NMonitoring::TDynamicCounters::TCounterPtr> WriteOverloadCount;
    std::vector<NMonitoring::TDynamicCounters::TCounterPtr> WriteOverloadBytes;

public:
    const std::shared_ptr<TWriteCounters> WritingCounters;
    const TCSInitialization Initialization;
    TTxProgressCounters TxProgress;

    void OnWaitingOverload(const EOverloadStatus status) const;

    void OnWriteOverload(const EOverloadStatus status, const ui32 size) const;

    void OnStartWriteRequest() const {
        WriteRequests->Add(1);
    }

    void OnFailedWriteResponse(const EWriteFailReason reason) const;

    void OnSuccessWriteResponse() const {
        WriteRequests->Sub(1);
        SuccessWriteRequests->Add(1);
    }

    void OnWritePutBlobsSuccess(const TDuration d) const {
        HistogramSuccessWritePutBlobsDurationMs->Collect(d.MilliSeconds());
    }

    void OnWriteMiddle1PutBlobsSuccess(const TDuration d) const {
        HistogramSuccessWriteMiddle1PutBlobsDurationMs->Collect(d.MilliSeconds());
    }

    void OnWriteMiddle2PutBlobsSuccess(const TDuration d) const {
        HistogramSuccessWriteMiddle2PutBlobsDurationMs->Collect(d.MilliSeconds());
    }

    void OnWriteMiddle3PutBlobsSuccess(const TDuration d) const {
        HistogramSuccessWriteMiddle3PutBlobsDurationMs->Collect(d.MilliSeconds());
    }

    void OnWriteMiddle4PutBlobsSuccess(const TDuration d) const {
        HistogramSuccessWriteMiddle4PutBlobsDurationMs->Collect(d.MilliSeconds());
    }

    void OnWriteMiddle5PutBlobsSuccess(const TDuration d) const {
        HistogramSuccessWriteMiddle5PutBlobsDurationMs->Collect(d.MilliSeconds());
    }

    void OnWriteMiddle6PutBlobsSuccess(const TDuration d) const {
        HistogramSuccessWriteMiddle6PutBlobsDurationMs->Collect(d.MilliSeconds());
    }

    void OnWritePutBlobsFail(const TDuration d) const {
        HistogramFailedWritePutBlobsDurationMs->Collect(d.MilliSeconds());
    }

    void OnWriteTxComplete(const TDuration d) const {
        HistogramWriteTxCompleteDurationMs->Collect(d.MilliSeconds());
    }

    void OnInternalCompactionInfo(const ui64 bytes, const ui32 portionsCount) const {
        InternalCompactionGranuleBytes->SetValue(bytes);
        InternalCompactionGranulePortionsCount->SetValue(portionsCount);
    }

    void OnSplitCompactionInfo(const ui64 bytes, const ui32 portionsCount) const {
        SplitCompactionGranuleBytes->SetValue(bytes);
        SplitCompactionGranulePortionsCount->SetValue(portionsCount);
    }

    void OnWriteOverloadMetadata(const ui64 size) const {
        OverloadMetadataBytes->Add(size);
        OverloadMetadataCount->Add(1);
    }

    void OnWriteOverloadCompaction(const ui64 size) const {
        OverloadCompactionBytes->Add(size);
        OverloadCompactionCount->Add(1);
    }

    void OnWriteOverloadShardTx(const ui64 size) const {
        OverloadShardTxBytes->Add(size);
        OverloadShardTxCount->Add(1);
    }

    void OnWriteOverloadShardWrites(const ui64 size) const {
        OverloadShardWritesBytes->Add(size);
        OverloadShardWritesCount->Add(1);
    }

    void OnWriteOverloadShardWritesSize(const ui64 size) const {
        OverloadShardWritesSizeBytes->Add(size);
        OverloadShardWritesSizeCount->Add(1);
    }

    void SkipIndexationInputDueToSplitCompaction(const ui64 size) const {
        SkipIndexationInputDueToSplitCompactionBytes->Add(size);
        SkipIndexationInputDueToSplitCompactionCount->Add(1);
    }

    void SkipIndexationInputDueToGranuleOverload(const ui64 size) const {
        SkipIndexationInputDueToGranuleOverloadBytes->Add(size);
        SkipIndexationInputDueToGranuleOverloadCount->Add(1);
    }

    void FutureIndexationInput(const ui64 size) const {
        FutureIndexationInputBytes->Add(size);
    }

    void IndexationInput(const ui64 size) const {
        IndexationInputBytes->Add(size);
    }

    void OnIndexMetadataLimit(const ui64 limit) const {
        IndexMetadataLimitBytes->Set(limit);
    }

    void OnStartBackground() const {
        StartBackgroundCount->Add(1);
    }

    void OnTooEarly() const {
        TooEarlyBackgroundCount->Add(1);
    }

    void OnSetupCompaction() const {
        SetupCompactionCount->Add(1);
    }

    void OnSetupIndexation() const {
        SetupIndexationCount->Add(1);
    }

    void OnSetupTtl() const {
        SetupTtlCount->Add(1);
    }

    void OnSetupCleanup() const {
        SetupCleanupCount->Add(1);
    }

    TCSCounters();
};

}   // namespace NKikimr::NColumnShard
