#include "kqp_write_actor.h"

#include "kqp_write_table.h"
#include "kqp_write_actor_settings.h"

#include <util/generic/singleton.h>
#include <ydb/core/actorlib_impl/long_timer.h>
#include <ydb/core/base/tablet_pipecache.h>
#include <ydb/core/engine/minikql/minikql_engine_host.h>
#include <ydb/core/formats/arrow/arrow_batch_builder.h>
#include <ydb/core/kqp/common/buffer/buffer.h>
#include <ydb/core/kqp/common/kqp_data_integrity_trails.h>
#include <ydb/core/kqp/common/kqp_tx_manager.h>
#include <ydb/core/kqp/common/kqp_yql.h>
#include <ydb/core/kqp/common/simple/kqp_event_ids.h>
#include <ydb/core/protos/kqp_physical.pb.h>
#include <ydb/core/protos/query_stats.pb.h>
#include <ydb/core/scheme/scheme_types_proto.h>
#include <ydb/core/tx/data_events/events.h>
#include <ydb/core/tx/data_events/payload_helper.h>
#include <ydb/core/tx/data_events/shards_splitter.h>
#include <ydb/core/tx/scheme_cache/scheme_cache.h>
#include <ydb/core/tx/tx.h>
#include <ydb/core/persqueue/events/global.h>
#include <ydb/library/actors/core/actorsystem.h>
#include <ydb/library/actors/core/interconnect.h>
#include <ydb/library/wilson_ids/wilson.h>
#include <ydb/library/yql/dq/actors/compute/dq_compute_actor_impl.h>
#include <yql/essentials/public/issue/yql_issue_message.h>


namespace {
    TDuration CalculateNextAttemptDelay(const NKikimr::NKqp::TWriteActorSettings& settings, ui64 attempt) {
        auto delay = settings.StartRetryDelay;
        for (ui64 index = 0; index < attempt && delay * (1 - settings.UnsertaintyRatio) <= settings.MaxRetryDelay; ++index) {
            delay *= settings.Multiplier;
        }

        delay *= 1 + settings.UnsertaintyRatio * (1 - 2 * RandomNumber<double>());
        delay = Min(delay, settings.MaxRetryDelay);

        return delay;
    }

    NKikimrDataEvents::TEvWrite::TOperation::EOperationType GetOperation(NKikimrKqp::TKqpTableSinkSettings::EType type) {
        switch (type) {
        case NKikimrKqp::TKqpTableSinkSettings::MODE_FILL:
        case NKikimrKqp::TKqpTableSinkSettings::MODE_REPLACE:
            return NKikimrDataEvents::TEvWrite::TOperation::OPERATION_REPLACE;
        case NKikimrKqp::TKqpTableSinkSettings::MODE_UPSERT:
            return NKikimrDataEvents::TEvWrite::TOperation::OPERATION_UPSERT;
        case NKikimrKqp::TKqpTableSinkSettings::MODE_INSERT:
            return NKikimrDataEvents::TEvWrite::TOperation::OPERATION_INSERT;
        case NKikimrKqp::TKqpTableSinkSettings::MODE_DELETE:
            return NKikimrDataEvents::TEvWrite::TOperation::OPERATION_DELETE;
        case NKikimrKqp::TKqpTableSinkSettings::MODE_UPDATE:
            return NKikimrDataEvents::TEvWrite::TOperation::OPERATION_UPDATE;
        default:
            return NKikimrDataEvents::TEvWrite::TOperation::OPERATION_UNSPECIFIED;
        }
    }

    void FillEvWritePrepare(NKikimr::NEvents::TDataEvents::TEvWrite* evWrite, ui64 shardId, ui64 txId, const NKikimr::NKqp::IKqpTransactionManagerPtr& txManager) {
        evWrite->Record.SetTxId(txId);
        auto* protoLocks = evWrite->Record.MutableLocks();
        protoLocks->SetOp(NKikimrDataEvents::TKqpLocks::Commit);

        const auto prepareSettings = txManager->GetPrepareTransactionInfo();
        if (!prepareSettings.ArbiterColumnShard) {
            for (const ui64 sendingShardId : prepareSettings.SendingShards) {
                protoLocks->AddSendingShards(sendingShardId);
            }
            for (const ui64 receivingShardId : prepareSettings.ReceivingShards) {
                protoLocks->AddReceivingShards(receivingShardId);
            }
            if (prepareSettings.Arbiter) {
                protoLocks->SetArbiterShard(*prepareSettings.Arbiter);
            }
        } else if (prepareSettings.ArbiterColumnShard == shardId
                    && !prepareSettings.SendingShards.empty()
                    && !prepareSettings.ReceivingShards.empty()) {
            protoLocks->SetArbiterColumnShard(*prepareSettings.ArbiterColumnShard);
            for (const ui64 sendingShardId : prepareSettings.SendingShards) {
                protoLocks->AddSendingShards(sendingShardId);
            }
            for (const ui64 receivingShardId : prepareSettings.ReceivingShards) {
                protoLocks->AddReceivingShards(receivingShardId);
            }
        } else if (!prepareSettings.SendingShards.empty()
                    && !prepareSettings.ReceivingShards.empty()) {
            protoLocks->SetArbiterColumnShard(*prepareSettings.ArbiterColumnShard);
            protoLocks->AddSendingShards(*prepareSettings.ArbiterColumnShard);
            protoLocks->AddReceivingShards(*prepareSettings.ArbiterColumnShard);
            if (prepareSettings.SendingShards.contains(shardId)) {
                protoLocks->AddSendingShards(shardId);
            }
            if (prepareSettings.ReceivingShards.contains(shardId)) {
                protoLocks->AddReceivingShards(shardId);
            }
            std::sort(
                std::begin(*protoLocks->MutableSendingShards()),
                std::end(*protoLocks->MutableSendingShards()));
            std::sort(
                std::begin(*protoLocks->MutableReceivingShards()),
                std::end(*protoLocks->MutableReceivingShards()));
        }

        const auto locks = txManager->GetLocks(shardId);
        for (const auto& lock : locks) {
            *protoLocks->AddLocks() = lock;
        }
    }

    void FillEvWriteRollback(NKikimr::NEvents::TDataEvents::TEvWrite* evWrite, ui64 shardId, const NKikimr::NKqp::IKqpTransactionManagerPtr& txManager) {
        auto* protoLocks = evWrite->Record.MutableLocks();
        protoLocks->SetOp(NKikimrDataEvents::TKqpLocks::Rollback);

        const auto locks = txManager->GetLocks(shardId);
        for (const auto& lock : locks) {
            *protoLocks->AddLocks() = lock;
        }
    }

    void FillTopicsCommit(NKikimrPQ::TDataTransaction& transaction, const NKikimr::NKqp::IKqpTransactionManagerPtr& txManager) {
        transaction.SetOp(NKikimrPQ::TDataTransaction::Commit);
        const auto prepareSettings = txManager->GetPrepareTransactionInfo();

        if (!prepareSettings.ArbiterColumnShard) {
            for (const ui64 sendingShardId : prepareSettings.SendingShards) {
                transaction.AddSendingShards(sendingShardId);
            }
            for (const ui64 receivingShardId : prepareSettings.ReceivingShards) {
                transaction.AddReceivingShards(receivingShardId);
            }
        } else {
            transaction.AddSendingShards(*prepareSettings.ArbiterColumnShard);
            transaction.AddReceivingShards(*prepareSettings.ArbiterColumnShard);
        }
    }

    std::optional<NKikimrDataEvents::TMvccSnapshot> GetOptionalMvccSnapshot(const NKikimrKqp::TKqpTableSinkSettings& settings) {
        if (settings.HasMvccSnapshot()) {
            return settings.GetMvccSnapshot();
        } else {
            return std::nullopt;
        }
    }
}


namespace NKikimr {
namespace NKqp {

struct IKqpTableWriterCallbacks {
    virtual ~IKqpTableWriterCallbacks() = default;

    // Ready to accept writes
    virtual void OnReady() = 0;

    // EvWrite statuses
    virtual void OnPrepared(IKqpTransactionManager::TPrepareResult&& preparedInfo, ui64 dataSize) = 0;
    virtual void OnCommitted(ui64 shardId, ui64 dataSize) = 0;
    virtual void OnMessageAcknowledged(ui64 dataSize) = 0;

    virtual void OnError(NYql::NDqProto::StatusIds::StatusCode statusCode, NYql::EYqlIssueCode id, const TString& message, const NYql::TIssues& subIssues) = 0;
    virtual void OnError(NYql::NDqProto::StatusIds::StatusCode statusCode, NYql::TIssues&& issues) = 0;
};

struct TKqpTableWriterStatistics {
    ui64 ReadRows = 0;
    ui64 ReadBytes = 0;
    ui64 WriteRows = 0;
    ui64 WriteBytes = 0;
    ui64 EraseRows = 0;
    ui64 EraseBytes = 0;

    THashSet<ui64> AffectedPartitions;


    void UpdateStats(const NKikimrQueryStats::TTxStats& txStats, const TTableId& tableId) {
        for (const auto& tableAccessStats : txStats.GetTableAccessStats()) {
            YQL_ENSURE(tableAccessStats.GetTableInfo().GetPathId() == tableId.PathId.LocalPathId);
            ReadRows += tableAccessStats.GetSelectRow().GetRows();
            ReadRows += tableAccessStats.GetSelectRange().GetRows();
            ReadBytes += tableAccessStats.GetSelectRow().GetBytes();
            ReadBytes += tableAccessStats.GetSelectRange().GetBytes();
            WriteRows += tableAccessStats.GetUpdateRow().GetRows();
            WriteBytes += tableAccessStats.GetUpdateRow().GetBytes();
            EraseRows += tableAccessStats.GetEraseRow().GetRows();
            EraseBytes += tableAccessStats.GetEraseRow().GetRows();
        }

        for (const auto& perShardStats : txStats.GetPerShardStats()) {
            AffectedPartitions.insert(perShardStats.GetShardId());
        }
    }

    void FillStats(NYql::NDqProto::TDqTaskStats* stats, const TString& tablePath) {
        if (ReadRows + WriteRows + EraseRows == 0) {
            // Avoid empty table_access stats
            return;
        }
        NYql::NDqProto::TDqTableStats* tableStats = nullptr;
        for (size_t i = 0; i < stats->TablesSize(); ++i) {
            auto* table = stats->MutableTables(i);
            if (table->GetTablePath() == tablePath) {
                tableStats = table;
            }
        }
        if (!tableStats) {
            tableStats = stats->AddTables();
            tableStats->SetTablePath(tablePath);
        }

        tableStats->SetReadRows(tableStats->GetReadRows() + ReadRows);
        tableStats->SetReadBytes(tableStats->GetReadBytes() + ReadBytes);
        tableStats->SetWriteRows(tableStats->GetWriteRows() + WriteRows);
        tableStats->SetWriteBytes(tableStats->GetWriteBytes() + WriteBytes);
        tableStats->SetEraseRows(tableStats->GetEraseRows() + EraseRows);
        tableStats->SetEraseBytes(tableStats->GetEraseBytes() + EraseBytes);
    
        ReadRows = 0;
        ReadBytes = 0;
        WriteRows = 0;
        WriteBytes = 0;
        EraseRows = 0;
        EraseBytes = 0;

        tableStats->SetAffectedPartitions(
            tableStats->GetAffectedPartitions() + AffectedPartitions.size());
        AffectedPartitions.clear();
    }
};

class TKqpTableWriteActor : public TActorBootstrapped<TKqpTableWriteActor> {
    using TBase = TActorBootstrapped<TKqpTableWriteActor>;

    struct TEvPrivate {
        enum EEv {
            EvShardRequestTimeout = EventSpaceBegin(TKikimrEvents::ES_PRIVATE),
            EvResolveRequestPlanned,
            EvReattachToShard,
        };

        struct TEvShardRequestTimeout : public TEventLocal<TEvShardRequestTimeout, EvShardRequestTimeout> {
            ui64 ShardId;

            TEvShardRequestTimeout(ui64 shardId)
                : ShardId(shardId) {
            }
        };

        struct TEvResolveRequestPlanned : public TEventLocal<TEvResolveRequestPlanned, EvResolveRequestPlanned> {
        };

        struct TEvReattachToShard : public TEventLocal<TEvReattachToShard, EvReattachToShard> {
            const ui64 TabletId;

            explicit TEvReattachToShard(ui64 tabletId)
                : TabletId(tabletId) {}
        };
    };

    enum class EMode {
        WRITE,
        PREPARE,
        COMMIT,
        IMMEDIATE_COMMIT,
    };

public:
    TKqpTableWriteActor(
        IKqpTableWriterCallbacks* callbacks,
        const TTableId& tableId,
        const TStringBuf tablePath,
        const ui64 lockTxId,
        const ui64 lockNodeId,
        const bool inconsistentTx,
        const bool isOlap,
        TVector<NScheme::TTypeInfo> keyColumnTypes,
        std::shared_ptr<NKikimr::NMiniKQL::TScopedAlloc> alloc,
        const std::optional<NKikimrDataEvents::TMvccSnapshot>& mvccSnapshot,
        const NKikimrDataEvents::ELockMode lockMode,
        const IKqpTransactionManagerPtr& txManager,
        const TActorId sessionActorId,
        TIntrusivePtr<TKqpCounters> counters)
        : MessageSettings(GetWriteActorSettings())
        , Alloc(alloc)
        , MvccSnapshot(mvccSnapshot)
        , LockMode(lockMode)
        , TableId(tableId)
        , TablePath(tablePath)
        , LockTxId(lockTxId)
        , LockNodeId(lockNodeId)
        , InconsistentTx(inconsistentTx)
        , IsOlap(isOlap)
        , KeyColumnTypes(std::move(keyColumnTypes))
        , Callbacks(callbacks)
        , TxManager(txManager ? txManager : CreateKqpTransactionManager(/* collectOnly= */ true))
        , Counters(counters)
    {
        LogPrefix = TStringBuilder() << "Table: `" << TablePath << "` (" << TableId << "), " << "SessionActorId: " << sessionActorId;
        ShardedWriteController = CreateShardedWriteController(
            TShardedWriteControllerSettings {
                .MemoryLimitTotal = MessageSettings.InFlightMemoryLimitPerActorBytes,
                .Inconsistent = InconsistentTx,
            },
            Alloc);

        Counters->WriteActorsCount->Inc();
    }

    void Bootstrap() {
        LogPrefix = TStringBuilder() << "SelfId: " << this->SelfId() << ", " << LogPrefix;
        try {
            const auto partitioning = TxManager->GetPartitioning(TableId);
            if (!partitioning) {
                Resolve();
            } else {
                Partitioning = partitioning;
                Prepare();
            }
        } catch (const TMemoryLimitExceededException&) {
            RuntimeError(
                NYql::NDqProto::StatusIds::PRECONDITION_FAILED,
                NYql::TIssuesIds::KIKIMR_PRECONDITION_FAILED,
                TStringBuilder() << "Memory limit exception"
                    << ", current limit is " << Alloc->GetLimit() << " bytes.");
            return;
        } catch (...) {
            RuntimeError(
                NYql::NDqProto::StatusIds::INTERNAL_ERROR,
                NYql::TIssuesIds::KIKIMR_INTERNAL_ERROR,
                CurrentExceptionMessage());
            return;
        }

        Become(&TKqpTableWriteActor::StateProcessing);
    }

    static constexpr char ActorName[] = "KQP_TABLE_WRITE_ACTOR";

    i64 GetMemory() const {
        return IsReady()
            ? ShardedWriteController->GetMemory()
            : 0;
    }

    bool IsReady() const {
        return ShardedWriteController->IsReady();
    }

    bool IsEmpty() const {
        return ShardedWriteController->IsEmpty();
    }

    const TTableId& GetTableId() const {
        return TableId;
    }

    TVector<NKikimrDataEvents::TLock> GetLocks() const {
        return TxManager->GetLocks();
    }

    TVector<ui64> GetShardsIds() const {
        return ShardedWriteController->GetShardsIds();
    }

    std::optional<size_t> GetShardsCount() const {
        return InconsistentTx
            ? std::nullopt
            : std::optional<size_t>(ShardedWriteController->GetShardsCount());
    }

    using TWriteToken = IShardedWriteController::TWriteToken;

    void Open(
        const TWriteToken token,
        NKikimrDataEvents::TEvWrite::TOperation::EOperationType operationType,
        TVector<NKikimrKqp::TKqpColumnMetadataProto>&& keyColumnsMetadata,
        TVector<NKikimrKqp::TKqpColumnMetadataProto>&& columnsMetadata,
        std::vector<ui32>&& writeIndexes,
        i64 priority) {
        YQL_ENSURE(!Closed);
        ShardedWriteController->Open(
            token,
            TableId,
            operationType,
            std::move(keyColumnsMetadata),
            std::move(columnsMetadata),
            std::move(writeIndexes),
            priority);

        // At current time only insert operation can fail.
        NeedToFlushBeforeCommit |= (operationType == NKikimrDataEvents::TEvWrite::TOperation::OPERATION_INSERT);
        CA_LOG_D("Open: token=" << token);
    }

    void Write(TWriteToken token, IDataBatchPtr data) {
        YQL_ENSURE(!Closed);
        YQL_ENSURE(ShardedWriteController);
        CA_LOG_D("Write: token=" << token);
        ShardedWriteController->Write(token, std::move(data));
        UpdateShards();
    }

    void Close(TWriteToken token) {
        YQL_ENSURE(!Closed);
        YQL_ENSURE(ShardedWriteController);
        CA_LOG_D("Close: token=" << token);

        ShardedWriteController->Close(token);
        UpdateShards();
    }

    void Close() {
        YQL_ENSURE(!Closed);
        YQL_ENSURE(ShardedWriteController);
        YQL_ENSURE(ShardedWriteController->IsAllWritesClosed());
        Closed = true;
        ShardedWriteController->Close();
    }

    void CleanupClosedTokens() {
        YQL_ENSURE(ShardedWriteController);
        ShardedWriteController->CleanupClosedTokens();
        NeedToFlushBeforeCommit = false;
    }

    void SetParentTraceId(NWilson::TTraceId traceId) {
        ParentTraceId = std::move(traceId);
    }

    bool IsClosed() const {
        return Closed;
    }

    bool IsFinished() const {
        return IsClosed() && ShardedWriteController->IsAllWritesFinished();
    }

    STFUNC(StateProcessing) {
        try {
            switch (ev->GetTypeRewrite()) {
                hFunc(NKikimr::NEvents::TDataEvents::TEvWriteResult, Handle);
                hFunc(TEvTxProxySchemeCache::TEvNavigateKeySetResult, Handle);
                hFunc(TEvTxProxySchemeCache::TEvResolveKeySetResult, Handle);
                hFunc(TEvDataShard::TEvProposeTransactionAttachResult, Handle);
                hFunc(TEvPrivate::TEvReattachToShard, Handle);
                hFunc(TEvDataShard::TEvProposeTransactionRestart, Handle);
                hFunc(TEvPipeCache::TEvDeliveryProblem, Handle);
                hFunc(TEvPrivate::TEvShardRequestTimeout, Handle);
                hFunc(TEvPrivate::TEvResolveRequestPlanned, Handle);
                IgnoreFunc(TEvInterconnect::TEvNodeConnected);
                IgnoreFunc(TEvTxProxySchemeCache::TEvInvalidateTableResult);
            default:
                AFL_ENSURE(false)("unknown message", ev->GetTypeRewrite());
            }
        } catch (const TMemoryLimitExceededException&) {
            RuntimeError(
                NYql::NDqProto::StatusIds::PRECONDITION_FAILED,
                NYql::TIssuesIds::KIKIMR_PRECONDITION_FAILED,
                TStringBuilder() << "Memory limit exception"
                    << ", current limit is " << Alloc->GetLimit() << " bytes.");
            return;
        } catch (...) {
            RuntimeError(
                NYql::NDqProto::StatusIds::INTERNAL_ERROR,
                NYql::TIssuesIds::KIKIMR_INTERNAL_ERROR,
                CurrentExceptionMessage());
            return;
        }
    }

    bool IsResolving() const {
        return ResolveAttempts > 0;
    }

    void RetryResolve() {
        if (!IsResolving()) {
            Resolve();
        }
    }

    void Resolve() {
        AFL_ENSURE(InconsistentTx || IsOlap);
        TableWriteActorSpan = NWilson::TSpan(TWilsonKqp::TableWriteActor, NWilson::TTraceId(ParentTraceId),
            "WaitForTableResolve", NWilson::EFlags::AUTO_END);

        if (IsOlap) {
            ResolveTable();
        } else {
            ResolveShards();
        }
    }

    void PlanResolve() {
        CA_LOG_D("Plan resolve with delay " << CalculateNextAttemptDelay(MessageSettings, ResolveAttempts));
        TlsActivationContext->Schedule(
            CalculateNextAttemptDelay(MessageSettings, ResolveAttempts),
            new IEventHandle(SelfId(), SelfId(), new TEvPrivate::TEvResolveRequestPlanned{}, 0, 0));
    }

    void Handle(TEvPrivate::TEvResolveRequestPlanned::TPtr&) {
        RetryResolve();
    }

    void ResolveTable() {
        Counters->WriteActorsShardResolve->Inc();
        SchemeEntry.reset();
        Partitioning.reset();

        if (ResolveAttempts++ >= MessageSettings.MaxResolveAttempts) {
            CA_LOG_E(TStringBuilder()
                << "Too many table resolve attempts for table `" << TablePath << "` (" << TableId << ").");
            RuntimeError(
                NYql::NDqProto::StatusIds::SCHEME_ERROR,
                NYql::TIssuesIds::KIKIMR_SCHEME_ERROR,
                TStringBuilder()
                << "Too many table resolve attempts for table `" << TablePath << "`.");
            return;
        }

        CA_LOG_D("Resolve TableId=" << TableId);
        TAutoPtr<NSchemeCache::TSchemeCacheNavigate> request(new NSchemeCache::TSchemeCacheNavigate());
        NSchemeCache::TSchemeCacheNavigate::TEntry entry;
        entry.TableId = TableId;
        entry.RequestType = NSchemeCache::TSchemeCacheNavigate::TEntry::ERequestType::ByTableId;
        entry.Operation = NSchemeCache::TSchemeCacheNavigate::OpTable;
        entry.SyncVersion = false;
        entry.ShowPrivatePath = true;
        request->ResultSet.emplace_back(entry);

        Send(MakeSchemeCacheID(), new TEvTxProxySchemeCache::TEvNavigateKeySet(request), 0, 0, TableWriteActorSpan.GetTraceId());
    }

    void Handle(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev) {
        auto& resultSet = ev->Get()->Request->ResultSet;
        YQL_ENSURE(resultSet.size() == 1);

        if (ev->Get()->Request->ErrorCount > 0) {
            CA_LOG_E(TStringBuilder() << "Failed to get table: "
                << TableId << "'. Entry: " << resultSet[0].ToString());
            PlanResolve();
            return;
        }

        SchemeEntry = resultSet[0];

        CA_LOG_D("Resolved TableId=" << TableId << " ("
            << TablePath << " "
            << TableId.SchemaVersion << ")");

        if (TableId.SchemaVersion != SchemeEntry->TableId.SchemaVersion) {
            RuntimeError(
                NYql::NDqProto::StatusIds::SCHEME_ERROR,
                NYql::TIssuesIds::KIKIMR_SCHEME_ERROR,
                TStringBuilder() << "Schema was updated.");
            return;
        }

        YQL_ENSURE(IsOlap && (SchemeEntry->Kind == NSchemeCache::TSchemeCacheNavigate::KindColumnTable));

        Prepare();
    }

    void ResolveShards() {
        YQL_ENSURE(!KeyColumnTypes.empty());
        CA_LOG_D("Resolve shards for TableId=" << TableId);

        AFL_ENSURE(InconsistentTx); // Only for CTAS

        const TVector<TCell> minKey(KeyColumnTypes.size());
        const TTableRange range(minKey, true, {}, false, false);
        YQL_ENSURE(range.IsFullRange(KeyColumnTypes.size()));
        auto keyRange = MakeHolder<TKeyDesc>(
            TableId,
            range,
            TKeyDesc::ERowOperation::Update, // Only for CTAS
            KeyColumnTypes,
            TVector<TKeyDesc::TColumnOp>{});

        TAutoPtr<NSchemeCache::TSchemeCacheRequest> request(new NSchemeCache::TSchemeCacheRequest());
        request->ResultSet.emplace_back(std::move(keyRange));

        TAutoPtr<TEvTxProxySchemeCache::TEvResolveKeySet> resolveReq(new TEvTxProxySchemeCache::TEvResolveKeySet(request));
        Send(MakeSchemeCacheID(), resolveReq.Release(), 0, 0, TableWriteActorSpan.GetTraceId());
    }

    void Handle(TEvTxProxySchemeCache::TEvResolveKeySetResult::TPtr& ev) {
        auto* request = ev->Get()->Request.Get();

        if (request->ErrorCount > 0) {
            CA_LOG_E(TStringBuilder() << "Failed to get table: "
                << TableId << "'");
            PlanResolve();
            return;
        }

        YQL_ENSURE(request->ResultSet.size() == 1);
        Partitioning = std::move(request->ResultSet[0].KeyDescription->Partitioning);

        CA_LOG_D("Resolved shards for TableId=" << TableId << ". PartitionsCount=" << Partitioning->size() << ".");

        Prepare();
    }

    void Handle(NKikimr::NEvents::TDataEvents::TEvWriteResult::TPtr& ev) {
        auto getIssues = [&ev]() {
            NYql::TIssues issues;
            NYql::IssuesFromMessage(ev->Get()->Record.GetIssues(), issues);
            return issues;
        };

        CA_LOG_D("Recv EvWriteResult from ShardID=" << ev->Get()->Record.GetOrigin()
            << ", Status=" << NKikimrDataEvents::TEvWriteResult::EStatus_Name(ev->Get()->GetStatus())
            << ", TxId=" << ev->Get()->Record.GetTxId()
            << ", Locks= " << [&]() {
                TStringBuilder builder;
                for (const auto& lock : ev->Get()->Record.GetTxLocks()) {
                    builder << lock.ShortDebugString();
                }
                return builder;
            }()
            << ", Cookie=" << ev->Cookie);
        UpdateStats(ev->Get()->Record.GetTxStats());

        TxManager->AddParticipantNode(ev->Sender.NodeId());

        switch (ev->Get()->GetStatus()) {
        case NKikimrDataEvents::TEvWriteResult::STATUS_UNSPECIFIED: {
            CA_LOG_E("Got UNSPECIFIED for table `"
                    << TablePath << "`."
                    << " ShardID=" << ev->Get()->Record.GetOrigin() << ","
                    << " Sink=" << this->SelfId() << "."
                    << getIssues().ToOneLineString());
            TxManager->SetError(ev->Get()->Record.GetOrigin());
            RuntimeError(
                NYql::NDqProto::StatusIds::UNSPECIFIED,
                NYql::TIssuesIds::DEFAULT_ERROR,
                TStringBuilder() << "Unspecified error. Table `"
                    << TablePath << "`.",
                getIssues());
            return;
        }
        case NKikimrDataEvents::TEvWriteResult::STATUS_PREPARED: {
            ProcessWritePreparedShard(ev);
            return;
        }
        case NKikimrDataEvents::TEvWriteResult::STATUS_COMPLETED: {
            ProcessWriteCompletedShard(ev);
            return;
        }
        case NKikimrDataEvents::TEvWriteResult::STATUS_ABORTED: {
            CA_LOG_E("Got ABORTED for table `"
                    << TablePath << "`."
                    << " ShardID=" << ev->Get()->Record.GetOrigin() << ","
                    << " Sink=" << this->SelfId() << "."
                    << getIssues().ToOneLineString());
            TxManager->SetError(ev->Get()->Record.GetOrigin());
            RuntimeError(
                NYql::NDqProto::StatusIds::ABORTED,
                NYql::TIssuesIds::KIKIMR_OPERATION_ABORTED,
                TStringBuilder() << "Operation aborted.",
                getIssues());
            return;
        }
        case NKikimrDataEvents::TEvWriteResult::STATUS_WRONG_SHARD_STATE:
            CA_LOG_E("Got WRONG SHARD STATE for table `"
                    << TablePath << "`."
                    << " ShardID=" << ev->Get()->Record.GetOrigin() << ","
                    << " Sink=" << this->SelfId() << "."
                    << getIssues().ToOneLineString());
            TxManager->SetError(ev->Get()->Record.GetOrigin());
            
            if (InconsistentTx) {
                ResetShardRetries(ev->Get()->Record.GetOrigin(), ev->Cookie);
                RetryResolve();
            } else {
                RuntimeError(
                    NYql::NDqProto::StatusIds::UNAVAILABLE,
                    NYql::TIssuesIds::KIKIMR_TEMPORARILY_UNAVAILABLE,
                    TStringBuilder() << "Wrong shard state. Table `"
                        << TablePath << "`.",
                    getIssues());
            }
            return;
        case NKikimrDataEvents::TEvWriteResult::STATUS_INTERNAL_ERROR: {
            CA_LOG_E("Got INTERNAL ERROR for table `"
                    << TablePath << "`."
                    << " ShardID=" << ev->Get()->Record.GetOrigin() << ","
                    << " Sink=" << this->SelfId() << "."
                    << getIssues().ToOneLineString());
            TxManager->SetError(ev->Get()->Record.GetOrigin());
            RuntimeError(
                NYql::NDqProto::StatusIds::INTERNAL_ERROR,
                NYql::TIssuesIds::KIKIMR_INTERNAL_ERROR,
                TStringBuilder() << "Internal error while executing transaction.",
                getIssues());
            return;
        }
        case NKikimrDataEvents::TEvWriteResult::STATUS_DISK_SPACE_EXHAUSTED: {
            CA_LOG_E("Got DISK_SPACE_EXHAUSTED for table `"
                    << TablePath << "`."
                    << " ShardID=" << ev->Get()->Record.GetOrigin() << ","
                    << " Sink=" << this->SelfId() << "."
                    << getIssues().ToOneLineString());
            TxManager->SetError(ev->Get()->Record.GetOrigin());
            RuntimeError(
                NYql::NDqProto::StatusIds::UNAVAILABLE,
                NYql::TIssuesIds::KIKIMR_DISK_SPACE_EXHAUSTED,
                TStringBuilder() << "Disk space exhausted. Table `"
                    << TablePath << "`.",
                getIssues());
            return;
        }
        case NKikimrDataEvents::TEvWriteResult::STATUS_OUT_OF_SPACE: {
            CA_LOG_W("Got OUT_OF_SPACE for table `"
                << TablePath << "`."
                << " ShardID=" << ev->Get()->Record.GetOrigin() << ","
                << " Sink=" << this->SelfId() << "."
                << " Ignored this error."
                << getIssues().ToOneLineString());
            // TODO: support waiting
            if (!InconsistentTx)  {
                TxManager->SetError(ev->Get()->Record.GetOrigin());
                RuntimeError(
                    NYql::NDqProto::StatusIds::OVERLOADED,
                    NYql::TIssuesIds::KIKIMR_OVERLOADED,
                    TStringBuilder() << "Tablet " << ev->Get()->Record.GetOrigin() << " is out of space. Table `"
                        << TablePath << "`.",
                    getIssues());
            }
            return;
        }        
        case NKikimrDataEvents::TEvWriteResult::STATUS_OVERLOADED: {
            CA_LOG_W("Got OVERLOADED for table `"
                << TablePath << "`."
                << " ShardID=" << ev->Get()->Record.GetOrigin() << ","
                << " Sink=" << this->SelfId() << "."
                << " Ignored this error."
                << getIssues().ToOneLineString());
            // TODO: support waiting
            if (!InconsistentTx)  {
                TxManager->SetError(ev->Get()->Record.GetOrigin());
                RuntimeError(
                    NYql::NDqProto::StatusIds::OVERLOADED,
                    NYql::TIssuesIds::KIKIMR_OVERLOADED,
                    TStringBuilder() << "Kikimr cluster or one of its subsystems is overloaded."
                        << " Tablet " << ev->Get()->Record.GetOrigin() << " is overloaded. Table `"
                        << TablePath << "`.",
                    getIssues());
            }
            return;
        }
        case NKikimrDataEvents::TEvWriteResult::STATUS_CANCELLED: {
            CA_LOG_E("Got CANCELLED for table `"
                    << TablePath << "`."
                    << " ShardID=" << ev->Get()->Record.GetOrigin() << ","
                    << " Sink=" << this->SelfId() << "."
                    << getIssues().ToOneLineString());
            TxManager->SetError(ev->Get()->Record.GetOrigin());
            RuntimeError(
                NYql::NDqProto::StatusIds::CANCELLED,
                NYql::TIssuesIds::KIKIMR_OPERATION_CANCELLED,
                TStringBuilder() << "Operation cancelled.",
                getIssues());
            return;
        }
        case NKikimrDataEvents::TEvWriteResult::STATUS_BAD_REQUEST: {
            CA_LOG_E("Got BAD REQUEST for table `"
                    << TablePath << "`."
                    << " ShardID=" << ev->Get()->Record.GetOrigin() << ","
                    << " Sink=" << this->SelfId() << "."
                    << getIssues().ToOneLineString());
            TxManager->SetError(ev->Get()->Record.GetOrigin());
            RuntimeError(
                NYql::NDqProto::StatusIds::BAD_REQUEST,
                NYql::TIssuesIds::KIKIMR_BAD_REQUEST,
                TStringBuilder() << "Bad request. Table: `"
                    << TablePath << "`.",
                getIssues());
            return;
        }
        case NKikimrDataEvents::TEvWriteResult::STATUS_SCHEME_CHANGED: {
            CA_LOG_E("Got SCHEME CHANGED for table `"
                    << TablePath << "`."
                    << " ShardID=" << ev->Get()->Record.GetOrigin() << ","
                    << " Sink=" << this->SelfId() << "."
                    << getIssues().ToOneLineString());
            if (InconsistentTx) {
                ResetShardRetries(ev->Get()->Record.GetOrigin(), ev->Cookie);
                RetryResolve();
            } else {
                TxManager->SetError(ev->Get()->Record.GetOrigin());
                RuntimeError(
                    NYql::NDqProto::StatusIds::SCHEME_ERROR,
                    NYql::TIssuesIds::KIKIMR_SCHEME_MISMATCH,
                    TStringBuilder() << "Scheme changed. Table: `"
                        << TablePath << "`.",
                    getIssues());
            }
            return;
        }
        case NKikimrDataEvents::TEvWriteResult::STATUS_LOCKS_BROKEN: {
            CA_LOG_E("Got LOCKS BROKEN for table `"
                    << TablePath << "`."
                    << " ShardID=" << ev->Get()->Record.GetOrigin() << ","
                    << " Sink=" << this->SelfId() << "."
                    << getIssues().ToOneLineString());

            TxManager->BreakLock(ev->Get()->Record.GetOrigin());
            YQL_ENSURE(TxManager->BrokenLocks());
            TxManager->SetError(ev->Get()->Record.GetOrigin());
            RuntimeError(
                NYql::NDqProto::StatusIds::ABORTED,
                NYql::TIssuesIds::KIKIMR_LOCKS_INVALIDATED,
                TStringBuilder() << "Transaction locks invalidated. Table: `"
                    << TablePath << "`.",
                getIssues());
            return;
        }
        case NKikimrDataEvents::TEvWriteResult::STATUS_CONSTRAINT_VIOLATION: {
            CA_LOG_E("Got CONSTRAINT VIOLATION for table `" << TablePath << "`."
                    << " ShardID=" << ev->Get()->Record.GetOrigin() << ","
                    << " Sink=" << this->SelfId() << "."
                    << getIssues().ToOneLineString());
            TxManager->SetError(ev->Get()->Record.GetOrigin());
            RuntimeError(
                NYql::NDqProto::StatusIds::PRECONDITION_FAILED,
                NYql::TIssuesIds::KIKIMR_CONSTRAINT_VIOLATION,
                TStringBuilder() << "Constraint violated. Table: `"
                    << TablePath << "`.",
                getIssues());
            return;
        }
        }
    }

    void ProcessWritePreparedShard(NKikimr::NEvents::TDataEvents::TEvWriteResult::TPtr& ev) {
        YQL_ENSURE(Mode == EMode::PREPARE);
        const auto& record = ev->Get()->Record;
        AFL_ENSURE(record.GetTxLocks().empty());

        IKqpTransactionManager::TPrepareResult preparedInfo;
        preparedInfo.ShardId = record.GetOrigin();
        preparedInfo.MinStep = record.GetMinStep();
        preparedInfo.MaxStep = record.GetMaxStep();

        preparedInfo.Coordinator = 0;
        if (record.DomainCoordinatorsSize()) {
            auto domainCoordinators = TCoordinators(TVector<ui64>(record.GetDomainCoordinators().begin(),
                                                                  record.GetDomainCoordinators().end()));
            preparedInfo.Coordinator = domainCoordinators.Select(*TxId);
        }

        OnMessageReceived(ev->Get()->Record.GetOrigin());
        const auto result = ShardedWriteController->OnMessageAcknowledged(
                ev->Get()->Record.GetOrigin(), ev->Cookie);
        if (result) {
            YQL_ENSURE(result->IsShardEmpty);
            Callbacks->OnPrepared(std::move(preparedInfo), result->DataSize);
        }
    }

    void ProcessWriteCompletedShard(NKikimr::NEvents::TDataEvents::TEvWriteResult::TPtr& ev) {
        CA_LOG_D("Got completed result TxId=" << ev->Get()->Record.GetTxId()
            << ", TabletId=" << ev->Get()->Record.GetOrigin()
            << ", Cookie=" << ev->Cookie
            << ", Mode=" << static_cast<int>(Mode)
            << ", Locks=" << [&]() {
                TStringBuilder builder;
                for (const auto& lock : ev->Get()->Record.GetTxLocks()) {
                    builder << lock.ShortDebugString();
                }
                return builder;
            }());

        if (Mode == EMode::WRITE) {
            for (const auto& lock : ev->Get()->Record.GetTxLocks()) {
                if (!TxManager->AddLock(ev->Get()->Record.GetOrigin(), lock)) {
                    YQL_ENSURE(TxManager->BrokenLocks());
                    NYql::TIssues issues;
                    issues.AddIssue(*TxManager->GetLockIssue());
                    RuntimeError(
                        NYql::NDqProto::StatusIds::ABORTED,
                        std::move(issues));
                    return;
                }
            }
        }

        if (Mode == EMode::COMMIT) {
            Callbacks->OnCommitted(ev->Get()->Record.GetOrigin(), 0);
            return;
        }

        OnMessageReceived(ev->Get()->Record.GetOrigin());
        const auto result = ShardedWriteController->OnMessageAcknowledged(
                ev->Get()->Record.GetOrigin(), ev->Cookie);
        if (result && result->IsShardEmpty && Mode == EMode::IMMEDIATE_COMMIT) {
            Callbacks->OnCommitted(ev->Get()->Record.GetOrigin(), result->DataSize);
        } else if (result) {
            AFL_ENSURE(Mode == EMode::WRITE);
            Callbacks->OnMessageAcknowledged(result->DataSize);
        }
    }

    void OnMessageReceived(const ui64 shardId) {
        if (auto it = SendTime.find(shardId); it != std::end(SendTime)) {
            Counters->WriteActorWritesLatencyHistogram->Collect((TInstant::Now() - it->second).MicroSeconds());
            SendTime.erase(it);
        }
    }

    void SetPrepare(ui64 txId) {
        CA_LOG_D("SetPrepare; txId=" << txId);
        YQL_ENSURE(Mode == EMode::WRITE);
        Mode = EMode::PREPARE;
        TxId = txId;
        ShardedWriteController->AddCoveringMessages();
    }

    void SetDistributedCommit() {
        CA_LOG_D("SetDistributedCommit; txId=" << *TxId);
        YQL_ENSURE(Mode == EMode::PREPARE);
        Mode = EMode::COMMIT;
    }

    void SetImmediateCommit() {
        CA_LOG_D("SetImmediateCommit");
        YQL_ENSURE(Mode == EMode::WRITE);
        Mode = EMode::IMMEDIATE_COMMIT;

        if (ShardedWriteController->GetShardsCount() == 1) {
            ShardedWriteController->AddCoveringMessages();
        } else {
            YQL_ENSURE(ShardedWriteController->GetShardsCount() == 0);
        }
    }

    void UpdateShards() {
        for (const auto& shardInfo : ShardedWriteController->ExtractShardUpdates()) {
            TxManager->AddShard(shardInfo.ShardId, IsOlap, TablePath);
            IKqpTransactionManager::TActionFlags flags = IKqpTransactionManager::EAction::WRITE;
            if (shardInfo.HasRead) {
                flags |= IKqpTransactionManager::EAction::READ;
            }
            TxManager->AddAction(shardInfo.ShardId, flags);
        }
    }

    void FlushBuffers() {
        ShardedWriteController->FlushBuffers();
        UpdateShards();
    }

    bool FlushToShards() {
        bool ok = true;
        ShardedWriteController->ForEachPendingShard([&](const auto& shardInfo) {
            if (ok && !SendDataToShard(shardInfo.ShardId)) {
                ok = false;
            }
        });
        return ok;
    }

    bool SendDataToShard(const ui64 shardId) {
        YQL_ENSURE(Mode != EMode::COMMIT);

        const auto metadata = ShardedWriteController->GetMessageMetadata(shardId);
        YQL_ENSURE(metadata);
        if (metadata->SendAttempts >= MessageSettings.MaxWriteAttempts) {
            CA_LOG_W("ShardId=" << shardId
                    << " for table '" << TablePath
                    << "': retry limit exceeded."
                    << " Sink=" << this->SelfId() << ".");
            RetryResolve();
            return false;
        }

        const bool isPrepare = metadata->IsFinal && Mode == EMode::PREPARE;
        const bool isImmediateCommit = metadata->IsFinal && Mode == EMode::IMMEDIATE_COMMIT;

        auto evWrite = std::make_unique<NKikimr::NEvents::TDataEvents::TEvWrite>();

        evWrite->Record.SetTxMode(isPrepare
            ? (TxManager->IsVolatile()
                ? NKikimrDataEvents::TEvWrite::MODE_VOLATILE_PREPARE
                : NKikimrDataEvents::TEvWrite::MODE_PREPARE)
            : NKikimrDataEvents::TEvWrite::MODE_IMMEDIATE);

        if (isImmediateCommit) {
            const auto locks = TxManager->GetLocks(shardId);
            if (!locks.empty()) {
                auto* protoLocks = evWrite->Record.MutableLocks();
                protoLocks->SetOp(NKikimrDataEvents::TKqpLocks::Commit);
                protoLocks->AddSendingShards(shardId);
                protoLocks->AddReceivingShards(shardId);
                for (const auto& lock : locks) {
                    *protoLocks->AddLocks() = lock;
                }
            }
        } else if (isPrepare) {
            YQL_ENSURE(TxId);
            FillEvWritePrepare(evWrite.get(), shardId, *TxId, TxManager);
        } else if (!InconsistentTx) {
            evWrite->SetLockId(LockTxId, LockNodeId);
            evWrite->Record.SetLockMode(LockMode);

            if (LockMode == NKikimrDataEvents::OPTIMISTIC_SNAPSHOT_ISOLATION) {
                YQL_ENSURE(MvccSnapshot);
            }

            if (MvccSnapshot) {
                *evWrite->Record.MutableMvccSnapshot() = *MvccSnapshot;
            }
        }

        const auto serializationResult = ShardedWriteController->SerializeMessageToPayload(shardId, *evWrite);
        YQL_ENSURE(isPrepare || isImmediateCommit || serializationResult.TotalDataSize > 0);

        if (metadata->SendAttempts == 0) {
            if (!isPrepare) {
                Counters->WriteActorImmediateWrites->Inc();
            } else {
                Counters->WriteActorPrepareWrites->Inc();
            }
            Counters->WriteActorWritesSizeHistogram->Collect(serializationResult.TotalDataSize);
            Counters->WriteActorWritesOperationsHistogram->Collect(metadata->OperationsCount);

            for (const auto& operation : evWrite->Record.GetOperations()) {
                if (operation.GetType() == NKikimrDataEvents::TEvWrite::TOperation::OPERATION_INSERT
                       || operation.GetType() == NKikimrDataEvents::TEvWrite::TOperation::OPERATION_UPDATE) {
                    Counters->WriteActorReadWriteOperations->Inc();
                } else {
                    Counters->WriteActorWriteOnlyOperations->Inc();
                }
            }

            SendTime[shardId] = TInstant::Now();
        } else {
            YQL_ENSURE(!isPrepare);
            Counters->WriteActorImmediateWritesRetries->Inc();
        }

        if (isPrepare && MvccSnapshot) {
            bool needMvccSnapshot = false;
            for (const auto& operation : evWrite->Record.GetOperations()) {
                if (operation.GetType() == NKikimrDataEvents::TEvWrite::TOperation::OPERATION_INSERT) {
                    // This operation may fail with an incorrect unique constraint violation otherwise
                    needMvccSnapshot = true;
                    break;
                }
            }
            if (needMvccSnapshot) {
                *evWrite->Record.MutableMvccSnapshot() = *MvccSnapshot;
            }
        }

        NDataIntegrity::LogIntegrityTrails("EvWriteTx", evWrite->Record.GetTxId(), shardId, TlsActivationContext->AsActorContext(), "WriteActor");

        CA_LOG_D("Send EvWrite to ShardID=" << shardId << ", isPrepare=" << isPrepare << ", isImmediateCommit=" << isImmediateCommit << ", TxId=" << evWrite->Record.GetTxId()
            << ", LockTxId=" << evWrite->Record.GetLockTxId() << ", LockNodeId=" << evWrite->Record.GetLockNodeId()
            << ", Locks= " << [&]() {
                TStringBuilder builder;
                for (const auto& lock : evWrite->Record.GetLocks().GetLocks()) {
                    builder << lock.ShortDebugString();
                }
                return builder;
            }()
            << ", Size=" << serializationResult.TotalDataSize << ", Cookie=" << metadata->Cookie
            << ", OperationsCount=" << evWrite->Record.OperationsSize() << ", IsFinal=" << metadata->IsFinal
            << ", Attempts=" << metadata->SendAttempts << ", Mode=" << static_cast<int>(Mode)
            << ", BufferMemory=" << GetMemory());

        AFL_ENSURE(Mode == EMode::WRITE || metadata->IsFinal);

        LinkedPipeCache = true;
        Send(
            PipeCacheId,
            new TEvPipeCache::TEvForward(evWrite.release(), shardId, /* subscribe */ true),
            0,
            metadata->Cookie,
            NWilson::TTraceId(ParentTraceId));

        ShardedWriteController->OnMessageSent(shardId, metadata->Cookie);

        if (InconsistentTx) {
            TlsActivationContext->Schedule(
                CalculateNextAttemptDelay(MessageSettings, metadata->SendAttempts),
                new IEventHandle(
                    SelfId(),
                    SelfId(),
                    new TEvPrivate::TEvShardRequestTimeout(shardId),
                    0,
                    metadata->Cookie));
        }

        return true;
    }

    void RetryShard(const ui64 shardId, const std::optional<ui64> ifCookieEqual) {
        const auto metadata = ShardedWriteController->GetMessageMetadata(shardId);
        if (!metadata || (ifCookieEqual && metadata->Cookie != ifCookieEqual)) {
            CA_LOG_I("Retry failed: not found ShardID=" << shardId << " with Cookie=" << ifCookieEqual.value_or(0));
            return;
        }

        CA_LOG_D("Retry ShardID=" << shardId
            << ", Cookie=" << ifCookieEqual.value_or(0)
            << ", Attempt=" << metadata->SendAttempts
            << ", Next Delay=" << CalculateNextAttemptDelay(MessageSettings, metadata->SendAttempts));
        SendDataToShard(shardId);
    }

    void ResetShardRetries(const ui64 shardId, const ui64 cookie) {
        ShardedWriteController->ResetRetries(shardId, cookie);
    }

    void Handle(TEvPrivate::TEvShardRequestTimeout::TPtr& ev) {
        CA_LOG_I("Timeout shardID=" << ev->Get()->ShardId);
        YQL_ENSURE(InconsistentTx);
        RetryShard(ev->Get()->ShardId, ev->Cookie);
    }

    void Handle(TEvPipeCache::TEvDeliveryProblem::TPtr& ev) {
        CA_LOG_W("TEvDeliveryProblem was received from tablet: " << ev->Get()->TabletId);
        if (InconsistentTx) {
            RetryShard(ev->Get()->TabletId, std::nullopt);
            return;
        }

        const auto state = TxManager->GetState(ev->Get()->TabletId);
        if ((state == IKqpTransactionManager::PREPARED
                    || state == IKqpTransactionManager::EXECUTING)
                && TxManager->ShouldReattach(ev->Get()->TabletId, TlsActivationContext->Now())) {
            // Disconnected while waiting for other shards to prepare
            auto& reattachState = TxManager->GetReattachState(ev->Get()->TabletId);
            CA_LOG_N("Shard " << ev->Get()->TabletId << " delivery problem (reattaching in "
                        << reattachState.ReattachInfo.Delay << ")");

            Schedule(reattachState.ReattachInfo.Delay, new TEvPrivate::TEvReattachToShard(ev->Get()->TabletId));
        } else if (state == IKqpTransactionManager::EXECUTING) {
            TxManager->SetError(ev->Get()->TabletId);
            RuntimeError(
                NYql::NDqProto::StatusIds::UNDETERMINED,
                NYql::TIssuesIds::KIKIMR_OPERATION_STATE_UNKNOWN,
                TStringBuilder()
                    << "State of operation is unknown. "
                    << "Error writing to table `" << TablePath << "`"
                    << ". Transaction state unknown for tablet " << ev->Get()->TabletId << ".");
            return;
        } else if (state == IKqpTransactionManager::PROCESSING
                || state == IKqpTransactionManager::PREPARING
                || state == IKqpTransactionManager::PREPARED) {
            TxManager->SetError(ev->Get()->TabletId);
            RuntimeError(
                NYql::NDqProto::StatusIds::UNAVAILABLE,
                NYql::TIssuesIds::KIKIMR_TEMPORARILY_UNAVAILABLE,
                TStringBuilder()
                    << "Kikimr cluster or one of its subsystems was unavailable. "
                    << "Error writing to table `" << TablePath << "`"
                    << ": can't deliver message to tablet " << ev->Get()->TabletId << ".");
            return;
        } else {
            AFL_ENSURE(state == IKqpTransactionManager::FINISHED || state == IKqpTransactionManager::ERROR);
        }
    }

    void Handle(TEvDataShard::TEvProposeTransactionAttachResult::TPtr& ev) {
        const auto& record = ev->Get()->Record;
        const ui64 shardId = record.GetTabletId();

        auto& reattachState = TxManager->GetReattachState(shardId);
        if (reattachState.Cookie != ev->Cookie) {
            return;
        }

        const auto shardState = TxManager->GetState(shardId);
        switch (shardState) {
            case IKqpTransactionManager::EXECUTING:
                YQL_ENSURE(Mode == EMode::COMMIT || Mode == EMode::IMMEDIATE_COMMIT);
                break;
            case IKqpTransactionManager::PREPARED:
                YQL_ENSURE(Mode == EMode::PREPARE);
                break;
            case IKqpTransactionManager::PREPARING:
            case IKqpTransactionManager::FINISHED:
            case IKqpTransactionManager::ERROR:
            case IKqpTransactionManager::PROCESSING:
                YQL_ENSURE(false);
        }

        if (record.GetStatus() == NKikimrProto::OK) {
            // Transaction still exists at this shard
            CA_LOG_D("Reattached to shard " << shardId);
            TxManager->Reattached(shardId);
            return;
        }

        if (Mode == EMode::PREPARE) {
            RuntimeError(
                NYql::NDqProto::StatusIds::UNAVAILABLE,
                NYql::TIssuesIds::KIKIMR_TEMPORARILY_UNAVAILABLE,
                TStringBuilder()
                    << "ShardId=" << shardId
                    << " for table '" << TablePath
                    << "': attach transaction failed.");
            return;
        } else {
            RuntimeError(
                NYql::NDqProto::StatusIds::UNDETERMINED,
                NYql::TIssuesIds::KIKIMR_OPERATION_STATE_UNKNOWN,
                TStringBuilder()
                    << "ShardId=" << shardId
                    << " for table '" << TablePath
                    << "': attach transaction failed.");
            return;
        }
    }

    void Handle(TEvDataShard::TEvProposeTransactionRestart::TPtr& ev) {
        const auto& record = ev->Get()->Record;
        const ui64 shardId = record.GetTabletId();

        CA_LOG_D("Got transaction restart event from tabletId: " << shardId);

        switch (TxManager->GetState(shardId)) {
            case IKqpTransactionManager::EXECUTING: {
                TxManager->SetRestarting(shardId);
                return;
            }
            case IKqpTransactionManager::FINISHED:
            case IKqpTransactionManager::ERROR: {
                return;
            }
            case IKqpTransactionManager::PREPARING:
            case IKqpTransactionManager::PREPARED:
            case IKqpTransactionManager::PROCESSING: {
                YQL_ENSURE(false);
            }
        }
    }

    void Handle(TEvPrivate::TEvReattachToShard::TPtr& ev) {
        const ui64 tabletId = ev->Get()->TabletId;
        auto& state = TxManager->GetReattachState(tabletId);

        CA_LOG_D("Reattach to shard " << tabletId);

        YQL_ENSURE(TxId);
        Send(MakePipePerNodeCacheID(false), new TEvPipeCache::TEvForward(
            new TEvDataShard::TEvProposeTransactionAttach(tabletId, *TxId),
            tabletId, /* subscribe */ true), 0, ++state.Cookie);
    }

    void Prepare() {
        if (TableWriteActorSpan) {
            TableWriteActorSpan.EndOk(); // Resolve finished
        }

        ResolveAttempts = 0;

        if (IsOlap) {
            YQL_ENSURE(SchemeEntry);
            ShardedWriteController->OnPartitioningChanged(*SchemeEntry);
        } else {
            ShardedWriteController->OnPartitioningChanged(Partitioning);
            Partitioning.reset();
        }

        if (InconsistentTx && Closed) {
            FlushBuffers();
            YQL_ENSURE(ShardedWriteController);
            YQL_ENSURE(ShardedWriteController->IsAllWritesClosed());
            ShardedWriteController->Close();
        }

        Callbacks->OnReady();
    }

    void RuntimeError(NYql::NDqProto::StatusIds::StatusCode statusCode, NYql::EYqlIssueCode id, const TString& message, const NYql::TIssues& subIssues = {}) {
        if (TableWriteActorSpan) {
            TableWriteActorSpan.EndError(message);
        }

        Callbacks->OnError(statusCode, id, message, subIssues);
    }

    void RuntimeError(NYql::NDqProto::StatusIds::StatusCode statusCode, NYql::TIssues&& issues) {
        if (TableWriteActorSpan) {
            TableWriteActorSpan.EndError(issues.ToOneLineString());
        }

        Callbacks->OnError(statusCode, std::move(issues));
    }

    void Unlink() {
        if (LinkedPipeCache) {
            Send(PipeCacheId, new TEvPipeCache::TEvUnlink(0));
            LinkedPipeCache = false;
        }
    }

    void PassAway() override {
        {
            Y_ABORT_UNLESS(Alloc);
            TGuard<NMiniKQL::TScopedAlloc> allocGuard(*Alloc);
            ShardedWriteController.Reset();
        }
        Counters->WriteActorsCount->Dec();
        Unlink();
        TActorBootstrapped<TKqpTableWriteActor>::PassAway();
    }

    void Terminate() {
        PassAway();
    }

    void UpdateStats(const NKikimrQueryStats::TTxStats& txStats) {
        Stats.UpdateStats(txStats, TableId);
    }

    void FillStats(NYql::NDqProto::TDqTaskStats* stats) {
        Stats.FillStats(stats, TablePath);
    }

    bool FlushBeforeCommit() const {
        return NeedToFlushBeforeCommit;
    }

private:
    NActors::TActorId PipeCacheId = NKikimr::MakePipePerNodeCacheID(false);
    bool LinkedPipeCache = false;

    TString LogPrefix;
    TWriteActorSettings MessageSettings;
    std::shared_ptr<NKikimr::NMiniKQL::TScopedAlloc> Alloc;

    const std::optional<NKikimrDataEvents::TMvccSnapshot> MvccSnapshot;
    const NKikimrDataEvents::ELockMode LockMode;

    const TTableId TableId;
    const TString TablePath;

    std::optional<ui64> TxId;
    const ui64 LockTxId;
    const ui64 LockNodeId;
    const bool InconsistentTx;
    const bool IsOlap;
    const TVector<NScheme::TTypeInfo> KeyColumnTypes;

    IKqpTableWriterCallbacks* Callbacks;

    std::optional<NSchemeCache::TSchemeCacheNavigate::TEntry> SchemeEntry;
    std::shared_ptr<const TVector<TKeyDesc::TPartitionInfo>> Partitioning;
    ui64 ResolveAttempts = 0;

    IKqpTransactionManagerPtr TxManager;
    bool Closed = false;
    bool NeedToFlushBeforeCommit = false;
    EMode Mode = EMode::WRITE;
    THashMap<ui64, TInstant> SendTime;

    IShardedWriteControllerPtr ShardedWriteController = nullptr;

    TIntrusivePtr<TKqpCounters> Counters;

    TKqpTableWriterStatistics Stats;

    NWilson::TTraceId ParentTraceId;
    NWilson::TSpan TableWriteActorSpan;
};

class TKqpDirectWriteActor : public TActorBootstrapped<TKqpDirectWriteActor>, public NYql::NDq::IDqComputeActorAsyncOutput, public IKqpTableWriterCallbacks {
    using TBase = TActorBootstrapped<TKqpDirectWriteActor>;

public:
    TKqpDirectWriteActor(
        NKikimrKqp::TKqpTableSinkSettings&& settings,
        NYql::NDq::TDqAsyncIoFactory::TSinkArguments&& args,
        TIntrusivePtr<TKqpCounters> counters)
        : LogPrefix(TStringBuilder() << "TxId: " << args.TxId << ", task: " << args.TaskId << ". ")
        , Settings(std::move(settings))
        , MessageSettings(GetWriteActorSettings())
        , OutputIndex(args.OutputIndex)
        , Callbacks(args.Callback)
        , Counters(counters)
        , Alloc(args.Alloc)
        , TxId(std::get<ui64>(args.TxId))
        , TableId(
            Settings.GetTable().GetOwnerId(),
            Settings.GetTable().GetTableId(),
            Settings.GetTable().GetVersion())
        , DirectWriteActorSpan(TWilsonKqp::DirectWriteActor, NWilson::TTraceId(args.TraceId), "TKqpDirectWriteActor")
    {
        EgressStats.Level = args.StatsLevel;

        TVector<NKikimrKqp::TKqpColumnMetadataProto> columnsMetadata(
            Settings.GetColumns().begin(),
            Settings.GetColumns().end());
        std::vector<ui32> writeIndex(
            Settings.GetWriteIndexes().begin(),
            Settings.GetWriteIndexes().end());

        TGuard guard(*Alloc);
        if (Settings.GetIsOlap()) {
            Batcher = CreateColumnDataBatcher(columnsMetadata, std::move(writeIndex), Alloc);
        } else {
            Batcher = CreateRowDataBatcher(columnsMetadata, std::move(writeIndex), Alloc);
        }
    }

    void Bootstrap() {
        LogPrefix = TStringBuilder() << "SelfId: " << this->SelfId() << ", " << LogPrefix;

        try {
            TVector<NScheme::TTypeInfo> keyColumnTypes;
            keyColumnTypes.reserve(Settings.GetKeyColumns().size());
            for (const auto& column : Settings.GetKeyColumns()) {
                auto typeInfoMod = NScheme::TypeInfoModFromProtoColumnType(column.GetTypeId(),
                    column.HasTypeInfo() ? &column.GetTypeInfo() : nullptr);
                keyColumnTypes.push_back(typeInfoMod.TypeInfo);
            }

            WriteTableActor = new TKqpTableWriteActor(
                this,
                TableId,
                Settings.GetTable().GetPath(),
                Settings.GetLockTxId(),
                Settings.GetLockNodeId(),
                Settings.GetInconsistentTx(),
                Settings.GetIsOlap(),
                std::move(keyColumnTypes),
                Alloc,
                GetOptionalMvccSnapshot(Settings),
                Settings.GetLockMode(),
                nullptr,
                TActorId{},
                Counters);
            WriteTableActor->SetParentTraceId(DirectWriteActorSpan.GetTraceId());
            WriteTableActorId = RegisterWithSameMailbox(WriteTableActor);

            TVector<NKikimrKqp::TKqpColumnMetadataProto> keyColumnsMetadata(
                Settings.GetKeyColumns().begin(),
                Settings.GetKeyColumns().end());
            TVector<NKikimrKqp::TKqpColumnMetadataProto> columnsMetadata(
                Settings.GetColumns().begin(),
                Settings.GetColumns().end());
            std::vector<ui32> writeIndex(
                Settings.GetWriteIndexes().begin(),
                Settings.GetWriteIndexes().end());
            YQL_ENSURE(Settings.GetPriority() == 0);
            WriteTableActor->Open(
                WriteToken,
                GetOperation(Settings.GetType()),
                std::move(keyColumnsMetadata),
                std::move(columnsMetadata),
                std::move(writeIndex),
                Settings.GetPriority());
            WaitingForTableActor = true;
        } catch (const TMemoryLimitExceededException&) {
            RuntimeError(
                NYql::NDqProto::StatusIds::PRECONDITION_FAILED,
                NYql::TIssuesIds::KIKIMR_PRECONDITION_FAILED,
                TStringBuilder() << "Memory limit exception"
                    << ", current limit is " << Alloc->GetLimit() << " bytes.",
                {});
            return;
        } catch (...) {
            RuntimeError(
                NYql::NDqProto::StatusIds::INTERNAL_ERROR,
                NYql::TIssuesIds::KIKIMR_INTERNAL_ERROR,
                CurrentExceptionMessage(),
                {});
            return;
        }
    }

    static constexpr char ActorName[] = "KQP_DIRECT_WRITE_ACTOR";

private:
    virtual ~TKqpDirectWriteActor() {
    }

    void CommitState(const NYql::NDqProto::TCheckpoint&) final {};
    void LoadState(const NYql::NDq::TSinkState&) final {};

    ui64 GetOutputIndex() const final {
        return OutputIndex;
    }

    const NYql::NDq::TDqAsyncStats& GetEgressStats() const final {
        return EgressStats;
    }

    i64 GetFreeSpace() const final {
        return (WriteTableActor && WriteTableActor->IsReady())
            ? MessageSettings.InFlightMemoryLimitPerActorBytes - GetMemory()
            : std::numeric_limits<i64>::min(); // Can't use zero here because compute can use overcommit!
    }

    i64 GetMemory() const {
        return (WriteTableActor && WriteTableActor->IsReady())
            ? WriteTableActor->GetMemory()
            : 0;
    }

    TMaybe<google::protobuf::Any> ExtraData() override {
        if (!WriteTableActor) {
            return {};
        }
        NKikimrKqp::TEvKqpOutputActorResultInfo resultInfo;
        for (const auto& lock : WriteTableActor->GetLocks()) {
            resultInfo.AddLocks()->CopyFrom(lock);
        }
        resultInfo.SetHasRead(
            GetOperation(Settings.GetType()) == NKikimrDataEvents::TEvWrite::TOperation::OPERATION_INSERT ||
            GetOperation(Settings.GetType()) == NKikimrDataEvents::TEvWrite::TOperation::OPERATION_UPDATE);
        google::protobuf::Any result;
        result.PackFrom(resultInfo);
        return result;
    }

    void SendData(NMiniKQL::TUnboxedValueBatch&& data, i64 size, const TMaybe<NYql::NDqProto::TCheckpoint>&, bool finished) final {
        YQL_ENSURE(!data.IsWide(), "Wide stream is not supported yet");
        YQL_ENSURE(!Closed);
        Closed = finished;
        EgressStats.Resume();
        Y_UNUSED(size);

        try {
            Batcher->AddData(data);
            YQL_ENSURE(WriteTableActor);
            WriteTableActor->Write(WriteToken, Batcher->Build());
            if (Closed) {
                WriteTableActor->Close(WriteToken);
                WriteTableActor->FlushBuffers();
                WriteTableActor->Close();
            }
        } catch (const TMemoryLimitExceededException&) {
            RuntimeError(
                NYql::NDqProto::StatusIds::PRECONDITION_FAILED,
                NYql::TIssuesIds::KIKIMR_PRECONDITION_FAILED,
                TStringBuilder() << "Memory limit exception"
                    << ", current limit is " << Alloc->GetLimit() << " bytes.",
                {});
            return;
        } catch (...) {
            RuntimeError(
                NYql::NDqProto::StatusIds::INTERNAL_ERROR,
                NYql::TIssuesIds::KIKIMR_INTERNAL_ERROR,
                CurrentExceptionMessage(),
                {});
            return;
        }

        Process();
    }

    void Process() {
        try {
            const bool outOfMemory = GetFreeSpace() <= 0;
            if (outOfMemory) {
                WaitingForTableActor = true;
            } else if (WaitingForTableActor) {
                ResumeExecution();
            }

            if (outOfMemory && !Settings.GetEnableStreamWrite()) {
                RuntimeError(
                    NYql::NDqProto::StatusIds::PRECONDITION_FAILED,
                    NYql::TIssuesIds::KIKIMR_PRECONDITION_FAILED,
                    TStringBuilder() << "Out of buffer memory. Used " << GetMemory()
                        << " bytes of " << MessageSettings.InFlightMemoryLimitPerActorBytes << " bytes.",
                    {});
                return;
            }

            if (!Closed && outOfMemory) {
                WriteTableActor->FlushBuffers();
            }

            if (Closed || outOfMemory) {
                if (!WriteTableActor->FlushToShards()) {
                    return;
                }
            }

            if (Closed && WriteTableActor->IsFinished()) {
                CA_LOG_D("Write actor finished");
                Callbacks->OnAsyncOutputFinished(GetOutputIndex());
            }
        } catch (const TMemoryLimitExceededException&) {
            RuntimeError(
                NYql::NDqProto::StatusIds::PRECONDITION_FAILED,
                NYql::TIssuesIds::KIKIMR_PRECONDITION_FAILED,
                TStringBuilder() << "Memory limit exception"
                    << ", current limit is " << Alloc->GetLimit() << " bytes.",
                {});
            return;
        } catch (...) {
            RuntimeError(
                NYql::NDqProto::StatusIds::INTERNAL_ERROR,
                NYql::TIssuesIds::KIKIMR_INTERNAL_ERROR,
                CurrentExceptionMessage(),
                {});
            return;
        }
    }

    void RuntimeError(NYql::NDqProto::StatusIds::StatusCode statusCode, NYql::EYqlIssueCode id, const TString& message, const NYql::TIssues& subIssues) {
        DirectWriteActorSpan.EndError(message);

        NYql::TIssue issue(message);
        SetIssueCode(id, issue);
        for (const auto& i : subIssues) {
            issue.AddSubIssue(MakeIntrusive<NYql::TIssue>(i));
        }

        NYql::TIssues issues;
        issues.AddIssue(std::move(issue));

        Callbacks->OnAsyncOutputError(OutputIndex, std::move(issues), statusCode);
    }

    void RuntimeError(NYql::NDqProto::StatusIds::StatusCode statusCode, NYql::TIssues&& issues) {
        DirectWriteActorSpan.EndError(issues.ToOneLineString());

        Callbacks->OnAsyncOutputError(OutputIndex, std::move(issues), statusCode);
    }

    void PassAway() override {
        if (WriteTableActor) {
            WriteTableActor->Terminate();
        }
        TActorBootstrapped<TKqpDirectWriteActor>::PassAway();
    }

    void ResumeExecution() {
        CA_LOG_D("Resuming execution.");
        WaitingForTableActor = false;
        Callbacks->ResumeExecution();
    }

    void OnReady() override {
        Process();
    }

    void OnPrepared(IKqpTransactionManager::TPrepareResult&&, ui64) override {
        AFL_ENSURE(false);
    }

    void OnCommitted(ui64, ui64) override {
        AFL_ENSURE(false);
    }

    void OnMessageAcknowledged(ui64 dataSize) override {
        EgressStats.Bytes += dataSize;
        EgressStats.Chunks++;
        EgressStats.Splits++;
        EgressStats.Resume();
        Process();
    }

    void OnError(NYql::NDqProto::StatusIds::StatusCode statusCode, NYql::EYqlIssueCode id, const TString& message, const NYql::TIssues& subIssues) override {
        RuntimeError(statusCode, id, message, subIssues);
    }

    void OnError(NYql::NDqProto::StatusIds::StatusCode statusCode, NYql::TIssues&& issues) override {
        RuntimeError(statusCode, std::move(issues));
    }

    void FillExtraStats(NYql::NDqProto::TDqTaskStats* stats, bool last, const NYql::NDq::TDqMeteringStats*) override {
        if (last && WriteTableActor) {
            WriteTableActor->FillStats(stats);
        }
    }

    TString LogPrefix;
    const NKikimrKqp::TKqpTableSinkSettings Settings;
    TWriteActorSettings MessageSettings;
    const ui64 OutputIndex;
    NYql::NDq::TDqAsyncStats EgressStats;
    NYql::NDq::IDqComputeActorAsyncOutput::ICallbacks * Callbacks = nullptr;
    TIntrusivePtr<TKqpCounters> Counters;
    std::shared_ptr<NKikimr::NMiniKQL::TScopedAlloc> Alloc;
    IDataBatcherPtr Batcher;

    const ui64 TxId;
    const TTableId TableId;
    TKqpTableWriteActor* WriteTableActor = nullptr;
    TActorId WriteTableActorId;

    TKqpTableWriteActor::TWriteToken WriteToken = 0;

    bool Closed = false;
    bool WaitingForTableActor = false;

    NWilson::TSpan DirectWriteActorSpan;
};


namespace {

struct TWriteToken {
    TPathId PathId;
    ui64 Cookie;

    bool IsEmpty() const {
        return !PathId;
    }
};

struct TTransactionSettings {
    ui64 TxId = 0;
    ui64 LockTxId = 0;
    ui64 LockNodeId = 0;
    bool InconsistentTx = false;
    std::optional<NKikimrDataEvents::TMvccSnapshot> MvccSnapshot;
    NKikimrDataEvents::ELockMode LockMode;
};

struct TWriteSettings {
    TTableId TableId;
    TString TablePath; // for error messages
    NKikimrDataEvents::TEvWrite::TOperation::EOperationType OperationType;
    TVector<NKikimrKqp::TKqpColumnMetadataProto> KeyColumns;
    TVector<NKikimrKqp::TKqpColumnMetadataProto> Columns;
    std::vector<ui32> WriteIndex;
    TTransactionSettings TransactionSettings;
    i64 Priority;
    bool EnableStreamWrite;
    bool IsOlap;

    struct TIndex {
        TTableId TableId;
        TString TablePath;
        TVector<NKikimrKqp::TKqpColumnMetadataProto> KeyColumns;
        TVector<NKikimrKqp::TKqpColumnMetadataProto> Columns;
        std::vector<ui32> WriteIndex;
        bool IsUniq;
    };

    std::vector<TIndex> Indexes;
};

struct TBufferWriteMessage {
    TActorId From;
    TWriteToken Token;
    bool Close = false;
    IDataBatchPtr Data;
};

struct TEvBufferWrite : public TEventLocal<TEvBufferWrite, TKqpEvents::EvBufferWrite> {
    bool Close = false;
    std::optional<TWriteToken> Token;
    std::optional<TWriteSettings> Settings;
    IDataBatchPtr Data;

    TInstant SendTime;
};

struct TEvBufferWriteResult : public TEventLocal<TEvBufferWriteResult, TKqpEvents::EvBufferWriteResult> {
    TWriteToken Token;
};

}


class TKqpBufferWriteActor :public TActorBootstrapped<TKqpBufferWriteActor>, public IKqpTableWriterCallbacks {
    using TBase = TActorBootstrapped<TKqpBufferWriteActor>;
    using TTopicTabletTxs = NTopic::TTopicOperationTransactions;

public:
    enum class EState {
        WRITING, // Allow to write data to buffer.
        FLUSHING, // Force flush (for uncommitted changes visibility). Can't accept any writes in this state.
        PREPARING, // Do preparation for commit. All writers are closed. New writes wouldn't be accepted.
        COMMITTING, // Do commit. All writers are closed. New writes wouldn't be accepted.
        ROLLINGBACK, // Do rollback. New writes wouldn't be accepted.
        FINISHED,
    };

public:
    TKqpBufferWriteActor(
        TKqpBufferWriterSettings&& settings)
        : SessionActorId(settings.SessionActorId)
        , MessageSettings(GetWriteActorSettings())
        , TxManager(settings.TxManager)
        , Alloc(settings.Alloc)
        , Counters(settings.Counters)
        , TxProxyMon(settings.TxProxyMon)
        , BufferWriteActorSpan(TWilsonKqp::BufferWriteActor, NWilson::TTraceId(settings.TraceId), "BufferWriteActor", NWilson::EFlags::AUTO_END)
    {
        State = EState::WRITING;
        Counters->BufferActorsCount->Inc();
        UpdateTracingState("Write", BufferWriteActorSpan.GetTraceId());
    }

    void Bootstrap() {
        LogPrefix = TStringBuilder() << "SelfId: " << this->SelfId() << ", SessionActorId: " << SessionActorId << ", " << LogPrefix;
        Become(&TKqpBufferWriteActor::StateWrite);
    }

    static constexpr char ActorName[] = "KQP_BUFFER_WRITE_ACTOR";

    // TODO: split states
    STFUNC(StateWrite) {
        try {
            switch (ev->GetTypeRewrite()) {
                hFunc(TEvKqpBuffer::TEvTerminate, Handle);
                hFunc(TEvKqpBuffer::TEvFlush, Handle);
                hFunc(TEvKqpBuffer::TEvCommit, Handle);
                hFunc(TEvKqpBuffer::TEvRollback, Handle);
                hFunc(TEvBufferWrite, Handle);

                hFunc(TEvTxProxy::TEvProposeTransactionStatus, Handle);
                hFunc(TEvPersQueue::TEvProposeTransactionResult, Handle);
                hFunc(NKikimr::NEvents::TDataEvents::TEvWriteResult, Handle);
                hFunc(TEvPipeCache::TEvDeliveryProblem, Handle);
            default:
                AFL_ENSURE(false)("unknown message", ev->GetTypeRewrite());
            }
        } catch (const TMemoryLimitExceededException&) {
            ReplyErrorAndDie(
                NYql::NDqProto::StatusIds::PRECONDITION_FAILED,
                NYql::TIssuesIds::KIKIMR_PRECONDITION_FAILED,
                TStringBuilder() << "Memory limit exception"
                    << ", current limit is " << Alloc->GetLimit() << " bytes.",
                {});
            return;
        } catch (...) {
            ReplyErrorAndDie(
                NYql::NDqProto::StatusIds::INTERNAL_ERROR,
                NYql::TIssuesIds::KIKIMR_INTERNAL_ERROR,
                CurrentExceptionMessage(),
                {});
            return;
        }
    }

    void Handle(TEvBufferWrite::TPtr& ev) {
        Counters->ForwardActorWritesLatencyHistogram->Collect((TInstant::Now() - ev->Get()->SendTime).MicroSeconds());
        TWriteToken token;
        if (!ev->Get()->Token) {
            AFL_ENSURE(ev->Get()->Settings);
            auto& settings = *ev->Get()->Settings;
            if (!WriteInfos.empty()) {
                AFL_ENSURE(LockTxId == settings.TransactionSettings.LockTxId);
                AFL_ENSURE(LockNodeId == settings.TransactionSettings.LockNodeId);
                AFL_ENSURE(InconsistentTx == settings.TransactionSettings.InconsistentTx);
            } else {
                LockTxId = settings.TransactionSettings.LockTxId;
                LockNodeId = settings.TransactionSettings.LockNodeId;
                InconsistentTx = settings.TransactionSettings.InconsistentTx;
            }

            auto createWriteActor = [&](const TTableId tableId, const TString& tablePath, const TVector<NKikimrKqp::TKqpColumnMetadataProto>& keyColumns) -> std::pair<TKqpTableWriteActor*, TActorId> {
                TVector<NScheme::TTypeInfo> keyColumnTypes;
                keyColumnTypes.reserve(keyColumns.size());
                for (const auto& column : keyColumns) {
                    auto typeInfoMod = NScheme::TypeInfoModFromProtoColumnType(column.GetTypeId(),
                        column.HasTypeInfo() ? &column.GetTypeInfo() : nullptr);
                    keyColumnTypes.push_back(typeInfoMod.TypeInfo);
                }
                TKqpTableWriteActor* ptr = new TKqpTableWriteActor(
                    this,
                    tableId,
                    tablePath,
                    LockTxId,
                    LockNodeId,
                    InconsistentTx,
                    settings.IsOlap,
                    std::move(keyColumnTypes),
                    Alloc,
                    settings.TransactionSettings.MvccSnapshot,
                    settings.TransactionSettings.LockMode,
                    TxManager,
                    SessionActorId,
                    Counters);
                ptr->SetParentTraceId(BufferWriteActorStateSpan.GetTraceId());
                TActorId id = RegisterWithSameMailbox(ptr);
                CA_LOG_D("Create new TableWriteActor for table `" << tablePath << "` (" << tableId << "). lockId=" << LockTxId << ". ActorId=" << id);

                return {ptr, id};
            };

            auto& writeInfo = WriteInfos[settings.TableId.PathId];
            if (!writeInfo.Actors.contains(settings.TableId.PathId)) {
                AFL_ENSURE(writeInfo.Actors.empty());
                const auto [ptr, id] = createWriteActor(settings.TableId, settings.TablePath, settings.KeyColumns);
                writeInfo.Actors.emplace(settings.TableId.PathId, TWriteInfo::TActorInfo{
                    .WriteActor = ptr,
                    .Id = id,
                });
            }

            for (const auto& indexSettings : settings.Indexes) {
                if (!writeInfo.Actors.contains(indexSettings.TableId.PathId)) {
                    const auto [ptr, id] = createWriteActor(indexSettings.TableId, indexSettings.TablePath, indexSettings.KeyColumns);
                    writeInfo.Actors.emplace(indexSettings.TableId.PathId, TWriteInfo::TActorInfo{
                        .WriteActor = ptr,
                        .Id = id,
                    });
                }
            }

            if (writeInfo.Actors.at(settings.TableId.PathId).WriteActor->GetTableId().SchemaVersion != settings.TableId.SchemaVersion) {
                CA_LOG_E("Scheme changed for table `"
                    << settings.TablePath << "`.");
                ReplyErrorAndDie(
                    NYql::NDqProto::StatusIds::SCHEME_ERROR,
                    NYql::TIssuesIds::KIKIMR_SCHEME_MISMATCH,
                    TStringBuilder() << "Scheme changed. Table: `"
                        << settings.TablePath << "`.",
                    {});
                return;
            }
            AFL_ENSURE(writeInfo.Actors.at(settings.TableId.PathId).WriteActor->GetTableId() == settings.TableId);

            EnableStreamWrite &= settings.EnableStreamWrite;

            token = TWriteToken{settings.TableId.PathId, CurrentWriteToken++};

            AFL_ENSURE(writeInfo.Actors.size() > settings.Indexes.size());
            for (auto& indexSettings : settings.Indexes) {
                writeInfo.Actors.at(indexSettings.TableId.PathId).Projections.emplace(token.Cookie, CreateDataBatchProjection(
                    settings.Columns,
                    settings.WriteIndex,
                    indexSettings.Columns,
                    indexSettings.WriteIndex,
                    Alloc));

                writeInfo.Actors.at(indexSettings.TableId.PathId).WriteActor->Open(
                    token.Cookie,
                    NKikimrDataEvents::TEvWrite::TOperation::OPERATION_UPSERT, // TODO: Operation for index (delete by key + upsert)
                    std::move(indexSettings.KeyColumns),
                    std::move(indexSettings.Columns),
                    std::move(indexSettings.WriteIndex),
                    settings.Priority);
            }

            writeInfo.Actors.at(settings.TableId.PathId).WriteActor->Open(
                token.Cookie,
                settings.OperationType,
                std::move(settings.KeyColumns),
                std::move(settings.Columns),
                std::move(settings.WriteIndex),
                settings.Priority);
        } else {
            token = *ev->Get()->Token;
        }

        auto& queue = RequestQueues[token.PathId];
        queue.emplace();
        auto& message = queue.back();

        message.Token = token;
        message.From = ev->Sender;
        message.Close = ev->Get()->Close;
        message.Data = ev->Get()->Data;
        
        Process();
    }

    bool NeedToFlush() {
        const bool outOfMemory = GetTotalFreeSpace() <= 0;
        const bool needToFlush = outOfMemory
            || State == EState::FLUSHING
            || State == EState::PREPARING
            || State == EState::COMMITTING
            || State == EState::ROLLINGBACK;
        return needToFlush;
    }

    bool NeedToFlushActor(const TKqpTableWriteActor* actor) {
        return NeedToFlush()
            && (State != EState::FLUSHING
                || !TxId // Flush between queries
                || actor->FlushBeforeCommit()); // Flush before commit
    }

    bool Process() {
        ProcessRequestQueue();
        if (!ProcessFlush()) {
            return false;
        }
        ProcessAckQueue();

        if (State == EState::FLUSHING) {
            bool isEmpty = true;
            ForEachWriteActor([&](const TKqpTableWriteActor* actor, const TActorId) {
                if (NeedToFlushActor(actor)) {
                    isEmpty &= actor->IsReady() && actor->IsEmpty();
                }
            });
            if (isEmpty) {
                OnFlushed();
            }
        }
        return true;
    }

    void ProcessRequestQueue() {
        for (auto& [pathId, queue] : RequestQueues) {
            auto& writeInfo = WriteInfos.at(pathId);

            for (const auto& [_, actor] : writeInfo.Actors) {
                if (!actor.WriteActor->IsReady()) {
                    CA_LOG_D("ProcessRequestQueue " << pathId << " NOT READY queue=" << queue.size());
                    return;
                }
            }

            while (!queue.empty()) {
                auto& message = queue.front();

                // if lookup isn't needed
                if (message.Data) {
                    for (auto& [actorPathId, actor] : writeInfo.Actors) {
                        if (actorPathId != pathId && actor.Projections.contains(message.Token.Cookie)) {
                            auto preparedBatch = actor.Projections.at(message.Token.Cookie)->Project(message.Data);
                            actor.WriteActor->Write(message.Token.Cookie, preparedBatch);
                        }
                    }
                    writeInfo.Actors.at(pathId).WriteActor->Write(message.Token.Cookie, std::move(message.Data));
                }

                if (message.Close) {
                    for (auto& [actorPathId, actor] : writeInfo.Actors) {
                        if (actorPathId != pathId && actor.Projections.contains(message.Token.Cookie)) {
                            actor.WriteActor->Close(message.Token.Cookie);
                        }
                    }
                    writeInfo.Actors.at(pathId).WriteActor->Close(message.Token.Cookie);
                }

                AckQueue.push(TAckMessage{
                    .ForwardActorId = message.From,
                    .Token = message.Token,
                    .DataSize = 0,
                });

                queue.pop();
            }
        }
    }

    void ProcessAckQueue() {
        while (!AckQueue.empty()) {
            const auto& item = AckQueue.front();
            if (GetTotalFreeSpace() >= item.DataSize) {
                auto result = std::make_unique<TEvBufferWriteResult>();
                result->Token = AckQueue.front().Token;
                Send(AckQueue.front().ForwardActorId, result.release());
                AckQueue.pop();
            } else {
                return;
            }
        }
    }

    bool ProcessFlush() {
        if (!EnableStreamWrite && GetTotalFreeSpace() <= 0) {
            ReplyErrorAndDie(
                NYql::NDqProto::StatusIds::PRECONDITION_FAILED,
                NYql::TIssuesIds::KIKIMR_PRECONDITION_FAILED,
                TStringBuilder() << "Out of buffer memory. Used " << GetTotalMemory()
                        << " bytes of " << MessageSettings.InFlightMemoryLimitPerActorBytes << " bytes.",
                {});
            return false;
        }

        if (NeedToFlush()) {
            CA_LOG_D("Flush data");

            bool flushFailed = false;
            ForEachWriteActor([&](TKqpTableWriteActor* actor, const TActorId) {
                if (!flushFailed && actor->IsReady() && NeedToFlushActor(actor)) {
                    actor->FlushBuffers();
                    if (!actor->FlushToShards()) {
                        flushFailed = true;
                    }
                }
            });

            if (flushFailed) {
                return false;
            }
        }
        return true;
    }

    bool Flush(NWilson::TTraceId traceId) {
        Counters->BufferActorFlushes->Inc();
        UpdateTracingState("Flush", std::move(traceId));
        OperationStartTime = TInstant::Now();

        CA_LOG_D("Start flush");
        YQL_ENSURE(State == EState::WRITING);
        State = EState::FLUSHING;
        CheckQueuesEmpty();
        return Process();
    }

    bool Prepare(std::optional<NWilson::TTraceId> traceId) {
        UpdateTracingState("Commit", std::move(traceId));
        OperationStartTime = TInstant::Now();

        CA_LOG_D("Start prepare for distributed commit");
        YQL_ENSURE(State == EState::WRITING);
        State = EState::PREPARING;
        CheckQueuesEmpty();
        AFL_ENSURE(TxId);
        ForEachWriteActor([&](TKqpTableWriteActor* actor, const TActorId) {
            AFL_ENSURE(!actor->FlushBeforeCommit());
            actor->SetPrepare(*TxId);
        });
        Close();
        if (!Process()) {
            return false;
        }
        SendToExternalShards(false);
        SendToTopics(false);
        return true;
    }

    bool ImmediateCommit(NWilson::TTraceId traceId) {
        Counters->BufferActorImmediateCommits->Inc();
        UpdateTracingState("Commit", std::move(traceId));
        OperationStartTime = TInstant::Now();

        CA_LOG_D("Start immediate commit");
        YQL_ENSURE(State == EState::WRITING);
        State = EState::COMMITTING;
        IsImmediateCommit = true;
        CheckQueuesEmpty();
        ForEachWriteActor([](TKqpTableWriteActor* actor, const TActorId) {
            actor->SetImmediateCommit();
        });
        Close();
        if (!Process()) {
            return false;
        }
        SendToTopics(true);
        return true;
    }

    void DistributedCommit() {
        Counters->BufferActorDistributedCommits->Inc();
        OperationStartTime = TInstant::Now();

        CA_LOG_D("Start distributed commit with TxId=" << *TxId);
        YQL_ENSURE(State == EState::PREPARING);
        State = EState::COMMITTING;
        CheckQueuesEmpty();
        ForEachWriteActor([](TKqpTableWriteActor* actor, const TActorId) {
            actor->SetDistributedCommit();
        });
        SendCommitToCoordinator();
    }

    void Rollback(NWilson::TTraceId traceId) {
        Counters->BufferActorRollbacks->Inc();
        UpdateTracingState("RollBack", std::move(traceId));

        CA_LOG_D("Start rollback");
        State = EState::ROLLINGBACK;
        SendToExternalShards(true);
    }

    void CheckQueuesEmpty() {
        for (const auto& [_, queue] : RequestQueues) {
            AFL_ENSURE(queue.empty());
        }
    }

    void SendToExternalShards(bool isRollback) {
        THashSet<ui64> shards = TxManager->GetShards();
        if (!isRollback) {
            ForEachWriteActor([&](const TKqpTableWriteActor* actor, const TActorId) {
                for (const auto& shardId : actor->GetShardsIds()) {
                    shards.erase(shardId);
                }
            });
        }

        for (const ui64 shardId : shards) {
            if (TxManager->GetLocks(shardId).empty()) {
                continue;
            }
            auto evWrite = std::make_unique<NKikimr::NEvents::TDataEvents::TEvWrite>(isRollback
                ? NKikimrDataEvents::TEvWrite::MODE_IMMEDIATE
                : (TxManager->IsVolatile()
                    ? NKikimrDataEvents::TEvWrite::MODE_VOLATILE_PREPARE
                    : NKikimrDataEvents::TEvWrite::MODE_PREPARE));

            if (isRollback) {
                FillEvWriteRollback(evWrite.get(), shardId, TxManager);
            } else {
                YQL_ENSURE(TxId);
                FillEvWritePrepare(evWrite.get(), shardId, *TxId, TxManager);
            }

            NDataIntegrity::LogIntegrityTrails("EvWriteTx", evWrite->Record.GetTxId(), shardId, TlsActivationContext->AsActorContext(), "BufferActor");

            SendTime[shardId] = TInstant::Now();
            CA_LOG_D("Send EvWrite (external) to ShardID=" << shardId << ", isPrepare=" << !isRollback << ", isImmediateCommit=" << isRollback << ", TxId=" << evWrite->Record.GetTxId()
            << ", LockTxId=" << evWrite->Record.GetLockTxId() << ", LockNodeId=" << evWrite->Record.GetLockNodeId()
            << ", Locks= " << [&]() {
                TStringBuilder builder;
                for (const auto& lock : evWrite->Record.GetLocks().GetLocks()) {
                    builder << lock.ShortDebugString();
                }
                return builder;
            }()
            << ", Size=" << 0 << ", Cookie=" << 0
            << ", OperationsCount=" << 0 << ", IsFinal=" << 1
            << ", Attempts=" << 0);

            // TODO: Track latecy
            Send(
                NKikimr::MakePipePerNodeCacheID(false),
                new TEvPipeCache::TEvForward(evWrite.release(), shardId, /* subscribe */ true),
                0,
                0,
                BufferWriteActorStateSpan.GetTraceId());
        }
    }

    void SendToTopics(bool isImmediateCommit) {
        if (!TxManager->HasTopics()) {
            return;
        }

        TTopicTabletTxs topicTxs;
        TxManager->BuildTopicTxs(topicTxs);

        TMaybe<ui64> writeId;
        if (TxManager->GetTopicOperations().HasWriteId()) {
            writeId = TxManager->GetTopicOperations().GetWriteId();
        }
        bool kafkaTransaction = TxManager->GetTopicOperations().HasKafkaOperations();

        for (auto& [tabletId, t] : topicTxs) {
            auto& transaction = t.tx;
            
            if (!isImmediateCommit) {
                FillTopicsCommit(transaction, TxManager);
            }

            if (t.hasWrite && writeId.Defined() && !kafkaTransaction) {
                auto* w = transaction.MutableWriteId();
                w->SetNodeId(SelfId().NodeId());
                w->SetKeyId(*writeId);
            } else if (t.hasWrite && kafkaTransaction) {
                auto* w = transaction.MutableWriteId();
                w->SetKafkaTransaction(true);
                w->MutableKafkaProducerInstanceId()->SetId(TxManager->GetTopicOperations().GetKafkaProducerInstanceId().Id);
                w->MutableKafkaProducerInstanceId()->SetEpoch(TxManager->GetTopicOperations().GetKafkaProducerInstanceId().Epoch);
            }
            transaction.SetImmediate(isImmediateCommit);

            auto ev = std::make_unique<TEvPersQueue::TEvProposeTransactionBuilder>();

            ActorIdToProto(SelfId(), ev->Record.MutableSourceActor());
            ev->Record.MutableData()->Swap(&transaction);

            if (!isImmediateCommit) {
                YQL_ENSURE(TxId);
                ev->Record.SetTxId(*TxId);
            }

            SendTime[tabletId] = TInstant::Now();

            CA_LOG_D("Executing KQP transaction on topic tablet: " << tabletId
            << ", writeId: " << writeId << ", isImmediateCommit: " << isImmediateCommit);

            Send(
                MakePipePerNodeCacheID(false),
                new TEvPipeCache::TEvForward(ev.release(), tabletId, /* subscribe */ true),
                0,
                0,
                BufferWriteActorStateSpan.GetTraceId());
        }
    }

    void SendCommitToCoordinator() {
        const auto commitInfo = TxManager->GetCommitInfo();

        auto ev = MakeHolder<TEvTxProxy::TEvProposeTransaction>();

        YQL_ENSURE(commitInfo.Coordinator);
        Coordinator = commitInfo.Coordinator;
        ev->Record.SetCoordinatorID(*Coordinator);

        auto& transaction = *ev->Record.MutableTransaction();
        auto& affectedSet = *transaction.MutableAffectedSet();
        affectedSet.Reserve(commitInfo.ShardsInfo.size());

        YQL_ENSURE(TxId);
        transaction.SetTxId(*TxId);
        transaction.SetMinStep(commitInfo.MinStep);
        transaction.SetMaxStep(commitInfo.MaxStep);
        if (TxManager->IsVolatile()) {
            transaction.SetFlags(TEvTxProxy::TEvProposeTransaction::FlagVolatile);
        }

        for (const auto& shardInfo : commitInfo.ShardsInfo) {
            auto& item = *affectedSet.Add();
            item.SetTabletId(shardInfo.ShardId);
            Y_ABORT_UNLESS(shardInfo.AffectedFlags != 0);
            item.SetFlags(shardInfo.AffectedFlags);
        }

        NDataIntegrity::LogIntegrityTrails("PlannedTx", *TxId, {}, TlsActivationContext->AsActorContext(), "BufferActor");

        CA_LOG_D("Execute planned transaction, coordinator: " << *Coordinator
            << ", volitale: " << ((transaction.GetFlags() & TEvTxProxy::TEvProposeTransaction::FlagVolatile) != 0)
            << ", shards: " << affectedSet.size());
        Send(
            MakePipePerNodeCacheID(false),
            new TEvPipeCache::TEvForward(ev.Release(), *Coordinator, /* subscribe */ true),
            0,
            0,
            BufferWriteActorStateSpan.GetTraceId());
    }

    void Close() {
        ForEachWriteActor([&](TKqpTableWriteActor* actor, const TActorId) {
            actor->Close();
        });
    }

    i64 GetTotalFreeSpace() const {
        return MessageSettings.InFlightMemoryLimitPerActorBytes - GetTotalMemory();
    }

    i64 GetTotalMemory() const {
        i64 totalMemory = 0;
        ForEachWriteActor([&](const TKqpTableWriteActor* actor, const TActorId) {
            totalMemory += actor->GetMemory();
        });
        return totalMemory;
    }

    THashSet<ui64> GetShardsIds() const {
        THashSet<ui64> shardIds;
        ForEachWriteActor([&](const TKqpTableWriteActor* actor, const TActorId) {
            for (const auto& id : actor->GetShardsIds()) {
                shardIds.insert(id);
            }
        });

        return shardIds;
    }

    void PassAway() override {
        Counters->BufferActorsCount->Dec();
        for (auto& [_, queue] : RequestQueues) {
            while (!queue.empty()) {
                queue.pop();
            }
        }

        ForEachWriteActor([](TKqpTableWriteActor* actor, const TActorId) {
            actor->Terminate();
        });

        Send(MakePipePerNodeCacheID(false), new TEvPipeCache::TEvUnlink(0));
        TActorBootstrapped<TKqpBufferWriteActor>::PassAway();
    }

    void Handle(TEvTxProxy::TEvProposeTransactionStatus::TPtr &ev) {
        TEvTxProxy::TEvProposeTransactionStatus* res = ev->Get();
        CA_LOG_D("Got transaction status, status: " << res->GetStatus());

        switch (res->GetStatus()) {
            case TEvTxProxy::TEvProposeTransactionStatus::EStatus::StatusAccepted:
                TxProxyMon->ClientTxStatusAccepted->Inc();
                break;
            case TEvTxProxy::TEvProposeTransactionStatus::EStatus::StatusProcessed:
                TxProxyMon->ClientTxStatusProcessed->Inc();
                break;
            case TEvTxProxy::TEvProposeTransactionStatus::EStatus::StatusConfirmed:
                TxProxyMon->ClientTxStatusConfirmed->Inc();
                break;

            case TEvTxProxy::TEvProposeTransactionStatus::EStatus::StatusPlanned:
                TxProxyMon->ClientTxStatusPlanned->Inc();
                TxPlanned = true;
                break;

            case TEvTxProxy::TEvProposeTransactionStatus::EStatus::StatusOutdated:
            case TEvTxProxy::TEvProposeTransactionStatus::EStatus::StatusDeclined:
            case TEvTxProxy::TEvProposeTransactionStatus::EStatus::StatusDeclinedNoSpace:
            case TEvTxProxy::TEvProposeTransactionStatus::EStatus::StatusRestarting:
                TxProxyMon->ClientTxStatusCoordinatorDeclined->Inc();
                ReplyErrorAndDie(
                    NYql::NDqProto::StatusIds::UNAVAILABLE,
                    NYql::TIssuesIds::KIKIMR_TEMPORARILY_UNAVAILABLE,
                    TStringBuilder() << "Failed to plan transaction, status: " << res->GetStatus(),
                    {});
                break;

            case TEvTxProxy::TEvProposeTransactionStatus::EStatus::StatusUnknown:
            case TEvTxProxy::TEvProposeTransactionStatus::EStatus::StatusAborted:
                TxProxyMon->ClientTxStatusCoordinatorDeclined->Inc();
                ReplyErrorAndDie(
                    NYql::NDqProto::StatusIds::INTERNAL_ERROR,
                    NYql::TIssuesIds::KIKIMR_INTERNAL_ERROR,
                    TStringBuilder() << "Unexpected TEvProposeTransactionStatus status: " << res->GetStatus(),
                    {});
                break;
        }
    }

    void Handle(TEvPersQueue::TEvProposeTransactionResult::TPtr& ev) {
        auto& event = ev->Get()->Record;
        const ui64 tabletId = event.GetOrigin();
        
        CA_LOG_D("Got ProposeTransactionResult" <<
              ", PQ tablet: " << tabletId <<
              ", status: " << NKikimrPQ::TEvProposeTransactionResult_EStatus_Name(event.GetStatus()));

        switch (event.GetStatus()) {
        case NKikimrPQ::TEvProposeTransactionResult::PREPARED:
            ProcessPreparedTopic(ev);
            return;
        case NKikimrPQ::TEvProposeTransactionResult::COMPLETE:
            ProcessCompletedTopic(ev);
            return;
        case NKikimrPQ::TEvProposeTransactionResult::ABORTED:
            CA_LOG_E("Got ABORTED ProposeTransactionResult for PQ."
                    << " ShardID=" << ev->Get()->Record.GetOrigin() << ","
                    << " Sink=" << this->SelfId() << ".");
            ReplyErrorAndDie(
                NYql::NDqProto::StatusIds::ABORTED,
                NYql::TIssuesIds::KIKIMR_OPERATION_ABORTED,
                TStringBuilder() << "Aborted proposal status for PQ. ",
                {});
            return;
        case NKikimrPQ::TEvProposeTransactionResult::BAD_REQUEST:
            CA_LOG_E("Got BAD REQUEST ProposeTransactionResult for PQ."
                    << " ShardID=" << ev->Get()->Record.GetOrigin() << ","
                    << " Sink=" << this->SelfId() << ".");
            ReplyErrorAndDie(
                NYql::NDqProto::StatusIds::BAD_REQUEST,
                NYql::TIssuesIds::KIKIMR_BAD_REQUEST,
                TStringBuilder() << "Bad request proposal status for PQ. ",
                {});
            return;
        case NKikimrPQ::TEvProposeTransactionResult::OVERLOADED:
            CA_LOG_E("Got OVERLOADED ProposeTransactionResult for PQ."
                    << " ShardID=" << ev->Get()->Record.GetOrigin() << ","
                    << " Sink=" << this->SelfId() << ".");
            ReplyErrorAndDie(
                NYql::NDqProto::StatusIds::OVERLOADED,
                NYql::TIssuesIds::KIKIMR_OVERLOADED,
                TStringBuilder() << "Overloaded proposal status for PQ. ",
                {});
            return;
        case NKikimrPQ::TEvProposeTransactionResult::CANCELLED:
            CA_LOG_E("Got CANCELLED ProposeTransactionResult for PQ."
                    << " ShardID=" << ev->Get()->Record.GetOrigin() << ","
                    << " Sink=" << this->SelfId() << ".");
            ReplyErrorAndDie(
                NYql::NDqProto::StatusIds::CANCELLED,
                NYql::TIssuesIds::KIKIMR_OPERATION_CANCELLED,
                TStringBuilder() << "Cancelled proposal status for PQ. ",
                {});
            return;
        default:
            CA_LOG_E("Got undefined ProposeTransactionResult for PQ."
                    << " ShardID=" << ev->Get()->Record.GetOrigin() << ","
                    << " Sink=" << this->SelfId() << ".");
            ReplyErrorAndDie(
                NYql::NDqProto::StatusIds::INTERNAL_ERROR,
                NYql::TIssuesIds::KIKIMR_INTERNAL_ERROR,
                TStringBuilder() << "Undefined proposal status for PQ. ",
                {});
            return;
        }
    }

    void Handle(TEvPipeCache::TEvDeliveryProblem::TPtr& ev) {
        CA_LOG_W("TEvDeliveryProblem was received from tablet: " << ev->Get()->TabletId);

        if (Coordinator == ev->Get()->TabletId) {
            if (ev->Get()->NotDelivered) {
                ReplyErrorAndDie(
                    NYql::NDqProto::StatusIds::UNAVAILABLE,
                    NYql::TIssuesIds::KIKIMR_TEMPORARILY_UNAVAILABLE,
                    TStringBuilder() << "Kikimr cluster or one of its subsystems was unavailable. Failed to deviler message to coordinator.",
                    {});
                return;
            }

            if (TxPlanned) {
                // Already planned
                return;
            }

            ReplyErrorAndDie(
                    NYql::NDqProto::StatusIds::UNDETERMINED,
                    NYql::TIssuesIds::KIKIMR_OPERATION_STATE_UNKNOWN,
                    TStringBuilder() << "State of operation is unknown. Failed to deviler message to coordinator.",
                    {});
            return;
        }

        if (State == EState::COMMITTING) {
            ReplyErrorAndDie(
                NYql::NDqProto::StatusIds::UNDETERMINED,
                NYql::TIssuesIds::KIKIMR_OPERATION_STATE_UNKNOWN,
                TStringBuilder() << "State of operation is unknown. Failed to deviler message.",
                {});
            return;
        } else {
            ReplyErrorAndDie(
                NYql::NDqProto::StatusIds::UNAVAILABLE,
                NYql::TIssuesIds::KIKIMR_TEMPORARILY_UNAVAILABLE,
                TStringBuilder() << "Kikimr cluster or one of its subsystems was unavailable. Failed to deviler message.",
                {});
            return;
        }
    }

    void Handle(TEvKqpBuffer::TEvTerminate::TPtr&) {
        if (State != EState::ROLLINGBACK && State != EState::FINISHED) {
            CancelProposal();
            Rollback(BufferWriteActorSpan.GetTraceId());
        }
        PassAway();
    }

    void Handle(TEvKqpBuffer::TEvFlush::TPtr& ev) {
        ExecuterActorId = ev->Get()->ExecuterActorId;
        ForEachWriteActor([](TKqpTableWriteActor* actor, const TActorId) {
            actor->FlushBuffers();
        });
        Flush(std::move(ev->TraceId));
    }

    void Handle(TEvKqpBuffer::TEvCommit::TPtr& ev) {
        ExecuterActorId = ev->Get()->ExecuterActorId;
        ForEachWriteActor([&](TKqpTableWriteActor* actor, const TActorId) {
            actor->FlushBuffers();
        });

        if (!TxManager->NeedCommit()) {
            RollbackAndDie(std::move(ev->TraceId));
        } else if (TxManager->BrokenLocks()) {
            NYql::TIssues issues;
            issues.AddIssue(*TxManager->GetLockIssue());
            ReplyErrorAndDie(
                NYql::NDqProto::StatusIds::ABORTED,
                std::move(issues));
            return;
        } else if (TxManager->IsSingleShard() && !TxManager->HasOlapTable() && (!WriteInfos.empty() || TxManager->HasTopics())) {
            TxManager->StartExecute();
            ImmediateCommit(std::move(ev->TraceId));
        } else {
            AFL_ENSURE(ev->Get()->TxId);
            TxId = ev->Get()->TxId;

            bool needToFlushBeforeCommit = false;
            ForEachWriteActor([&](TKqpTableWriteActor* actor, const TActorId) {
                needToFlushBeforeCommit |= actor->FlushBeforeCommit();
            });

            if (needToFlushBeforeCommit) {
                Flush(std::move(ev->TraceId));
            } else {
                TxManager->StartPrepare();
                Prepare(std::move(ev->TraceId));
            }
        }
    }

    void Handle(TEvKqpBuffer::TEvRollback::TPtr& ev) {
        ExecuterActorId = ev->Get()->ExecuterActorId;
        RollbackAndDie(std::move(ev->TraceId));
    }

    void RollbackAndDie(NWilson::TTraceId traceId) {
        Rollback(std::move(traceId));
        Send<ESendingType::Tail>(ExecuterActorId, new TEvKqpBuffer::TEvResult{});
        PassAway();
    }

    void Handle(NKikimr::NEvents::TDataEvents::TEvWriteResult::TPtr& ev) {
        auto getIssues = [&ev]() {
            NYql::TIssues issues;
            NYql::IssuesFromMessage(ev->Get()->Record.GetIssues(), issues);
            return issues;
        };

        CA_LOG_D("Recv EvWriteResult (external) from ShardID=" << ev->Get()->Record.GetOrigin()
            << ", Status=" << NKikimrDataEvents::TEvWriteResult::EStatus_Name(ev->Get()->GetStatus())
            << ", TxId=" << ev->Get()->Record.GetTxId()
            << ", Locks= " << [&]() {
                TStringBuilder builder;
                for (const auto& lock : ev->Get()->Record.GetTxLocks()) {
                    builder << lock.ShortDebugString();
                }
                return builder;
            }()
            << ", Cookie=" << ev->Cookie);

        auto getPathes = [&]() -> TString {
            const auto tableInfo = TxManager->GetShardTableInfo(ev->Get()->Record.GetOrigin());
            TStringBuilder builder;
            for (const auto& path : tableInfo.Pathes) {
                if (!builder.empty()) {
                    builder << ", ";
                }
                builder << "`" << path << "`";
            }
            return (tableInfo.Pathes.size() == 1 ? "Table: " : "Tables: ")  + builder;
        };

        TxManager->AddParticipantNode(ev->Sender.NodeId());

        // TODO: get rid of copy-paste
        switch (ev->Get()->GetStatus()) {
        case NKikimrDataEvents::TEvWriteResult::STATUS_UNSPECIFIED: {
            CA_LOG_E("Got UNSPECIFIED for tables."
                    << " ShardID=" << ev->Get()->Record.GetOrigin() << ","
                    << " Sink=" << this->SelfId() << "."
                    << getIssues().ToOneLineString());
            TxManager->SetError(ev->Get()->Record.GetOrigin());
            ReplyErrorAndDie(
                NYql::NDqProto::StatusIds::UNSPECIFIED,
                NYql::TIssuesIds::DEFAULT_ERROR,
                TStringBuilder() << "Unspecified error. " << getPathes() << ". "
                    << getIssues().ToOneLineString(),
                getIssues());
            return;
        }
        case NKikimrDataEvents::TEvWriteResult::STATUS_PREPARED: {
            ProcessWritePreparedShard(ev);
            return;
        }
        case NKikimrDataEvents::TEvWriteResult::STATUS_COMPLETED: {
            ProcessWriteCompletedShard(ev);
            return;
        }
        case NKikimrDataEvents::TEvWriteResult::STATUS_ABORTED: {
            CA_LOG_E("Got ABORTED for tables."
                    << " ShardID=" << ev->Get()->Record.GetOrigin() << ","
                    << " Sink=" << this->SelfId() << "."
                    << getIssues().ToOneLineString());
            TxManager->SetError(ev->Get()->Record.GetOrigin());
            ReplyErrorAndDie(
                NYql::NDqProto::StatusIds::ABORTED,
                NYql::TIssuesIds::KIKIMR_OPERATION_ABORTED,
                TStringBuilder() << "Operation aborted.",
                getIssues());
            return;
        }
        case NKikimrDataEvents::TEvWriteResult::STATUS_WRONG_SHARD_STATE: {
            CA_LOG_E("Got WRONG SHARD STATE for tables."
                    << " ShardID=" << ev->Get()->Record.GetOrigin() << ","
                    << " Sink=" << this->SelfId() << "."
                    << getIssues().ToOneLineString());
            TxManager->SetError(ev->Get()->Record.GetOrigin());
            ReplyErrorAndDie(
                NYql::NDqProto::StatusIds::UNAVAILABLE,
                NYql::TIssuesIds::KIKIMR_TEMPORARILY_UNAVAILABLE,
                TStringBuilder() << "Wrong shard state. " << getPathes() << ".",
                getIssues());
            return;
        }
        case NKikimrDataEvents::TEvWriteResult::STATUS_INTERNAL_ERROR: {
            CA_LOG_E("Got INTERNAL ERROR for tables."
                    << " ShardID=" << ev->Get()->Record.GetOrigin() << ","
                    << " Sink=" << this->SelfId() << "."
                    << getIssues().ToOneLineString());
            TxManager->SetError(ev->Get()->Record.GetOrigin());
            ReplyErrorAndDie(
                NYql::NDqProto::StatusIds::INTERNAL_ERROR,
                NYql::TIssuesIds::KIKIMR_INTERNAL_ERROR,
                TStringBuilder() << "Internal error while executing transaction.",
                getIssues());
            return;
        }
        case NKikimrDataEvents::TEvWriteResult::STATUS_DISK_SPACE_EXHAUSTED: {
            CA_LOG_E("Got DISK_SPACE_EXHAUSTED for tables."
                    << " ShardID=" << ev->Get()->Record.GetOrigin() << ","
                    << " Sink=" << this->SelfId() << "."
                    << getIssues().ToOneLineString());
            TxManager->SetError(ev->Get()->Record.GetOrigin());
            ReplyErrorAndDie(
                NYql::NDqProto::StatusIds::UNAVAILABLE,
                NYql::TIssuesIds::KIKIMR_DISK_SPACE_EXHAUSTED,
                TStringBuilder() << "Disk space exhausted. " << getPathes() << ".",
                getIssues());
            return;
        }
        case NKikimrDataEvents::TEvWriteResult::STATUS_OUT_OF_SPACE: {
            CA_LOG_W("Got OUT_OF_SPACE for tables."
                << " ShardID=" << ev->Get()->Record.GetOrigin() << ","
                << " Sink=" << this->SelfId() << "."
                << " Ignored this error."
                << getIssues().ToOneLineString());
            TxManager->SetError(ev->Get()->Record.GetOrigin());
            ReplyErrorAndDie(
                NYql::NDqProto::StatusIds::OVERLOADED,
                NYql::TIssuesIds::KIKIMR_OVERLOADED,
                TStringBuilder() << "Tablet " << ev->Get()->Record.GetOrigin() << " is out of space. " << getPathes() << ".",
                getIssues());
            return;
        }
        case NKikimrDataEvents::TEvWriteResult::STATUS_OVERLOADED: {
            CA_LOG_W("Got OVERLOADED for tables."
                << " ShardID=" << ev->Get()->Record.GetOrigin() << ","
                << " Sink=" << this->SelfId() << "."
                << " Ignored this error."
                << getIssues().ToOneLineString());
            TxManager->SetError(ev->Get()->Record.GetOrigin());
            ReplyErrorAndDie(
                NYql::NDqProto::StatusIds::OVERLOADED,
                NYql::TIssuesIds::KIKIMR_OVERLOADED,
                TStringBuilder() << "Kikimr cluster or one of its subsystems is overloaded."
                    << " Tablet " << ev->Get()->Record.GetOrigin() << " is overloaded."
                    << " " << getPathes() << ".",
                getIssues());
            return;
        }
        case NKikimrDataEvents::TEvWriteResult::STATUS_CANCELLED: {
            CA_LOG_E("Got CANCELLED for tables."
                    << " ShardID=" << ev->Get()->Record.GetOrigin() << ","
                    << " Sink=" << this->SelfId() << "."
                    << getIssues().ToOneLineString());
            TxManager->SetError(ev->Get()->Record.GetOrigin());
            ReplyErrorAndDie(
                NYql::NDqProto::StatusIds::CANCELLED,
                NYql::TIssuesIds::KIKIMR_OPERATION_CANCELLED,
                TStringBuilder() << "Operation cancelled.",
                getIssues());
            return;
        }
        case NKikimrDataEvents::TEvWriteResult::STATUS_BAD_REQUEST: {
            CA_LOG_E("Got BAD REQUEST for tables."
                    << " ShardID=" << ev->Get()->Record.GetOrigin() << ","
                    << " Sink=" << this->SelfId() << "."
                    << getIssues().ToOneLineString());
            TxManager->SetError(ev->Get()->Record.GetOrigin());
            ReplyErrorAndDie(
                NYql::NDqProto::StatusIds::BAD_REQUEST,
                NYql::TIssuesIds::KIKIMR_BAD_REQUEST,
                TStringBuilder() << "Bad request. " << getPathes() << ".",
                getIssues());
            return;
        }
        case NKikimrDataEvents::TEvWriteResult::STATUS_SCHEME_CHANGED: {
            CA_LOG_E("Got SCHEME CHANGED for table."
                    << " ShardID=" << ev->Get()->Record.GetOrigin() << ","
                    << " Sink=" << this->SelfId() << "."
                    << getIssues().ToOneLineString());
            TxManager->SetError(ev->Get()->Record.GetOrigin());
            ReplyErrorAndDie(
                NYql::NDqProto::StatusIds::SCHEME_ERROR,
                NYql::TIssuesIds::KIKIMR_SCHEME_MISMATCH,
                TStringBuilder() << "Scheme changed. " << getPathes() << ".",
                getIssues());
            return;
        }
        case NKikimrDataEvents::TEvWriteResult::STATUS_LOCKS_BROKEN: {
            CA_LOG_E("Got LOCKS BROKEN for table."
                    << " ShardID=" << ev->Get()->Record.GetOrigin() << ","
                    << " Sink=" << this->SelfId() << "."
                    << getIssues().ToOneLineString());
            TxManager->BreakLock(ev->Get()->Record.GetOrigin());
            YQL_ENSURE(TxManager->BrokenLocks());
            TxManager->SetError(ev->Get()->Record.GetOrigin());
            ReplyErrorAndDie(
                NYql::NDqProto::StatusIds::ABORTED,
                NYql::TIssuesIds::KIKIMR_LOCKS_INVALIDATED,
                TStringBuilder()
                    << "Transaction locks invalidated. "
                    << getPathes() << ".",
                getIssues());
            return;
        }
        case NKikimrDataEvents::TEvWriteResult::STATUS_CONSTRAINT_VIOLATION: {
            CA_LOG_E("Got CONSTRAINT VIOLATION for table."
                    << " ShardID=" << ev->Get()->Record.GetOrigin() << ","
                    << " Sink=" << this->SelfId() << "."
                    << getIssues().ToOneLineString());
            TxManager->SetError(ev->Get()->Record.GetOrigin());
            ReplyErrorAndDie(
                NYql::NDqProto::StatusIds::PRECONDITION_FAILED,
                NYql::TIssuesIds::KIKIMR_CONSTRAINT_VIOLATION,
                TStringBuilder() << "Constraint violated. " << getPathes() << ".",
                getIssues());
            return;
        }
        }
    }

    void OnMessageReceived(const ui64 shardId) {
        if (auto it = SendTime.find(shardId); it != std::end(SendTime)) {
            Counters->WriteActorWritesLatencyHistogram->Collect((TInstant::Now() - it->second).MicroSeconds());
            SendTime.erase(it);
        }
    }

    void OnOperationFinished(NMonitoring::THistogramPtr latencyHistogramUs) {
        latencyHistogramUs->Collect((TInstant::Now() - OperationStartTime).MicroSeconds());
    }

    void ProcessPreparedTopic(TEvPersQueue::TEvProposeTransactionResult::TPtr& ev) {
        if (State != EState::PREPARING) {
            CA_LOG_D("Ignored topic prepared event.");
            return;
        }
        OnMessageReceived(ev->Get()->Record.GetOrigin());
        CA_LOG_D("Got propose prepared result TxId=" << ev->Get()->Record.GetTxId()
            << ", TabletId=" << ev->Get()->Record.GetOrigin()
            << ", Cookie=" << ev->Cookie);

        const auto& record = ev->Get()->Record;
        IKqpTransactionManager::TPrepareResult preparedInfo;
        preparedInfo.ShardId = record.GetOrigin();
        preparedInfo.MinStep = record.GetMinStep();
        preparedInfo.MaxStep = record.GetMaxStep();

        preparedInfo.Coordinator = 0;
        if (record.DomainCoordinatorsSize()) {
            auto domainCoordinators = TCoordinators(TVector<ui64>(record.GetDomainCoordinators().begin(),
                                                                  record.GetDomainCoordinators().end()));
            preparedInfo.Coordinator = domainCoordinators.Select(*TxId);
        }

        OnPrepared(std::move(preparedInfo), 0);
    }

    void ProcessCompletedTopic(TEvPersQueue::TEvProposeTransactionResult::TPtr& ev) {
        NKikimrPQ::TEvProposeTransactionResult& event = ev->Get()->Record;

        if (State != EState::COMMITTING) {
            CA_LOG_D("Ignored completed event.");
            return;
        }
        OnMessageReceived(event.GetOrigin());
        CA_LOG_D("Got propose completed result" <<
              ", topic tablet: " << event.GetOrigin() <<
              ", status: " << NKikimrPQ::TEvProposeTransactionResult_EStatus_Name(event.GetStatus()));

        OnCommitted(event.GetOrigin(), 0);
    }

    void ProcessWritePreparedShard(NKikimr::NEvents::TDataEvents::TEvWriteResult::TPtr& ev) {
        if (State != EState::PREPARING) {
            CA_LOG_D("Ignored write prepared event.");
            return;
        }
        OnMessageReceived(ev->Get()->Record.GetOrigin());
        CA_LOG_D("Got prepared result TxId=" << ev->Get()->Record.GetTxId()
            << ", TabletId=" << ev->Get()->Record.GetOrigin()
            << ", Cookie=" << ev->Cookie);

        const auto& record = ev->Get()->Record;
        IKqpTransactionManager::TPrepareResult preparedInfo;
        preparedInfo.ShardId = record.GetOrigin();
        preparedInfo.MinStep = record.GetMinStep();
        preparedInfo.MaxStep = record.GetMaxStep();

        preparedInfo.Coordinator = 0;
        if (record.DomainCoordinatorsSize()) {
            auto domainCoordinators = TCoordinators(TVector<ui64>(record.GetDomainCoordinators().begin(),
                                                                  record.GetDomainCoordinators().end()));
            preparedInfo.Coordinator = domainCoordinators.Select(*TxId);
        }

        OnPrepared(std::move(preparedInfo), 0);
    }

    void ProcessWriteCompletedShard(NKikimr::NEvents::TDataEvents::TEvWriteResult::TPtr& ev) {
        if (State != EState::COMMITTING) {
            CA_LOG_D("Ignored write completed event.");
            return;
        }
        OnMessageReceived(ev->Get()->Record.GetOrigin());
        CA_LOG_D("Got completed result TxId=" << ev->Get()->Record.GetTxId()
            << ", TabletId=" << ev->Get()->Record.GetOrigin()
            << ", Cookie=" << ev->Cookie
            << ", Locks=" << [&]() {
                TStringBuilder builder;
                for (const auto& lock : ev->Get()->Record.GetTxLocks()) {
                    builder << lock.ShortDebugString();
                }
                return builder;
            }());

        OnCommitted(ev->Get()->Record.GetOrigin(), 0);
    }

    void OnReady() override {
        Process();
    }

    void OnPrepared(IKqpTransactionManager::TPrepareResult&& preparedInfo, ui64) override {
        if (State != EState::PREPARING) {
            return;
        }
        if (!preparedInfo.Coordinator || (TxManager->GetCoordinator() && preparedInfo.Coordinator != TxManager->GetCoordinator())) {
            CA_LOG_E("Handle TEvWriteResult: unable to select coordinator. Tx canceled, actorId: " << SelfId()
                << ", previously selected coordinator: " << TxManager->GetCoordinator()
                << ", coordinator selected at propose result: " << preparedInfo.Coordinator);

            TxProxyMon->TxResultAborted->Inc();
            ReplyErrorAndDie(NYql::NDqProto::StatusIds::CANCELLED,
                NKikimrIssues::TIssuesIds::TX_DECLINED_IMPLICIT_COORDINATOR,
                "Unable to choose coordinator.");
            return;
        }
        if (TxManager->ConsumePrepareTransactionResult(std::move(preparedInfo))) {
            OnOperationFinished(Counters->BufferActorPrepareLatencyHistogram);
            TxManager->StartExecute();
            Y_ABORT_UNLESS(GetTotalMemory() == 0);
            DistributedCommit();
            return;
        }
        Process();
    }

    void OnCommitted(ui64 shardId, ui64) override {
        if (State != EState::COMMITTING) {
            return;
        }
        if (TxManager->ConsumeCommitResult(shardId)) {
            CA_LOG_D("Committed TxId=" << TxId.value_or(0));
            OnOperationFinished(Counters->BufferActorCommitLatencyHistogram);
            State = EState::FINISHED;
            Send<ESendingType::Tail>(ExecuterActorId, new TEvKqpBuffer::TEvResult{
                BuildStats()
            });
            ExecuterActorId = {};
            Y_ABORT_UNLESS(GetTotalMemory() == 0);
            PassAway();
            return;
        }
    }

    void OnMessageAcknowledged(ui64) override {
        Process();
    }

    void OnFlushed() {
        YQL_ENSURE(State == EState::FLUSHING);
        UpdateTracingState("Write", BufferWriteActorSpan.GetTraceId());
        OnOperationFinished(Counters->BufferActorFlushLatencyHistogram);
        State = EState::WRITING;

        ForEachWriteActor([&](TKqpTableWriteActor* actor, const TActorId) {
            AFL_ENSURE(TxId || actor->IsEmpty());
            if (actor->IsEmpty()) {
                actor->CleanupClosedTokens();
            }
            if (!TxId) {
                actor->Unlink();
            }

            AFL_ENSURE(!actor->FlushBeforeCommit());
        });

        if (TxId) {
            TxManager->StartPrepare();
            Prepare(std::nullopt);
            return;
        }

        Send<ESendingType::Tail>(ExecuterActorId, new TEvKqpBuffer::TEvResult{
            BuildStats()
        });
        ExecuterActorId = {};
        Y_ABORT_UNLESS(GetTotalMemory() == 0);
    }

    void OnError(NYql::NDqProto::StatusIds::StatusCode statusCode, NYql::EYqlIssueCode id, const TString& message, const NYql::TIssues& subIssues) override {
        ReplyErrorAndDie(statusCode, id, message, subIssues);
    }

    void OnError(NYql::NDqProto::StatusIds::StatusCode statusCode, NYql::TIssues&& issues) override {
        ReplyErrorAndDie(statusCode, std::move(issues));
    }

    void ReplyErrorAndDie(NYql::NDqProto::StatusIds::StatusCode statusCode, auto id, const TString& message, const NYql::TIssues& subIssues = {}) {
        BufferWriteActorStateSpan.EndError(message);

        NYql::TIssue issue(message);
        SetIssueCode(id, issue);
        for (const auto& i : subIssues) {
            issue.AddSubIssue(MakeIntrusive<NYql::TIssue>(i));
        }

        NYql::TIssues issues;
        issues.AddIssue(std::move(issue));

        ReplyErrorAndDieImpl(statusCode, std::move(issues));
    }

    void ReplyErrorAndDie(NYql::NDqProto::StatusIds::StatusCode statusCode, NYql::TIssues&& issues) {
        BufferWriteActorStateSpan.EndError(issues.ToOneLineString());

        ReplyErrorAndDieImpl(statusCode, std::move(issues));
    }

    void UpdateTracingState(const char* name, std::optional<NWilson::TTraceId> traceId) {
        if (!traceId) {
            return;
        }
        BufferWriteActorStateSpan = NWilson::TSpan(TWilsonKqp::BufferWriteActorState, std::move(*traceId),
            name, NWilson::EFlags::AUTO_END);
        if (BufferWriteActorStateSpan.GetTraceId() != BufferWriteActorSpan.GetTraceId()) {
            BufferWriteActorStateSpan.Link(BufferWriteActorSpan.GetTraceId());
        }
        ForEachWriteActor([&](TKqpTableWriteActor* actor, const TActorId) {
            actor->SetParentTraceId(BufferWriteActorStateSpan.GetTraceId());
        });
    }

    void ReplyErrorAndDieImpl(NYql::NDqProto::StatusIds::StatusCode statusCode, NYql::TIssues&& issues) {
        CA_LOG_E("statusCode=" << NYql::NDqProto::StatusIds_StatusCode_Name(statusCode) << ". Issue=" << issues.ToString() << ". sessionActorId=" << SessionActorId << ". isRollback=" << (State == EState::ROLLINGBACK));

        Y_ABORT_UNLESS(!HasError);
        HasError = true;

        CancelProposal();
        if (State != EState::ROLLINGBACK) {
            Rollback(BufferWriteActorSpan.GetTraceId());
            // Rollback can't finish with error
            Send<ESendingType::Tail>(SessionActorId, new TEvKqpBuffer::TEvError{
                statusCode,
                std::move(issues)
            });
        }
        PassAway();
    }

    NYql::NDqProto::TDqTaskStats BuildStats() {
        NYql::NDqProto::TDqTaskStats result;
        ForEachWriteActor([&](TKqpTableWriteActor* actor, const TActorId) {
            actor->FillStats(&result);
        });
        return result;
    }

    void CancelProposal() {
        if (!TxId || !(State == EState::PREPARING || State == EState::COMMITTING)) {
            return;
        }
        for (const auto& shardId : TxManager->GetShards()) {
            const auto state = TxManager->GetState(shardId);

            if (state == IKqpTransactionManager::EShardState::PREPARING
                    || state == IKqpTransactionManager::EShardState::PREPARED
                    || (state == IKqpTransactionManager::EShardState::EXECUTING && IsImmediateCommit)) {
                TxManager->SetError(shardId);
                Send(MakePipePerNodeCacheID(false), new TEvPipeCache::TEvForward(
                    new TEvDataShard::TEvCancelTransactionProposal(*TxId), shardId, false));
            }
        }
    }

    void ForEachWriteActor(std::function<void(TKqpTableWriteActor*, const TActorId)>&& func) {
        for (auto& [_, writeInfo] : WriteInfos) {
            for (auto& [_, actorInfo] : writeInfo.Actors) {
                func(actorInfo.WriteActor, actorInfo.Id);
            }
        }
    }

    void ForEachWriteActor(std::function<void(const TKqpTableWriteActor*, const TActorId)>&& func) const {
        for (const auto& [_, writeInfo] : WriteInfos) {
            for (const auto& [_, actorInfo] : writeInfo.Actors) {
                func(actorInfo.WriteActor, actorInfo.Id);
            }
        }
    }

private:
    TString LogPrefix;
    const TActorId SessionActorId;
    TWriteActorSettings MessageSettings;

    TActorId ExecuterActorId;
    IKqpTransactionManagerPtr TxManager;

    std::optional<ui64> TxId;
    ui64 LockTxId = 0;
    ui64 LockNodeId = 0;
    bool InconsistentTx = false;
    bool EnableStreamWrite = true;

    bool IsImmediateCommit = false;
    bool TxPlanned = false;
    std::optional<ui64> Coordinator;

    std::shared_ptr<NKikimr::NMiniKQL::TScopedAlloc> Alloc;

    struct TWriteInfo {
        struct TActorInfo {
            THashMap<IShardedWriteController::TWriteToken, IDataBatchProjectionPtr> Projections;
            TKqpTableWriteActor* WriteActor = nullptr;
            TActorId Id;
        };

        THashMap<TPathId, TActorInfo> Actors;
    };

    THashMap<TPathId, TWriteInfo> WriteInfos;
    TKqpTableWriteActor::TWriteToken CurrentWriteToken = 0;

    EState State;
    bool HasError = false;
    THashMap<TPathId, std::queue<TBufferWriteMessage>> RequestQueues;

    struct TAckMessage {
        TActorId ForwardActorId;
        TWriteToken Token;
        i64 DataSize;
    };
    std::queue<TAckMessage> AckQueue;

    TIntrusivePtr<TKqpCounters> Counters;
    TIntrusivePtr<NTxProxy::TTxProxyMon> TxProxyMon;
    THashMap<ui64, TInstant> SendTime;
    TInstant OperationStartTime;

    NWilson::TSpan BufferWriteActorSpan;
    NWilson::TSpan BufferWriteActorStateSpan;
};

class TKqpForwardWriteActor : public TActorBootstrapped<TKqpForwardWriteActor>, public NYql::NDq::IDqComputeActorAsyncOutput {
    using TBase = TActorBootstrapped<TKqpForwardWriteActor>;

public:
    TKqpForwardWriteActor(
        NKikimrKqp::TKqpTableSinkSettings&& settings,
        NYql::NDq::TDqAsyncIoFactory::TSinkArguments&& args,
        TIntrusivePtr<TKqpCounters> counters)
        : LogPrefix(TStringBuilder() << "TxId: " << args.TxId << ", task: " << args.TaskId << ". ")
        , Settings(std::move(settings))
        , MessageSettings(GetWriteActorSettings())
        , Alloc(args.Alloc)
        , OutputIndex(args.OutputIndex)
        , Callbacks(args.Callback)
        , Counters(counters)
        , BufferActorId(ActorIdFromProto(Settings.GetBufferActorId()))
        , TxId(std::get<ui64>(args.TxId))
        , TableId(
            Settings.GetTable().GetOwnerId(),
            Settings.GetTable().GetTableId(),
            Settings.GetTable().GetVersion())
        , ForwardWriteActorSpan(TWilsonKqp::ForwardWriteActor, NWilson::TTraceId(args.TraceId), "ForwardWriteActor")
    {
        EgressStats.Level = args.StatsLevel;

        TVector<NKikimrKqp::TKqpColumnMetadataProto> columnsMetadata(
            Settings.GetColumns().begin(),
            Settings.GetColumns().end());
        std::vector<ui32> writeIndex(
            Settings.GetWriteIndexes().begin(),
            Settings.GetWriteIndexes().end());
        TGuard guard(*Alloc);
        if (Settings.GetIsOlap()) {
            Batcher = CreateColumnDataBatcher(columnsMetadata, std::move(writeIndex), Alloc);
        } else {
            Batcher = CreateRowDataBatcher(columnsMetadata, std::move(writeIndex), Alloc);
        }

        Counters->ForwardActorsCount->Inc();
    }

    void Bootstrap() {
        LogPrefix = TStringBuilder() << "SelfId: " << this->SelfId() << ", " << LogPrefix;
        Become(&TKqpForwardWriteActor::StateFuncFwd);
    }

    static constexpr char ActorName[] = "KQP_FORWARD_WRITE_ACTOR";

private:
    STFUNC(StateFuncFwd) {
        try {
            switch (ev->GetTypeRewrite()) {
                hFunc(TEvBufferWriteResult, Handle);
            default:
                AFL_ENSURE(false)("unknown message", ev->GetTypeRewrite());
            }
        } catch (...) {
            RuntimeError(
                CurrentExceptionMessage(),
                NYql::NDqProto::StatusIds::INTERNAL_ERROR);
            return;
        }
    }

    void Handle(TEvBufferWriteResult::TPtr& result) {
        CA_LOG_D("TKqpForwardWriteActor recieve EvBufferWriteResult from " << BufferActorId);

        WriteToken = result->Get()->Token;
        OnFlushed();
    }

    void OnFlushed() {
        InFlight = false;

        EgressStats.Bytes += DataSize;
        EgressStats.Chunks++;
        EgressStats.Splits++;
        EgressStats.Resume();

        Counters->ForwardActorWritesSizeHistogram->Collect(DataSize);
        DataSize = 0;

        if (Closed) {
            CA_LOG_D("Finished");
            Callbacks->OnAsyncOutputFinished(GetOutputIndex());
        } else {
            CA_LOG_D("Resume with freeSpace=" << GetFreeSpace());
            Callbacks->ResumeExecution();
        }
    }

    void WriteToBuffer() {
        InFlight = true;
        auto ev = std::make_unique<TEvBufferWrite>();

        ev->Data = Batcher->Build();
        ev->Data->DetachAlloc();
        ev->Close = Closed;

        if (!WriteToken.IsEmpty()) {
            ev->Token = WriteToken;
        } else {
            TVector<NKikimrKqp::TKqpColumnMetadataProto> keyColumnsMetadata(
                Settings.GetKeyColumns().begin(),
                Settings.GetKeyColumns().end());
            TVector<NKikimrKqp::TKqpColumnMetadataProto> columnsMetadata(
                Settings.GetColumns().begin(),
                Settings.GetColumns().end());
            std::vector<ui32> writeIndex(
                Settings.GetWriteIndexes().begin(),
                Settings.GetWriteIndexes().end());

            ev->Settings = TWriteSettings{
                .TableId = TableId,
                .TablePath = Settings.GetTable().GetPath(),
                .OperationType = GetOperation(Settings.GetType()),
                .KeyColumns = std::move(keyColumnsMetadata),
                .Columns = std::move(columnsMetadata),
                .WriteIndex = std::move(writeIndex),
                .TransactionSettings = TTransactionSettings{
                    .TxId = TxId,
                    .LockTxId = Settings.GetLockTxId(),
                    .LockNodeId = Settings.GetLockNodeId(),
                    .InconsistentTx = Settings.GetInconsistentTx(),
                    .MvccSnapshot = GetOptionalMvccSnapshot(Settings),
                    .LockMode = Settings.GetLockMode(),
                },
                .Priority = Settings.GetPriority(),
                .EnableStreamWrite = Settings.GetEnableStreamWrite(),
                .IsOlap = Settings.GetIsOlap(),
            };

            for (const auto& indexSettings : Settings.GetIndexes()) {
                TVector<NKikimrKqp::TKqpColumnMetadataProto> keyColumnsMetadata(
                    indexSettings.GetKeyColumns().begin(),
                    indexSettings.GetKeyColumns().end());
                TVector<NKikimrKqp::TKqpColumnMetadataProto> columnsMetadata(
                    indexSettings.GetColumns().begin(),
                    indexSettings.GetColumns().end());
                std::vector<ui32> writeIndex(
                    indexSettings.GetWriteIndexes().begin(),
                    indexSettings.GetWriteIndexes().end());
                AFL_ENSURE(writeIndex.size() == columnsMetadata.size());

                ev->Settings->Indexes.push_back(TWriteSettings::TIndex {
                    .TableId = TTableId(indexSettings.GetTable().GetOwnerId(),
                                        indexSettings.GetTable().GetTableId(),
                                        indexSettings.GetTable().GetVersion()),
                    .TablePath = indexSettings.GetTable().GetPath(),
                    .KeyColumns = std::move(keyColumnsMetadata),
                    .Columns = std::move(columnsMetadata),
                    .WriteIndex = std::move(writeIndex),
                    .IsUniq = indexSettings.GetIsUniq(),
                });
            }
        }

        ev->SendTime = TInstant::Now();

        if (ev->Data->IsEmpty() && ev->Close && WriteToken.IsEmpty()) {
            // Nothing was written
            OnFlushed();
            return;
        }

        CA_LOG_D("Send data=" << DataSize << ", closed=" << Closed << ", bufferActorId=" << BufferActorId);
        AFL_ENSURE(Send(BufferActorId, ev.release()));
    }

    void CommitState(const NYql::NDqProto::TCheckpoint&) final {};
    void LoadState(const NYql::NDq::TSinkState&) final {};

    ui64 GetOutputIndex() const final {
        return OutputIndex;
    }

    const NYql::NDq::TDqAsyncStats& GetEgressStats() const final {
        return EgressStats;
    }

    i64 GetFreeSpace() const final {
        return InFlight
            ? std::numeric_limits<i64>::min()
            : MessageSettings.MaxForwardedSize - DataSize;
    }

    TMaybe<google::protobuf::Any> ExtraData() override {
        return {};
    }

    void SendData(NMiniKQL::TUnboxedValueBatch&& data, i64 size, const TMaybe<NYql::NDqProto::TCheckpoint>&, bool finished) final {
        YQL_ENSURE(!data.IsWide(), "Wide stream is not supported yet");
        Closed |= finished;
        Batcher->AddData(data);
        DataSize += size;

        CA_LOG_D("Add data: " << size << " / " << DataSize);
        if (Closed || GetFreeSpace() <= 0) {
            WriteToBuffer();
        }
    }

    void RuntimeError(const TString& message, NYql::NDqProto::StatusIds::StatusCode statusCode, const NYql::TIssues& subIssues = {}) {
        ForwardWriteActorSpan.EndError(message);

        CA_LOG_E("RuntimeError: " << message);
        NYql::TIssue issue(message);
        for (const auto& i : subIssues) {
            issue.AddSubIssue(MakeIntrusive<NYql::TIssue>(i));
        }

        NYql::TIssues issues;
        issues.AddIssue(std::move(issue));

        Callbacks->OnAsyncOutputError(OutputIndex, std::move(issues), statusCode);
    }

    void PassAway() override {
        Counters->ForwardActorsCount->Dec();
        TActorBootstrapped<TKqpForwardWriteActor>::PassAway();
    }

    TString LogPrefix;
    const NKikimrKqp::TKqpTableSinkSettings Settings;
    TWriteActorSettings MessageSettings;
    std::shared_ptr<NKikimr::NMiniKQL::TScopedAlloc> Alloc;
    const ui64 OutputIndex;
    NYql::NDq::TDqAsyncStats EgressStats;
    NYql::NDq::IDqComputeActorAsyncOutput::ICallbacks * Callbacks = nullptr;
    TIntrusivePtr<TKqpCounters> Counters;

    TActorId BufferActorId;
    IDataBatcherPtr Batcher;

    i64 DataSize = 0;
    bool Closed = false;
    bool InFlight = false;

    const ui64 TxId;
    const TTableId TableId;

    TWriteToken WriteToken;
    NWilson::TSpan ForwardWriteActorSpan;
};

NActors::IActor* CreateKqpBufferWriterActor(TKqpBufferWriterSettings&& settings) {
    return new TKqpBufferWriteActor(std::move(settings));
}


void RegisterKqpWriteActor(NYql::NDq::TDqAsyncIoFactory& factory, TIntrusivePtr<TKqpCounters> counters) {
    factory.RegisterSink<NKikimrKqp::TKqpTableSinkSettings>(
        TString(NYql::KqpTableSinkName),
        [counters] (NKikimrKqp::TKqpTableSinkSettings&& settings, NYql::NDq::TDqAsyncIoFactory::TSinkArguments&& args) {
            if (!ActorIdFromProto(settings.GetBufferActorId())) {
                auto* actor = new TKqpDirectWriteActor(std::move(settings), std::move(args), counters);
                return std::make_pair<NYql::NDq::IDqComputeActorAsyncOutput*, NActors::IActor*>(actor, actor);
            } else {
                auto* actor = new TKqpForwardWriteActor(std::move(settings), std::move(args), counters);
                return std::make_pair<NYql::NDq::IDqComputeActorAsyncOutput*, NActors::IActor*>(actor, actor);
            }
        });
}

}
}
