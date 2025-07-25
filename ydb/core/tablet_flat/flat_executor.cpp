#include "flat_executor.h"
#include "flat_executor_bootlogic.h"
#include "flat_executor_txloglogic.h"
#include "flat_executor_borrowlogic.h"
#include "flat_bio_actor.h"
#include "flat_executor_snapshot.h"
#include "flat_scan_actor.h"
#include "flat_ops_compact.h"
#include "flat_part_loader.h"
#include "flat_store_hotdog.h"
#include "flat_store_solid.h"
#include "flat_exec_broker.h"
#include "flat_exec_seat.h"
#include "flat_exec_commit_mgr.h"
#include "flat_exec_scans.h"
#include "flat_exec_memory.h"
#include "flat_boot_cookie.h"
#include "flat_boot_oven.h"
#include "flat_executor_tx_env.h"
#include "flat_executor_counters.h"
#include "logic_snap_main.h"
#include "logic_alter_main.h"
#include "flat_abi_evol.h"
#include "probes.h"
#include "shared_sausagecache.h"
#include "util_fmt_abort.h"
#include "util_fmt_desc.h"

#include <ydb/core/base/appdata.h>
#include <ydb/core/base/hive.h>
#include <ydb/core/base/tablet_pipecache.h>
#include <ydb/core/control/lib/immediate_control_board_impl.h>
#include <ydb/core/scheme/scheme_type_registry.h>
#include <ydb/core/tablet/tablet_counters_aggregator.h>
#include <ydb/library/wilson_ids/wilson.h>
#include <ydb/library/yverify_stream/yverify_stream.h>

#include <library/cpp/monlib/service/pages/templates.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/core/monotonic_provider.h>

#include <util/generic/xrange.h>
#include <util/generic/ymath.h>

namespace NKikimr {
namespace NTabletFlatExecutor {

static constexpr ui64 MaxTxInFly = 10000;

LWTRACE_USING(TABLET_FLAT_PROVIDER)

struct TCompactionChangesCtx {
    NKikimrExecutorFlat::TTablePartSwitch& Proto;
    TProdCompact::TResults* Results;

    TCompactionChangesCtx(
            NKikimrExecutorFlat::TTablePartSwitch& proto,
            TProdCompact::TResults* results = nullptr)
        : Proto(proto)
        , Results(results)
    { }
};

class TMemTableMemoryConsumersCollection : public NTable::IMemTableMemoryConsumersCollection {
public:
    TMemTableMemoryConsumersCollection(TActorSystem* actorSystem, TActorId owner)
        : ActorSystem(actorSystem)
        , Owner(owner)
        , MemoryControllerId(NMemory::MakeMemoryControllerId())
    {}

    void Register(ui32 table) override {
        Send(new NMemory::TEvMemTableRegister(table));
    }

    void Unregister(ui32 table) override {
        Send(new NMemory::TEvMemTableUnregister(table));
    }

    void CompactionComplete(TIntrusivePtr<NMemory::IMemoryConsumer> consumer) override {
        Send(new NMemory::TEvMemTableCompacted(std::move(consumer)));
    }

private:
    void Send(IEventBase* ev) {
        ActorSystem->Send(new IEventHandle(MemoryControllerId, Owner, ev));
    }

    TActorSystem* ActorSystem;
    const TActorId Owner;
    const TActorId MemoryControllerId;
};

TTableSnapshotContext::TTableSnapshotContext() = default;
TTableSnapshotContext::~TTableSnapshotContext() = default;

using namespace NResourceBroker;

class TExecutor::TActiveTransactionZone {
public:
    explicit TActiveTransactionZone(TExecutor* self)
        : Self(self)
    {
        Y_DEBUG_ABORT_UNLESS(!Self->ActiveTransaction);
        Self->ActiveTransaction = true;
        Active = true;
    }

    ~TActiveTransactionZone() {
        Done();
    }

    void Done() {
        if (Active) {
            Self->ActiveTransaction = false;
            Active = false;
        }
    }

private:
    TExecutor* Self;
    bool Active = false;
};

TExecutor::TExecutor(
        NFlatExecutorSetup::ITablet* owner,
        const TActorId& ownerActorId)
    : TActor(&TThis::StateInit)
    , Time(TAppData::TimeProvider)
    , Owner(owner)
    , OwnerActorId(ownerActorId)
    , Emitter(new TIdEmitter)
    , CounterEventsInFlight(new TEvTabletCounters::TInFlightCookie)
    , Stats(new TExecutorStatsImpl())
    , LogFlushDelayOverrideUsec(-1, -1, 60*1000*1000)
    , MaxCommitRedoMB(256, 1, 4096)
{}

TExecutor::~TExecutor() {

}

bool TExecutor::OnUnhandledException(const std::exception& e) {
    if (AppData()->FeatureFlags.GetEnableTabletRestartOnUnhandledExceptions()) {
        if (auto log = Logger->Log(ELnLev::Crit)) {
            log << "Tablet " << TabletId() << " unhandled exception " << TypeName(e) << ": " << e.what()
                << '\n' << TBackTrace::FromCurrentException().PrintToString();
        }
        Broken();
        return true;
    }

    // Exception will propagate and cause the process to crash
    return false;
}

ui64 TExecutor::Stamp() const noexcept
{
    return CommitManager ? CommitManager->Stamp() : TTxStamp{ Generation0, Step0 }.Raw;
}

TActorContext TExecutor::SelfCtx() const {
    return TActivationContext::ActorContextFor(SelfId());
}

TActorContext TExecutor::OwnerCtx() const {
    return TActivationContext::ActorContextFor(OwnerActorId);
}

void TExecutor::Registered(TActorSystem *sys, const TActorId&)
{
    Logger = new NUtil::TLogger(sys, NKikimrServices::TABLET_EXECUTOR);
    Broker = new TBroker(this, Emitter);
    Scans = new TScans(Logger.Get(), this, Emitter, Owner, OwnerActorId);
    Memory = new TMemory(Logger.Get(), this, Emitter, Sprintf(" at tablet %" PRIu64, Owner->TabletID()));
    MemTableMemoryConsumersCollection = new TMemTableMemoryConsumersCollection(NActors::TActivationContext::ActorSystem(), SelfId());
    TString myTabletType = TTabletTypes::TypeToStr(Owner->TabletType());
    AppData()->Icb->RegisterSharedControl(LogFlushDelayOverrideUsec, myTabletType + "_LogFlushDelayOverrideUsec");
    AppData()->Icb->RegisterSharedControl(MaxCommitRedoMB, "TabletControls.MaxCommitRedoMB");

    // instantiate alert counters so even never reported alerts are created
    GetServiceCounters(AppData()->Counters, "tablets")->GetCounter("alerts_pending_nodata", true);
    GetServiceCounters(AppData()->Counters, "tablets")->GetCounter("alerts_req_nodata", true);
    GetServiceCounters(AppData()->Counters, "tablets")->GetCounter("alerts_scan_nodata", true);
    GetServiceCounters(AppData()->Counters, "tablets")->GetCounter("alerts_scan_broken", true);
    GetServiceCounters(AppData()->Counters, "tablets")->GetCounter("alerts_boot_nodata", true);
    GetServiceCounters(AppData()->Counters, "tablets")->GetCounter("alerts_broken", true);
}

void TExecutor::PassAway() {
    if (auto logl = Logger->Log(ELnLev::Info)) {
        auto *waste = LogicSnap ? &LogicSnap->Waste() : nullptr;

        logl
            << NFmt::Do(*this) << " suiciding, " << NFmt::If(waste, true);
    }

    if (CompactionLogic) {
        CompactionLogic->Stop();
    }

    if (Broker || Scans || Memory) {
        Send(NResourceBroker::MakeResourceBrokerID(), new NResourceBroker::TEvResourceBroker::TEvNotifyActorDied);
    }

    Scans->Drop();
    Owner = nullptr;

    Send(MakeSharedPageCacheId(), new NSharedCache::TEvUnregister());

    return TActor::PassAway();
}

void TExecutor::Broken() {
    GetServiceCounters(AppData()->Counters, "tablets")->GetCounter("alerts_broken", true)->Inc();

    if (BootLogic)
        BootLogic->Cancel();

    if (Owner) {
        TabletCountersForgetTablet(Owner->TabletID(), Owner->TabletType(),
            Owner->Info()->TenantPathId, Stats->IsFollower(), SelfId());
        Owner->Detach(OwnerCtx());
    }

    return PassAway();
}

void TExecutor::RecreatePageCollectionsCache()
{
    PrivatePageCache = MakeHolder<TPrivatePageCache>();
    StickyPagesMemory = 0;

    Stats->PacksMetaBytes = 0;

    for (const auto &it : Database->GetScheme().Tables) {
        auto subset = Database->Subset(it.first, NTable::TEpoch::Max(), { }, { });

        for (auto &partView : subset->Flatten) {
            AddCachesOfBundle(partView);
        }
    }

    if (TransactionWaitPads) {
        auto it = TransactionWaitPads.begin();
        while (it != TransactionWaitPads.end()) {
            it->second->WaitingSpan.EndOk();
            TSeat* seat = it->second->Seat;
            Y_ENSURE(seat->State == ESeatState::Waiting);
            seat->State = ESeatState::None;
            TransactionWaitPads.erase(it++);

            if (seat->Cancelled) {
                FinishCancellation(seat, false);
            } else {
                EnqueueActivation(seat, false);
            }
        }
    }
}

void TExecutor::ReflectSchemeSettings()
{
    for (const auto &it : Scheme().Tables) {
        auto &policy = *it.second.CompactionPolicy;

        Scans->Configure(it.first,
            policy.ReadAheadLoThreshold,
            policy.ReadAheadHiThreshold,
            policy.DefaultTaskPriority,
            policy.BackupResourceBrokerTask);
    }

    if (CommitManager) {
        using ETactic = TEvBlobStorage::TEvPut::ETactic;

        auto fast = Scheme().Executor.LogFastTactic;

        CommitManager->SetTactic(fast ? ETactic::TacticMinLatency : ETactic::TacticDefault);
    }
}

void TExecutor::OnYellowChannels(TVector<ui32> yellowMoveChannels, TVector<ui32> yellowStopChannels) {
    size_t oldMoveCount = Stats->YellowMoveChannels.size();
    size_t oldStopCount = Stats->YellowStopChannels.size();
    CheckYellow(std::move(yellowMoveChannels), std::move(yellowStopChannels));
    if (oldMoveCount != Stats->YellowMoveChannels.size() ||
        oldStopCount != Stats->YellowStopChannels.size())
    {
        Owner->OnYellowChannelsChanged();
    }
}

void TExecutor::CheckYellow(TVector<ui32> &&yellowMoveChannels, TVector<ui32> &&yellowStopChannels, bool terminal) {
    if (!yellowMoveChannels && !yellowStopChannels) {
        // Make sure to send known yellow channels one last time
        if (Stats->YellowMoveChannels && terminal) {
            SendReassignYellowChannels(Stats->YellowMoveChannels);
        }
        return;
    }

    size_t oldMoveCount = Stats->YellowMoveChannels.size();
    size_t oldStopCount = Stats->YellowStopChannels.size();
    Stats->YellowMoveChannels.insert(Stats->YellowMoveChannels.end(), yellowMoveChannels.begin(), yellowMoveChannels.end());
    SortUnique(Stats->YellowMoveChannels);
    size_t newMoveCount = Stats->YellowMoveChannels.size();
    Stats->IsAnyChannelYellowMove = !Stats->YellowMoveChannels.empty();
    Stats->YellowStopChannels.insert(Stats->YellowStopChannels.end(), yellowStopChannels.begin(), yellowStopChannels.end());
    SortUnique(Stats->YellowStopChannels);
    size_t newStopCount = Stats->YellowStopChannels.size();
    Stats->IsAnyChannelYellowStop = !Stats->YellowStopChannels.empty();

    if (newMoveCount > oldMoveCount) {
        if (auto line = Logger->Log(ELnLev::Debug)) {
            line << NFmt::Do(*this) << " CheckYellow current light yellow move channels:";
            for (ui32 channel : Stats->YellowMoveChannels) {
                line << ' ' << channel;
            }
        }
    }
    if (newStopCount > oldStopCount) {
        if (auto line = Logger->Log(ELnLev::Debug)) {
            line << NFmt::Do(*this) << " CheckYellow current yellow stop channels:";
            for (ui32 channel : Stats->YellowStopChannels) {
                line << ' ' << channel;
            }
        }
    }

    // Request reassignment of currently known yellow channels
    // Each time we discover a new yellow channel or every 15 seconds
    if (newMoveCount != oldMoveCount || !HasYellowCheckInFly || terminal) {
        SendReassignYellowChannels(Stats->YellowMoveChannels);
    }

    if (HasYellowCheckInFly || terminal)
        return;

    HasYellowCheckInFly = true;
    Schedule(TDuration::Seconds(15), new TEvPrivate::TEvCheckYellow());
}

void TExecutor::SendReassignYellowChannels(const TVector<ui32> &yellowChannels) {
    if (Owner->ReassignChannelsEnabled()) {
        auto* info = Owner->Info();
        if (Y_LIKELY(info) && info->HiveId) {
            Send(MakePipePerNodeCacheID(false),
                new TEvPipeCache::TEvForward(
                    new TEvHive::TEvReassignTabletSpace(info->TabletID, yellowChannels),
                    info->HiveId,
                    /* subscribe */ false));
        }
    }
}

void TExecutor::UpdateYellow() {
    Register(CreateTabletDSChecker(SelfId(), Owner->Info()));
}

void TExecutor::UpdateCompactions() {
    CompactionLogic->UpdateCompactions();
    Schedule(TDuration::Minutes(1), new TEvPrivate::TEvUpdateCompactions);
}

void TExecutor::Handle(TEvTablet::TEvCheckBlobstorageStatusResult::TPtr &ev) {
    Y_ENSURE(HasYellowCheckInFly);
    HasYellowCheckInFly = false;

    const auto* info = Owner->Info();
    Y_ENSURE(info);

    TVector<ui32> lightYellowMoveGroups = std::move(ev->Get()->LightYellowMoveGroups);
    SortUnique(lightYellowMoveGroups);
    TVector<ui32> yellowStopGroups = std::move(ev->Get()->YellowStopGroups);
    SortUnique(yellowStopGroups);

    // Transform groups to a list of channels
    TVector<ui32> lightYellowMoveChannels;
    TVector<ui32> yellowStopChannels;
    for (ui32 channel : xrange(info->Channels.size())) {
        const ui32 group = info->ChannelInfo(channel)->LatestEntry()->GroupID;
        auto it = std::lower_bound(lightYellowMoveGroups.begin(), lightYellowMoveGroups.end(), group);
        if (it != lightYellowMoveGroups.end() && *it == group) {
            lightYellowMoveChannels.push_back(channel);
        }
        auto itStop = std::lower_bound(yellowStopGroups.begin(), yellowStopGroups.end(), group);
        if (itStop != yellowStopGroups.end() && *itStop == group) {
            yellowStopChannels.push_back(channel);
        }
    }

    auto prevMoveChannels = Stats->YellowMoveChannels;
    auto prevStopChannels = Stats->YellowStopChannels;
    Stats->YellowMoveChannels.clear();
    Stats->YellowStopChannels.clear();
    Stats->IsAnyChannelYellowMove = false;
    Stats->IsAnyChannelYellowStop = false;

    CheckYellow(std::move(lightYellowMoveChannels), std::move(yellowStopChannels));

    if (prevMoveChannels != Stats->YellowMoveChannels ||
        prevStopChannels != Stats->YellowStopChannels)
    {
        Owner->OnYellowChannelsChanged();
    }
}

void TExecutor::ActivateFollower(const TActorContext &ctx) {
    if (auto logl = Logger->Log(ELnLev::Info))
        logl << NFmt::Do(*this) << " activating executor";

    auto loadedState = BootLogic->ExtractState();
    BootLogic.Destroy();

    Y_ENSURE(Counters, "Expected to have Counters initialized during Boot processing");

    Y_ENSURE(!GcLogic);
    Y_ENSURE(!LogicRedo);
    Y_ENSURE(!LogicAlter);

    Database = loadedState->Database;
    BorrowLogic = loadedState->Loans;

    Y_ENSURE(!CompactionLogic);

    ResourceMetrics = MakeHolder<NMetrics::TResourceMetrics>(Owner->TabletID(), FollowerId, Launcher);

    PendingBlobQueue.Config.TabletID = Owner->TabletID();
    PendingBlobQueue.Config.Generation = Generation();
    PendingBlobQueue.Config.Follower = true;
    PendingBlobQueue.Config.NoDataCounter = GetServiceCounters(AppData()->Counters, "tablets")->GetCounter("alerts_pending_nodata", true);

    ReadResourceProfile();
    RecreatePageCollectionsCache();
    ReflectSchemeSettings();

    RequestInMemPagesForDatabase();

    Become(&TThis::StateFollower);
    Stats->IsActive = true;
    Stats->FollowerId = FollowerId;

    PlanTransactionActivation();

    Owner->ActivateExecutor(OwnerCtx());

    UpdateCounters(ctx);
    ApplyFollowerPostponedUpdates();
}

void TExecutor::Active(const TActorContext &ctx) {
    if (auto logl = Logger->Log(ELnLev::Info))
        logl << NFmt::Do(*this) << " activating executor";

    auto loadedState = BootLogic->ExtractState();
    BootLogic.Destroy();

    Y_ENSURE(Counters, "Expected to have Counters initialized during Boot processing");

    CommitManager = loadedState->CommitManager;
    Database = loadedState->Database;
    LogicSnap = loadedState->Snap;
    GcLogic = loadedState->GcLogic;
    LogicRedo = loadedState->Redo;
    LogicAlter = loadedState->Alter;
    BorrowLogic = loadedState->Loans;
    Stats->CompactedPartLoans = BorrowLogic->GetCompactedLoansList();
    Stats->HasSharedBlobs = BorrowLogic->GetHasFlag();

    CommitManager->Start(this, Owner->Tablet(), &Step0, Counters.Get());

    CompactionLogic = THolder<TCompactionLogic>(new TCompactionLogic(MemTableMemoryConsumersCollection.Get(), Logger.Get(), Broker.Get(), this, loadedState->Comp,
                                                                     Sprintf("tablet-%" PRIu64, Owner->TabletID())));
    VacuumLogic = MakeHolder<TVacuumLogic>(static_cast<NActors::IActorOps*>(this), this, Owner, Logger.Get(), GcLogic.Get());
    LogicRedo->InstallCounters(Counters.Get(), AppTxCounters);

    ResourceMetrics = MakeHolder<NMetrics::TResourceMetrics>(Owner->TabletID(), 0, Launcher);

    PendingBlobQueue.Config.TabletID = Owner->TabletID();
    PendingBlobQueue.Config.Generation = Generation();
    PendingBlobQueue.Config.Follower = false;
    PendingBlobQueue.Config.NoDataCounter = GetServiceCounters(AppData()->Counters, "tablets")->GetCounter("alerts_pending_nodata", true);

    ReadResourceProfile();
    RecreatePageCollectionsCache();
    ReflectSchemeSettings();

    RequestInMemPagesForDatabase();

    Become(&TThis::StateWork);
    Stats->IsActive = true;
    Stats->FollowerId = 0;

    CompactionLogic->Start();

    for (const auto &it: Database->GetScheme().Tables)
        CompactionLogic->UpdateInMemStatsStep(it.first, 0, Database->GetTableMemSize(it.first));

    UpdateCompactions();

    LeaseEnabled = Owner->ReadOnlyLeaseEnabled();
    if (LeaseEnabled) {
        LeaseDuration = Owner->ReadOnlyLeaseDuration();
        if (!LeaseDuration) {
            LeaseEnabled = false;
        } else {
            LeaseDurationUpdated = true;
        }
    }

    MakeLogSnapshot();

    if (loadedState->ShouldSnapshotScheme) {
        TTxStamp stamp = Stamp();
        auto alter = Database->GetScheme().GetSnapshot();
        alter->SetRewrite(true);
        auto change = alter->SerializeAsString();
        Database->RollUp(stamp, change, {}, {});
        auto commit = CommitManager->Begin(true, ECommit::Misc, {});
        LogicAlter->Clear();
        LogicAlter->WriteLog(*commit, std::move(change));
        CommitManager->Commit(commit);
    }

    if (auto error = CheckBorrowConsistency()) {
        if (auto logl = Logger->Log(ELnLev::Crit))
            logl << NFmt::Do(*this) << " Borrow consistency failed: " << error;
    }

    PlanTransactionActivation();

    Owner->ActivateExecutor(OwnerCtx());

    UpdateCounters(ctx);
}

void TExecutor::TranscriptBootOpResult(ui32 res, const TActorContext &ctx) {
    switch (res) {
    case TExecutorBootLogic::OpResultUnhandled:
        return; // do nothing?
    case TExecutorBootLogic::OpResultContinue:
        return; // do nothing.
    case TExecutorBootLogic::OpResultComplete:
        return Active(ctx);
    case TExecutorBootLogic::OpResultBroken:
        if (auto logl = Logger->Log(ELnLev::Error)) {
            logl << NFmt::Do(*this) << " Broken while booting";
        }

        return Broken();
    default:
        Y_TABLET_ERROR("unknown boot result");
    }
}

void TExecutor::TranscriptFollowerBootOpResult(ui32 res, const TActorContext &ctx) {
    switch (res) {
    case TExecutorBootLogic::OpResultUnhandled:
        return;
    case TExecutorBootLogic::OpResultContinue:
        return;
    case TExecutorBootLogic::OpResultComplete:
        return ActivateFollower(ctx);
    case TExecutorBootLogic::OpResultBroken:
        if (auto logl = Logger->Log(ELnLev::Error)) {
            logl << NFmt::Do(*this) << " Broken while follower booting";
        }

        return Broken();
    default:
        Y_TABLET_ERROR("unknown boot result");
    }
}

std::unique_ptr<TSeat> TExecutor::RemoveTransaction(ui64 id) {
    auto it = Transactions.find(id);
    Y_ENSURE(it != Transactions.end(), "Cannot find transaction " << id);
    auto res = std::move(it->second);
    res->State = ESeatState::Done;
    Transactions.erase(it);
    return res;
}

void TExecutor::FinishCancellation(TSeat* seat, bool activateMore) {
    UnpinTransactionPages(*seat);
    Memory->ReleaseMemory(*seat);
    --Stats->TxInFly;
    Counters->Simple()[TExecutorCounters::DB_TX_IN_FLY] = Stats->TxInFly;
    if (AppTxCounters && seat->TxType != UnknownTxType) {
        AppTxCounters->TxSimple(seat->TxType, COUNTER_TT_INFLY) -= 1;
    }
    RemoveTransaction(seat->UniqID);
    if (activateMore) {
        PlanTransactionActivation();
        MaybeRelaxRejectProbability();
    }
}

void TExecutor::EnqueueActivation(TSeat* seat, bool activate) {
    LWTRACK(TransactionEnqueued, seat->Self->Orbit, seat->UniqID);
    seat->StartEnqueuedSpan();
    if (!seat->LowPriority) {
        seat->State = ESeatState::Active;
        ActivationQueue.PushBack(seat);
        ActivateTransactionWaiting++;
        if (activate && ActivateTransactionInFlight < ActivateTransactionWaiting) {
            Send(SelfId(), new TEvPrivate::TEvActivateExecution());
            ++ActivateTransactionInFlight;
        }
    } else {
        seat->State = ESeatState::ActiveLow;
        ActivationLowQueue.PushBack(seat);
        ActivateLowTransactionWaiting++;
        if (activate && ActivateLowTransactionInFlight < 1) {
            Send(SelfId(), new TEvPrivate::TEvActivateLowExecution());
            ++ActivateLowTransactionInFlight;
        }
    }
}

void TExecutor::PlanTransactionActivation() {
    if (!CanExecuteTransaction())
        return;

    const ui64 limitTxInFly = Scheme().Executor.LimitInFlyTx;
    while (PendingQueue && (!limitTxInFly || (Stats->TxInFly - Stats->TxPending < limitTxInFly))) {
        TSeat* seat = PendingQueue.PopFront();
        Y_ENSURE(seat->State == ESeatState::Pending);
        seat->State = ESeatState::None;
        seat->FinishPendingSpan();
        --Stats->TxPending;
        EnqueueActivation(seat, false);
    }

    while (ActivateTransactionInFlight < ActivateTransactionWaiting) {
        Send(SelfId(), new TEvPrivate::TEvActivateExecution());
        ++ActivateTransactionInFlight;
    }

    while (ActivateLowTransactionWaiting > 0 && ActivateLowTransactionInFlight < 1) {
        Send(SelfId(), new TEvPrivate::TEvActivateLowExecution());
        ++ActivateLowTransactionInFlight;
    }
}

void TExecutor::TryActivateWaitingTransaction(TIntrusivePtr<NPageCollection::TPagesWaitPad>&& waitPad) {
    Y_ENSURE(waitPad);
    Y_ENSURE(waitPad->PendingRequests);
    if (--waitPad->PendingRequests) {
        LogWaitingTransaction(std::move(waitPad));
    } else {
        ActivateWaitingTransaction(std::move(waitPad));
    }
}

void TExecutor::ActivateWaitingTransaction(TIntrusivePtr<NPageCollection::TPagesWaitPad>&& waitPad) {
    bool activate = CanExecuteTransaction();
    bool cancelled = false;
    if (auto it = TransactionWaitPads.find(waitPad.Get()); it != TransactionWaitPads.end()) {
        it->second->WaitingSpan.EndOk();
        TSeat* seat = it->second->Seat;
        Y_ENSURE(seat->State == ESeatState::Waiting);
        seat->State = ESeatState::None;
        TransactionWaitPads.erase(it);
        if (seat->Cancelled) {
            FinishCancellation(seat, false);
            cancelled = true;
            if (auto logl = Logger->Log(ELnLev::Debug)) {
                logl << NFmt::Do(*this) << " " << NFmt::Do(*seat) << " cancelled";
            }
        } else {
            EnqueueActivation(seat, activate);
            if (auto logl = Logger->Log(ELnLev::Debug)) {
                logl << NFmt::Do(*this) << " " << NFmt::Do(*seat) << " activated";
            }
        }
    }
    if (cancelled && activate) {
        PlanTransactionActivation();
        MaybeRelaxRejectProbability();
    }
}

void TExecutor::LogWaitingTransaction(TIntrusivePtr<NPageCollection::TPagesWaitPad>&& waitPad) {
    if (auto it = TransactionWaitPads.find(waitPad.Get()); it != TransactionWaitPads.end()) {
        TSeat* seat = it->second->Seat;
        if (auto logl = Logger->Log(ELnLev::Debug)) {
            logl << NFmt::Do(*this) << " " << NFmt::Do(*seat) << " still waiting " << waitPad->PendingRequests << " requests";
        }
    }
}

void TExecutor::AddCachesOfBundle(const NTable::TPartView &partView)
{
    auto *partStore = partView.As<NTable::TPartStore>();

    {
        ui32 room = 0;

        while (auto *pack = partStore->Packet(room++))
            Stats->PacksMetaBytes += pack->Meta.Raw.size();
    }

    for (auto &cache : partStore->PageCollections)
        AddSingleCache(cache);

    if (const auto &blobs = partStore->Pseudo)
        AddSingleCache(blobs);
}

void TExecutor::AddSingleCache(const TIntrusivePtr<TPrivatePageCache::TInfo> &info)
{
    PrivatePageCache->RegisterPageCollection(info);
    // TODO: handle different cache modes
    Send(MakeSharedPageCacheId(), new NSharedCache::TEvAttach(info->PageCollection, ECacheMode::Regular));

    StickyPagesMemory += info->GetStickySize();

    Counters->Simple()[TExecutorCounters::CACHE_TOTAL_STICKY] = StickyPagesMemory;
}

void TExecutor::DropCachesOfBundle(const NTable::TPart &part)
{
    auto *partStore = CheckedCast<const NTable::TPartStore*>(&part);

    {
        ui32 room = 0;

        while (auto *pack = partStore->Packet(room++))
            NUtil::SubSafe(Stats->PacksMetaBytes, ui64(pack->Meta.Raw.size()));
    }

    for (auto &cache : partStore->PageCollections)
        DropSingleCache(cache->Id);

    if (const auto &blobs = partStore->Pseudo)
        DropSingleCache(blobs->Id);
}

void TExecutor::DropSingleCache(const TLogoBlobID &label)
{
    auto pageCollection = PrivatePageCache->GetPageCollection(label);

    ui64 stickySize = pageCollection->GetStickySize();
    Y_ENSURE(StickyPagesMemory >= stickySize);
    StickyPagesMemory -= stickySize;

    PrivatePageCache->ForgetPageCollection(pageCollection);

    // Note: Shared Cache will send TEvResult with NKikimrProto::RACE status
    // it activates all transactions that are waiting for being dropped page collection
    Send(MakeSharedPageCacheId(), new NSharedCache::TEvDetach(label));

    Counters->Simple()[TExecutorCounters::CACHE_PINNED_SET] = PrivatePageCache->GetStats().PinnedSetSize;
    Counters->Simple()[TExecutorCounters::CACHE_PINNED_LOAD] = PrivatePageCache->GetStats().PinnedLoadSize;
    Counters->Simple()[TExecutorCounters::CACHE_TOTAL_STICKY] = StickyPagesMemory;
}

void TExecutor::TranslateCacheTouchesToSharedCache() {
    auto touches = PrivatePageCache->GetPrepareSharedTouched();
    if (touches.empty())
        return;
    Send(MakeSharedPageCacheId(), new NSharedCache::TEvTouch(std::move(touches)));
}

void TExecutor::RequestInMemPagesForDatabase(bool pendingOnly) {
    const auto& scheme = Scheme();
    for (auto& pr : scheme.Tables) {
        const ui32 tid = pr.first;
        if (pendingOnly && !pr.second.PendingCacheEnable) {
            continue;
        }
        auto stickyColumns = GetStickyColumns(tid);
        if (stickyColumns) {
            auto subset = Database->Subset(tid, NTable::TEpoch::Max(), { } , { });

            for (auto &partView: subset->Flatten)
                RequestInMemPagesForPartStore(tid, partView, stickyColumns);
        }
        pr.second.PendingCacheEnable = false;
    }
}

void TExecutor::StickInMemPages(NSharedCache::TEvResult *msg) {
    const auto& scheme = Scheme();
    for (auto& pr : scheme.Tables) {
        const ui32 tid = pr.first;
        auto subset = Database->Subset(tid, NTable::TEpoch::Max(), { } , { });
        for (auto &partView : subset->Flatten) {
            auto partStore = partView.As<NTable::TPartStore>();
            for (auto &pageCollection : partStore->PageCollections) {
                // Note: page collection search optimization seems useless
                if (pageCollection->PageCollection == msg->PageCollection) {
                    ui64 stickySizeBefore = pageCollection->GetStickySize();
                    for (auto& loaded : msg->Pages) {
                        pageCollection->AddSticky(loaded.PageId, loaded.Page);
                    }
                    StickyPagesMemory += pageCollection->GetStickySize() - stickySizeBefore;
                }
            }
        }
    }
    // Note: the next call of ProvideBlock will also fill pages bodies
}

TExecutorCaches TExecutor::CleanupState() {
    TExecutorCaches caches;

    if (BootLogic) {
        BootLogic->Cancel();
        caches = BootLogic->DetachCaches();
    } else {
        if (PrivatePageCache) {
            caches.PageCaches = PrivatePageCache->DetachPrivatePageCache();
        }
        if (Database) {
            Database->EnumerateTxStatusParts([&caches](const TIntrusiveConstPtr<NTable::TTxStatusPart>& txStatus) {
                caches.TxStatusCaches[txStatus->Label] = txStatus->TxStatusPage->GetRaw();
            });
        }
    }

    BootLogic.Destroy();
    PendingBlobQueue.Clear();
    PostponedFollowerUpdates.clear();
    PendingPartSwitches.clear();
    ReadyPartSwitches = 0;
    Y_ENSURE(!LogicRedo);
    Database.Destroy();
    Y_ENSURE(!GcLogic);
    Y_ENSURE(!LogicAlter);
    Y_ENSURE(!CompactionLogic);
    BorrowLogic.Destroy();
    VacuumLogic.Destroy();

    return caches;
}

void TExecutor::Boot(TEvTablet::TEvBoot::TPtr &ev, const TActorContext &ctx) {
    if (Stats->IsFollower()) {
        TabletCountersForgetTablet(Owner->TabletID(), Owner->TabletType(),
            Owner->Info()->TenantPathId, Stats->IsFollower(), SelfId());
    }

    if (!Counters) {
        Counters = MakeHolder<TExecutorCounters>();
        CountersBaseline = MakeHolder<TExecutorCounters>();
        Counters->RememberCurrentStateAsBaseline(*CountersBaseline);
    }

    RegisterTabletFlatProbes();

    Become(&TThis::StateBoot);
    Stats->IsActive = false;
    Stats->FollowerId = 0;

    TEvTablet::TEvBoot *msg = ev->Get();
    Generation0 = msg->Generation;
    Step0 = 0;
    Launcher = msg->Launcher;
    Memory->SetProfiles(msg->ResourceProfiles);

    const ui64 maxBootBytesInFly = 12 * 1024 * 1024;

    auto executorCaches = CleanupState();

    BootLogic.Reset(new TExecutorBootLogic(this, SelfId(), Owner->Info(), maxBootBytesInFly));

    ProcessIoStats(
        NBlockIO::EDir::Read, NBlockIO::EPriority::Fast,
        std::move(msg->GroupReadBytes),
        std::move(msg->GroupReadOps),
        ctx);

    const auto res = BootLogic->ReceiveBoot(ev, std::move(executorCaches));
    return TranscriptBootOpResult(res, ctx);
}

void TExecutor::FollowerBoot(TEvTablet::TEvFBoot::TPtr &ev, const TActorContext &ctx) {
    Y_ENSURE(CurrentStateFunc() == &TThis::StateInit
        || CurrentStateFunc() == &TThis::StateFollowerBoot
        || CurrentStateFunc() == &TThis::StateFollower);

    if (!Counters) {
        Counters = MakeHolder<TExecutorCounters>();
        CountersBaseline = MakeHolder<TExecutorCounters>();
        Counters->RememberCurrentStateAsBaseline(*CountersBaseline);
    }

    RegisterTabletFlatProbes();

    Become(&TThis::StateFollowerBoot);

    TEvTablet::TEvFBoot *msg = ev->Get();
    Generation0 = msg->Generation;
    Step0 = 0;
    Launcher = msg->Launcher;
    Memory->SetProfiles(msg->ResourceProfiles);
    FollowerId = msg->FollowerID;

    Stats->IsActive = false;
    Stats->FollowerId = FollowerId;

    const ui64 maxBootBytesInFly = 12 * 1024 * 1024;

    auto executorCaches = CleanupState();

    BootLogic.Reset(new TExecutorBootLogic(this, SelfId(), Owner->Info(), maxBootBytesInFly));

    ProcessIoStats(
        NBlockIO::EDir::Read, NBlockIO::EPriority::Fast,
        std::move(msg->GroupReadBytes),
        std::move(msg->GroupReadOps),
        ctx);

    const auto res = BootLogic->ReceiveFollowerBoot(ev, std::move(executorCaches));
    return TranscriptFollowerBootOpResult(res, ctx);
}

void TExecutor::Restored(TEvTablet::TEvRestored::TPtr &ev, const TActorContext &ctx) {
    Y_ENSURE(CurrentStateFunc() == &TThis::StateBoot && BootLogic);

    TEvTablet::TEvRestored *msg = ev->Get();
    Y_ENSURE(Generation() == msg->Generation);

    const TExecutorBootLogic::EOpResult res = BootLogic->ReceiveRestored(ev);
    return TranscriptBootOpResult(res, ctx);
}

void TExecutor::DetachTablet() {
    TabletCountersForgetTablet(Owner->TabletID(), Owner->TabletType(),
        Owner->Info()->TenantPathId, Stats->IsFollower(), SelfId());
    return PassAway();
}

void TExecutor::FollowerUpdate(THolder<TEvTablet::TFUpdateBody> upd) {
    if (BootLogic) {
        Y_ENSURE(CurrentStateFunc() == &TThis::StateFollowerBoot);
        PostponedFollowerUpdates.emplace_back(std::move(upd));
    } else if (PendingPartSwitches) {
        Y_ENSURE(CurrentStateFunc() == &TThis::StateFollower);
        PostponedFollowerUpdates.emplace_back(std::move(upd));
    } else {
        Y_ENSURE(CurrentStateFunc() == &TThis::StateFollower);
        Y_ENSURE(PostponedFollowerUpdates.empty());
        ApplyFollowerUpdate(std::move(upd));
    }
}

void TExecutor::FollowerAuxUpdate(TString upd) {
    if (BootLogic) {
        Y_ENSURE(CurrentStateFunc() == &TThis::StateFollowerBoot);
        PostponedFollowerUpdates.emplace_back(new TEvTablet::TFUpdateBody(std::move(upd)));
    } else if (PendingPartSwitches) {
        Y_ENSURE(CurrentStateFunc() == &TThis::StateFollower);
        PostponedFollowerUpdates.emplace_back(new TEvTablet::TFUpdateBody(std::move(upd)));
    } else {
        Y_ENSURE(CurrentStateFunc() == &TThis::StateFollower);
        Y_ENSURE(PostponedFollowerUpdates.empty());
        ApplyFollowerAuxUpdate(upd);
    }
}

void TExecutor::FollowerAttached(ui32 totalFollowers) {
    Stats->FollowersCount = totalFollowers;
    NeedFollowerSnapshot = true;

    if (CurrentStateFunc() != &TThis::StateWork)
        return;

    MakeLogSnapshot();

    Owner->OnFollowersCountChanged();
}

void TExecutor::FollowerDetached(ui32 totalFollowers) {
    Stats->FollowersCount = totalFollowers;

    if (CurrentStateFunc() != &TThis::StateWork)
        return;

    Owner->OnFollowersCountChanged();
}

void TExecutor::FollowerSyncComplete() {
    Y_ENSURE(CurrentStateFunc() == &TThis::StateWork || CurrentStateFunc() == &TThis::StateBoot);
    if (GcLogic)
        GcLogic->FollowersSyncComplete(false);
    else if (BootLogic)
        BootLogic->FollowersSyncComplete();
    else
        Y_TABLET_ERROR("must not happens");
}

void TExecutor::FollowerGcApplied(ui32 step, TDuration followerSyncDelay) {
    if (auto logl = Logger->Log(ELnLev::Debug)) {
        logl << NFmt::Do(*this) << " switch applied on followers, step " << step;
    }

    auto it = InFlyCompactionGcBarriers.find(step);
    Y_ENSURE(it != InFlyCompactionGcBarriers.end());
    CheckCollectionBarrier(it->second);
    InFlyCompactionGcBarriers.erase(it);

    if (followerSyncDelay != TDuration::Max())
        Counters->Percentile()[TExecutorCounters::TX_PERCENTILE_FOLLOWERSYNC_LATENCY].IncrementFor(followerSyncDelay.MicroSeconds());
}

void TExecutor::CheckCollectionBarrier(TIntrusivePtr<TBarrier> &barrier) {
    if (barrier && barrier->RefCount() == 1) {
        GcLogic->ReleaseBarrier(barrier->Step);
        if (BorrowLogic->SetGcBarrier(GcLogic->GetActiveGcBarrier())) {
            // N.B. PassAway may have already been called
            if (Owner) {
                Owner->CompletedLoansChanged(OwnerCtx());
            }
        }
        if (VacuumLogic->NeedGC()) {
            GcLogic->SendCollectGarbage(ActorContext());
        }
    }

    barrier.Drop();
}

void TExecutor::ApplyFollowerPostponedUpdates() {
    while (PostponedFollowerUpdates && !PendingPartSwitches) {
        THolder<TEvTablet::TFUpdateBody> upd = std::move(PostponedFollowerUpdates.front());
        PostponedFollowerUpdates.pop_front();

        if (upd->Step) {
            ApplyFollowerUpdate(std::move(upd));
        } else {
            ApplyFollowerAuxUpdate(upd->AuxPayload);
        }
    }
}

void TExecutor::ApplyFollowerUpdate(THolder<TEvTablet::TFUpdateBody> update) {
    if (update->Step <= Step0 || CommitManager) {
        Y_TABLET_ERROR(
            NFmt::Do(*this) << " got unexpected follower update to Step "
            << update->Step << ", " << NFmt::If(CommitManager.Get()));
    }

    Step0 = update->Step;

    if (update->IsSnapshot) // do nothing over snapshot after initial one
        return;

    // Protect against recursive transactions in callbacks
    TActiveTransactionZone activeTransaction(this);

    TString schemeUpdate;
    TString dataUpdate;
    TStackVec<TString> partSwitches;
    TStackVec<TLogoBlob> loanPartInfos;
    TVector<NPageCollection::TMemGlob> annex;

    if (update->EmbeddedBody) { // we embed only regular updates
        Y_ENSURE(update->References.empty());
        dataUpdate = update->EmbeddedBody;
    } else {
        for (auto &xpair : update->References) {
            const TLogoBlobID &id = xpair.first;
            const TString &body = xpair.second;

            const NBoot::TCookie cookie(id.Cookie());
            Y_ENSURE(cookie.Type() == NBoot::TCookie::EType::Log);

            if (NBoot::TCookie::CookieRangeRaw().Has(cookie.Raw)) {
                auto group = Owner->Info()->GroupFor(id.Channel(), id.Generation());

                annex.emplace_back(NPageCollection::TGlobId{ id, group }, TSharedData::Copy(body));

                continue;
            }

            switch (cookie.Index()) {
            case NBoot::TCookie::EIdx::RedoLz4:
                if (dataUpdate)
                    dataUpdate.append(body);
                else
                    dataUpdate = body;
                break;
            case NBoot::TCookie::EIdx::Alter:
                if (schemeUpdate)
                    schemeUpdate.append(body);
                else
                    schemeUpdate = body;
                break;
            case NBoot::TCookie::EIdx::TurnLz4:
                partSwitches.push_back(body);
                break;
            case NBoot::TCookie::EIdx::Loan:
                loanPartInfos.push_back(TLogoBlob(id, body));
                break;
            case NBoot::TCookie::EIdx::GCExt:
                // ignore
                break;
            default:
                Y_TABLET_ERROR("unsupported blob kind");
            }
        }
    }

    if (schemeUpdate || dataUpdate || annex) {
        if (dataUpdate)
            dataUpdate = NPageCollection::TSlicer::Lz4()->Decode(dataUpdate);

        for (auto &subset : Database->RollUp(Stamp(), schemeUpdate, dataUpdate, annex))
            for (auto &partView: subset->Flatten)
                DropCachesOfBundle(*partView);

        if (schemeUpdate) {
            ReadResourceProfile();
            ReflectSchemeSettings();
            RequestInMemPagesForDatabase(/* pendingOnly */ true);
            Owner->OnFollowerSchemaUpdated();
        }

        if (dataUpdate) {
            Owner->OnFollowerDataUpdated();
        }
    }

    for (const TLogoBlob &loanQu : loanPartInfos) {
        const TString uncompressed = NPageCollection::TSlicer::Lz4()->Decode(loanQu.Buffer);

        TProtoBox<NKikimrExecutorFlat::TBorrowedPart> proto(uncompressed);

        // for now follower borrowed info is not cleared.
        // it's not problem as by design we expect limited number of loans
        BorrowLogic->RestoreFollowerBorrowedInfo(loanQu.Id, proto);
    }

    if (partSwitches) {
        NKikimrExecutorFlat::TFollowerPartSwitchAux auxProto;

        if (update->AuxPayload) {
            const TString auxBody = NPageCollection::TSlicer::Lz4()->Decode(update->AuxPayload);
            Y_ENSURE(auxProto.ParseFromString(auxBody));
            Y_ENSURE(auxProto.BySwitchAuxSize() <= partSwitches.size());
        }

        bool hadPendingPartSwitches = bool(PendingPartSwitches);

        ui32 nextAuxIdx = 0;
        for (ui32 idx : xrange(partSwitches.size())) {
            const TString uncompressed = NPageCollection::TSlicer::Lz4()->Decode(partSwitches[idx]);

            const TProtoBox<NKikimrExecutorFlat::TTablePartSwitch> proto(uncompressed);

            const NKikimrExecutorFlat::TFollowerPartSwitchAux::TBySwitch *aux = nullptr;
            if (proto.HasIntroducedParts() || proto.HasIntroducedTxStatus()) {
                Y_ENSURE(nextAuxIdx < auxProto.BySwitchAuxSize());
                aux = &auxProto.GetBySwitchAux(nextAuxIdx++);
            }

            const ui32 followerGcStep = update->NeedFollowerGcAck ? Step0 : 0;
            AddFollowerPartSwitch(proto, aux, followerGcStep, Step0);

            // Row version changes are rolled up immediately (similar to schema changes)
            if (proto.HasRowVersionChanges()) {
                const auto& changes = proto.GetRowVersionChanges();
                const ui32 tableId = changes.GetTable();

                if (Y_LIKELY(Scheme().Tables.contains(tableId))) {
                    for (const auto& range : changes.GetRemovedRanges()) {
                        const TRowVersion lower(range.GetLower().GetStep(), range.GetLower().GetTxId());
                        const TRowVersion upper(range.GetUpper().GetStep(), range.GetUpper().GetTxId());
                        Database->RollUpRemoveRowVersions(tableId, lower, upper);
                    }
                }
            }
        }

        if (!hadPendingPartSwitches) {
            ApplyReadyPartSwitches(); // safe to apply switches right now
        }
    } else if (update->NeedFollowerGcAck) {
        Send(Owner->Tablet(), new TEvTablet::TEvFGcAck(Owner->TabletID(), Generation(), Step0));
    }
}

void TExecutor::ApplyFollowerAuxUpdate(const TString &auxBody) {
    const TString aux = NPageCollection::TSlicer::Lz4()->Decode(auxBody);
    TProtoBox<NKikimrExecutorFlat::TFollowerAux> proto(aux);

    if (proto.HasUserAuxUpdate()) {
        TActiveTransactionZone activeTransaction(this);
        Owner->OnLeaderUserAuxUpdate(std::move(proto.GetUserAuxUpdate()));
    }
}

void TExecutor::RequestFromSharedCache(TAutoPtr<NPageCollection::TFetch> fetch,
    NBlockIO::EPriority priority,
    ESharedCacheRequestType requestType)
{
    Y_ENSURE(fetch->Pages.size() > 0, "Got TFetch req w/o any page");

    Send(MakeSharedPageCacheId(), new NSharedCache::TEvRequest(
        priority,
        fetch),
        0, (ui64)requestType);
}

void TExecutor::AddFollowerPartSwitch(
        const NKikimrExecutorFlat::TTablePartSwitch &switchProto,
        const NKikimrExecutorFlat::TFollowerPartSwitchAux::TBySwitch *aux,
        ui32 updateStep, ui32 step)
{
    auto& partSwitch = PendingPartSwitches.emplace_back();
    partSwitch.FollowerUpdateStep = updateStep;
    partSwitch.TableId = switchProto.GetTableId();
    partSwitch.Step = step;

    if (switchProto.HasIntroducedParts() && switchProto.GetIntroducedParts().BundlesSize()) {
        Y_ENSURE(aux && aux->HotBundlesSize() == switchProto.GetIntroducedParts().BundlesSize());
        for (auto x : xrange(aux->HotBundlesSize())) {
            NTable::TPartComponents c = TPageCollectionProtoHelper::MakePageCollectionComponents(aux->GetHotBundles(x));
            PrepareExternalPart(partSwitch, std::move(c));
        }
    }

    if (switchProto.HasIntroducedTxStatus()) {
        Y_ENSURE(aux && aux->HotTxStatusSize() == switchProto.GetIntroducedTxStatus().TxStatusSize());
        for (const auto &x : aux->GetHotTxStatus()) {
            auto dataId = TLargeGlobIdProto::Get(x.GetDataId());
            auto epoch = NTable::TEpoch(x.GetEpoch());
            const TString &data = x.GetData();
            PrepareExternalTxStatus(partSwitch, dataId, epoch, data);
        }
    }

    if (switchProto.HasTableSnapshoted())
        partSwitch.Head = NTable::TEpoch(switchProto.GetTableSnapshoted().GetHead());

    partSwitch.Changed.reserve(switchProto.ChangedBundlesSize());
    for (auto &x : switchProto.GetChangedBundles()) {
        auto &change = partSwitch.Changed.emplace_back();
        change.Label = LogoBlobIDFromLogoBlobID(x.GetLabel());
        if (x.HasLegacy())
            change.Legacy = x.GetLegacy();
        if (x.HasOpaque())
            change.Opaque = x.GetOpaque();
    }

    partSwitch.Deltas.reserve(switchProto.BundleDeltasSize());
    for (auto &x : switchProto.GetBundleDeltas()) {
        auto &delta = partSwitch.Deltas.emplace_back();
        delta.Label = LogoBlobIDFromLogoBlobID(x.GetLabel());
        if (x.HasDelta()) {
            delta.Delta = x.GetDelta();
        }
    }

    for (auto &x : switchProto.GetLeavingBundles())
        partSwitch.Leaving.push_back(LogoBlobIDFromLogoBlobID(x));

    for (auto &x : switchProto.GetLeavingTxStatus())
        partSwitch.LeavingTxStatus.push_back(LogoBlobIDFromLogoBlobID(x));

    partSwitch.Moves.reserve(switchProto.BundleMovesSize());
    for (auto &x : switchProto.GetBundleMoves()) {
        auto &move = partSwitch.Moves.emplace_back();
        move.Label = LogoBlobIDFromLogoBlobID(x.GetLabel());
        if (x.HasRebasedEpoch()) {
            move.RebasedEpoch = NTable::TEpoch(x.GetRebasedEpoch());
        }
        if (x.HasSourceTable()) {
            move.SourceTable = x.GetSourceTable();
        }
    }
}

bool TExecutor::PrepareExternalPart(TPendingPartSwitch &partSwitch, NTable::TPartComponents &&pc) {
    Y_ENSURE(pc);

    const ui32 tableId = partSwitch.TableId;
    const auto& dbScheme = Database->GetScheme();
    const auto& tableScheme = dbScheme.Tables.at(tableId);

    if (tableScheme.ColdBorrow && !partSwitch.FollowerUpdateStep) {
        const auto label = pc.PageCollectionComponents.at(0).LargeGlobId.Lead;
        if (label.TabletID() != TabletId()) {
            TVector<NPageCollection::TLargeGlobId> largeGlobIds(Reserve(pc.PageCollectionComponents.size()));
            for (const auto& c : pc.PageCollectionComponents) {
                largeGlobIds.push_back(c.LargeGlobId);
            }
            TIntrusiveConstPtr<NTable::TColdPart> part = new NTable::TColdPartStore(
                std::move(largeGlobIds),
                std::move(pc.Legacy),
                std::move(pc.Opaque),
                pc.GetEpoch());
            partSwitch.NewColdParts.push_back(std::move(part));
            return false;
        }
    }

    auto &bundle = partSwitch.NewBundles.emplace_back(std::move(pc));

    return PrepareExternalPart(partSwitch, bundle);
}

bool TExecutor::PrepareExternalPart(TPendingPartSwitch &partSwitch, TPendingPartSwitch::TNewBundle &bundle) {
    if (auto* stage = bundle.GetStage<TPendingPartSwitch::TMetaStage>()) {
        if (!stage->Finished()) {
            // N.B. this should only happen at most once per bundle
            for (auto it = stage->Loaders.begin(); it != stage->Loaders.end(); ++it) {
                auto group = it->LargeGlobId.Group;
                for (const TLogoBlobID& id : it->State.GetBlobs()) {
                    if (partSwitch.AddPendingBlob(id, TPendingPartSwitch::TBlobWaiter{ &bundle, it })) {
                        // First time we see this blob, enqueue the fetch operation
                        PendingBlobQueue.Enqueue(id, group, this, reinterpret_cast<uintptr_t>(&partSwitch));
                    }
                }
            }
            PendingBlobQueue.SendRequests(SelfId());
            return true;
        }

        auto pc = std::move(stage->PartComponents);
        bundle.Stage.emplace<TPendingPartSwitch::TLoaderStage>(std::move(pc));
    }

    if (auto* stage = bundle.GetStage<TPendingPartSwitch::TLoaderStage>()) {
        if (auto fetch = stage->Loader.Run({.PreloadIndex = true, .PreloadData = PreloadTablesData.contains(partSwitch.TableId)})) {
            Y_ENSURE(fetch.size() == 1, "Cannot handle loads from more than one page collection");

            for (auto req : fetch) {
                stage->Fetching = req->PageCollection.Get();
                RequestFromSharedCache(req, NBlockIO::EPriority::Fast, ESharedCacheRequestType::PendingInit);
            }

            ++partSwitch.PendingLoads;
            return true;
        }

        auto partView = stage->Loader.Result();
        bundle.Stage.emplace<TPendingPartSwitch::TResultStage>(std::move(partView));
        return false;
    }

    Y_TABLET_ERROR("Unexpected PrepareExternalPart called");
}

bool TExecutor::PrepareExternalTxStatus(
        TPendingPartSwitch &partSwitch,
        const NPageCollection::TLargeGlobId &dataId,
        NTable::TEpoch epoch,
        const TString &data)
{
    auto &txStatus = partSwitch.NewTxStatus.emplace_back(dataId, epoch, data);

    return PrepareExternalTxStatus(partSwitch, txStatus);
}

bool TExecutor::PrepareExternalTxStatus(TPendingPartSwitch &partSwitch, TPendingPartSwitch::TNewTxStatus &txStatus) {
    if (auto* stage = txStatus.GetStage<TPendingPartSwitch::TTxStatusLoadStage>()) {
        if (!stage->Finished()) {
            auto group = stage->Loader->LargeGlobId.Group;
            for (const TLogoBlobID& id : stage->Loader->State.GetBlobs()) {
                if (partSwitch.AddPendingBlob(id, TPendingPartSwitch::TBlobWaiter{ &txStatus })) {
                    // First time we see this blob, enqueue the fetch operation
                    PendingBlobQueue.Enqueue(id, group, this, reinterpret_cast<uintptr_t>(&partSwitch));
                }
            }
            PendingBlobQueue.SendRequests(SelfId());
            return true;
        }

        auto result = std::move(stage->TxStatus);
        txStatus.Stage.emplace<TPendingPartSwitch::TTxStatusResultStage>(std::move(result));
        return false;
    }

    Y_TABLET_ERROR("Unexpected PrepareExternalTxStatus call");
}

void TExecutor::OnBlobLoaded(const TLogoBlobID& id, TString body, uintptr_t cookie) {
    auto& partSwitch = *reinterpret_cast<TPendingPartSwitch*>(cookie);

    const auto p = partSwitch.PendingBlobs.equal_range(id);

    TStackVec<TPendingPartSwitch::TBlobWaiter> waiters;
    for (auto it = p.first; it != p.second; ++it) {
        waiters.push_back(std::move(it->second));
    }
    partSwitch.PendingBlobs.erase(p.first, p.second);

    bool waiting = false;

    for (auto& waiter : waiters) {
        if (auto* r = waiter.GetWaiter<TPendingPartSwitch::TNewBundleWaiter>()) {
            auto* stage = r->Bundle->GetStage<TPendingPartSwitch::TMetaStage>();
            Y_ENSURE(stage && !stage->Finished(),
                "Loaded blob " << id << " for a bundle in an unexpected state");
            if (stage->Accept(r->Loader, id, body)) {
                Y_ENSURE(stage->Finished());
                waiting |= PrepareExternalPart(partSwitch, *r->Bundle);
            }
            continue;
        }
        if (auto* r = waiter.GetWaiter<TPendingPartSwitch::TNewTxStatusWaiter>()) {
            auto* stage = r->TxStatus->GetStage<TPendingPartSwitch::TTxStatusLoadStage>();
            Y_ENSURE(stage && !stage->Finished(),
                "Loaded blob " << id << " for a tx status in an unexpected state");
            if (stage->Accept(id, body)) {
                Y_ENSURE(stage->Finished());
                waiting |= PrepareExternalTxStatus(partSwitch, *r->TxStatus);
            }
            continue;
        }
        Y_TABLET_ERROR("Loaded blob " << id << " for an unsupported waiter");
    }

    PendingBlobQueue.SendRequests(SelfId());

    if (!waiting) {
        AdvancePendingPartSwitches();
    }
}

void TExecutor::Handle(TEvBlobStorage::TEvGetResult::TPtr& ev, const TActorContext&) {
    if (!PendingBlobQueue.ProcessResult(ev->Get())) {
        if (auto logl = Logger->Log(ELnLev::Error)) {
            logl << NFmt::Do(*this) << " Broken while loading blobs";
        }

        return Broken();
    }
}

void TExecutor::Handle(TEvTablet::TEvGcForStepAckResponse::TPtr &ev) {
    if (ev->Get()->Generation != Generation()) {
        return;
    }

    VacuumLogic->OnGcForStepAckResponse(Generation(), ev->Get()->Step, OwnerCtx());
}

void TExecutor::AdvancePendingPartSwitches() {
    while (PendingPartSwitches && ApplyReadyPartSwitches()) {
        if (Stats->IsFollower()) {
            ApplyFollowerPostponedUpdates();
        }
    }

    // could be border change
    if (PendingPartSwitches.empty()) {
        PlanTransactionActivation();
        MaybeRelaxRejectProbability();

        // Note: followers don't have VacuumLogic
        if (NeedFollowerSnapshot || VacuumLogic && VacuumLogic->NeedLogSnaphot()) {
            MakeLogSnapshot();
        }
    }
}

bool TExecutor::ApplyReadyPartSwitches() {
    while (PendingPartSwitches) {
        auto step = PendingPartSwitches.front().Step;
        auto last = PendingPartSwitches.begin() + ReadyPartSwitches;
        while (last != PendingPartSwitches.end() && !last->PendingBlobs && !last->PendingLoads) {
            ++ReadyPartSwitches;
            ++last;
        }

        if (last != PendingPartSwitches.end() && last->Step == step) {
            return false; // some switch is not ready at this step
        }

        // Atomically update part switches related to a single step
        while (ReadyPartSwitches > 0 && PendingPartSwitches.front().Step == step) {
            ApplyExternalPartSwitch(PendingPartSwitches.front());
            PendingPartSwitches.pop_front();
            --ReadyPartSwitches;
        }
    }

    return true;
}

void TExecutor::RequestInMemPagesForPartStore(ui32 tableId, const NTable::TPartView &partView, const THashSet<NTable::TTag> &stickyColumns) {
    Y_DEBUG_ABORT_UNLESS(stickyColumns);

    auto rowScheme = RowScheme(tableId);

    for (size_t groupIndex : xrange(partView->GroupsCount)) {
        bool stickyGroup = false;
        for (const auto &column : partView->Scheme->Groups[groupIndex].Columns) {
            if (stickyColumns.contains(column.Tag)) {
                stickyGroup = true;
                break;
            }
        }

        if (stickyGroup) {
            auto req = partView.As<NTable::TPartStore>()->GetPages(groupIndex);
            // TODO: only request missing pages
            RequestFromSharedCache(req, NBlockIO::EPriority::Bkgr, ESharedCacheRequestType::InMemPages);
        }
    }
}

THashSet<NTable::TTag> TExecutor::GetStickyColumns(ui32 tableId) {
    auto *tableInfo = Scheme().GetTableInfo(tableId);

    THashSet<NTable::TTag> stickyColumns;
    if (!tableInfo) {
        return stickyColumns;
    }

    for (const auto &column : tableInfo->Columns) {
        const auto* family = tableInfo->Families.FindPtr(column.second.Family);
        if (family && family->Cache == NTable::NPage::ECache::Ever) {
            stickyColumns.insert(column.first);
        }
    }

    return stickyColumns;
}

void TExecutor::ApplyExternalPartSwitch(TPendingPartSwitch &partSwitch) {
    TVector<NTable::TPartView> newParts;
    newParts.reserve(partSwitch.NewBundles.size());
    auto stickyColumns = GetStickyColumns(partSwitch.TableId);

    for (auto &bundle : partSwitch.NewBundles) {
        auto* stage = bundle.GetStage<TPendingPartSwitch::TResultStage>();
        Y_ENSURE(stage && stage->PartView, "Missing bundle result in part switch");
        AddCachesOfBundle(stage->PartView);
        if (stickyColumns) {
            RequestInMemPagesForPartStore(partSwitch.TableId, stage->PartView, stickyColumns);
        }
        newParts.push_back(std::move(stage->PartView));
    }

    TVector<TIntrusiveConstPtr<NTable::TColdPart>> newColdParts = std::move(partSwitch.NewColdParts);

    TVector<TIntrusiveConstPtr<NTable::TTxStatusPart>> newTxStatus;
    newTxStatus.reserve(partSwitch.NewTxStatus.size());
    for (auto &txStatus : partSwitch.NewTxStatus) {
        auto* stage = txStatus.GetStage<TPendingPartSwitch::TTxStatusResultStage>();
        Y_ENSURE(stage && stage->TxStatus, "Missing tx status result in part switch");
        newTxStatus.push_back(std::move(stage->TxStatus));
    }

    if (partSwitch.Changed) {
        NTable::TBundleSlicesMap updatedBundles;
        for (auto &change : partSwitch.Changed) {
            auto overlay = NTable::TOverlay::Decode(change.Legacy, change.Opaque);
            Y_ENSURE(overlay.Slices && *overlay.Slices,
                "Change for bundle " << change.Label << " has unexpected empty slices");
            updatedBundles[change.Label] = std::move(overlay.Slices);
        }

        Database->ReplaceSlices(partSwitch.TableId, std::move(updatedBundles));
    }

    if (partSwitch.Deltas) {
        TVector<TLogoBlobID> labels(Reserve(partSwitch.Deltas.size()));
        for (auto &delta : partSwitch.Deltas) {
            labels.emplace_back(delta.Label);
        }

        NTable::TBundleSlicesMap updatedSlices = Database->LookupSlices(partSwitch.TableId, labels);

        for (auto &delta : partSwitch.Deltas) {
            auto overlay = NTable::TOverlay{ nullptr, updatedSlices.at(delta.Label) };
            overlay.ApplyDelta(delta.Delta);
            updatedSlices[delta.Label] = overlay.Slices;
        }

        Database->ReplaceSlices(partSwitch.TableId, std::move(updatedSlices));
    }

    if (partSwitch.FollowerUpdateStep) {
        auto subset = Database->PartSwitchSubset(partSwitch.TableId, partSwitch.Head, partSwitch.Leaving, partSwitch.LeavingTxStatus);

        if (partSwitch.Head != subset->Head) {
            Y_TABLET_ERROR("Follower table epoch head has diverged from leader");
        } else if (*subset && !subset->IsStickedToHead()) {
            Y_TABLET_ERROR("Follower table replace subset isn't sticked to head");
        }

        Y_ENSURE(newColdParts.empty(), "Unexpected cold part at a follower");
        Database->Replace(partSwitch.TableId, *subset, std::move(newParts), std::move(newTxStatus));

        for (auto &gone : subset->Flatten)
            DropCachesOfBundle(*gone);

        Send(Owner->Tablet(), new TEvTablet::TEvFGcAck(Owner->TabletID(), Generation(), partSwitch.FollowerUpdateStep));
    } else {
        bool merged = false;
        for (auto &partView : newParts) {
            Database->Merge(partSwitch.TableId, partView);
            merged = true;

            if (CompactionLogic) {
                CompactionLogic->BorrowedPart(partSwitch.TableId, std::move(partView));
            }
        }
        for (auto &part : newColdParts) {
            Database->Merge(partSwitch.TableId, part);
            merged = true;

            if (CompactionLogic) {
                CompactionLogic->BorrowedPart(partSwitch.TableId, std::move(part));
            }
        }
        for (auto &txStatus : newTxStatus) {
            Database->Merge(partSwitch.TableId, txStatus);
            merged = true;
        }
        if (merged) {
            Database->MergeDone(partSwitch.TableId);
        }
    }

    if (partSwitch.Moves) {
        struct TMoveState {
            TVector<TLogoBlobID> Bundles;
            THashMap<TLogoBlobID, NTable::TEpoch> BundleToEpoch;
        };

        TMap<ui32, TMoveState> perTable;
        for (auto& move : partSwitch.Moves) {
            auto& state = perTable[move.SourceTable];
            state.Bundles.push_back(move.Label);
            if (move.RebasedEpoch != NTable::TEpoch::Max()) {
                state.BundleToEpoch.emplace(move.Label, move.RebasedEpoch);
            }
        }

        // N.B. there should be a single source table per part switch
        for (auto& [sourceTable, state] : perTable) {
            // Rebase source parts to their respective new epochs
            auto srcSubset = Database->PartSwitchSubset(sourceTable, NTable::TEpoch::Zero(), state.Bundles, { });
            TVector<NTable::TPartView> rebased(Reserve(srcSubset->Flatten.size()));
            for (const auto& partView : srcSubset->Flatten) {
                Y_ENSURE(!partView->TxIdStats, "Cannot move parts with uncommitted deltas");
                NTable::TEpoch epoch = state.BundleToEpoch.Value(partView->Label, partView->Epoch);
                rebased.push_back(partView.CloneWithEpoch(epoch));
            }

            // Remove source parts from the source table
            Database->Replace(sourceTable, *srcSubset, { }, { });

            if (CompactionLogic) {
                CompactionLogic->RemovedParts(sourceTable, state.Bundles);
            }

            // Merge rebased parts to the destination table
            for (auto& partView : rebased) {
                Database->Merge(partSwitch.TableId, partView);

                if (CompactionLogic) {
                    CompactionLogic->BorrowedPart(partSwitch.TableId, std::move(partView));
                }
            }
        }

        Database->MergeDone(partSwitch.TableId);
    }
}

void TExecutor::LeaseConfirmed(ui64 confirmedCookie) {
    bool leaseUpdated = false;
    while (!LeaseCommits.empty()) {
        auto& l = LeaseCommits.front();
        if (l.Cookie <= confirmedCookie) {
            LeaseEnd = Max(LeaseEnd, l.LeaseEnd);

            auto callbacks = std::move(l.Callbacks);
            LeaseCommitsByEnd.erase(l.ByEndIterator);
            LeaseCommits.pop_front();

            for (auto& callback : callbacks) {
                callback();
            }

            leaseUpdated = true;
        } else {
            break;
        }
    }

    if (leaseUpdated && LeaseCommits.empty()) {
        if (LeaseDurationIncreases < 2) {
            // Calculate how much of a lease is left after a full round trip
            // When we are left with less than a third of lease duration we want
            // to increase lease duration so we would have enough time for
            // processing read-only requests without additional commits
            TMonotonic ts = AppData()->MonotonicTimeProvider->Now();
            if ((LeaseEnd - ts) < LeaseDuration / 3) {
                LeaseDuration *= 2;
                LeaseDurationUpdated = true;
                ++LeaseDurationIncreases;
            }
        }

        // We want to schedule a new commit before the lease expires
        if (!LeaseExtendPending) {
            Schedule(LeaseEnd - LeaseDuration / 3, new TEvPrivate::TEvLeaseExtend);
            LeaseExtendPending = true;
        }
    }
}

TExecutor::TLeaseCommit* TExecutor::AddLeaseConfirm() {
    if (!LeaseEnabled || Y_UNLIKELY(LeaseDropped)) {
        return nullptr;
    }

    TMonotonic ts = AppData()->MonotonicTimeProvider->Now();
    TLeaseCommit* lease = &LeaseCommits.emplace_back(0, ts, ts + LeaseDuration, ++LeaseCommitsCounter);
    lease->ByEndIterator = LeaseCommitsByEnd.emplace(lease->LeaseEnd, lease);

    Send(Owner->Tablet(), new TEvTablet::TEvConfirmLeader(Owner->TabletID(), Generation()), 0, lease->Cookie);

    return lease;
}

TExecutor::TLeaseCommit* TExecutor::AttachLeaseCommit(TLogCommit* commit, bool force) {
    if (!LeaseEnabled || Y_UNLIKELY(LeaseDropped)) {
        return nullptr;
    }

    if (force || LeaseDurationUpdated) {
        NKikimrExecutorFlat::TLeaseInfoMetadata proto;
        ActorIdToProto(SelfId(), proto.MutableLeaseHolder());
        proto.SetLeaseDurationUs(LeaseDuration.MicroSeconds());

        TString data;
        bool ok = proto.SerializeToString(&data);
        Y_ENSURE(ok);

        commit->Metadata.emplace_back(ui32(NBoot::ELogCommitMeta::LeaseInfo), std::move(data));
    }

    TMonotonic ts = AppData()->MonotonicTimeProvider->Now();
    TLeaseCommit* lease = &LeaseCommits.emplace_back(commit->Step, ts, ts + LeaseDuration, ++LeaseCommitsCounter);
    LeaseCommitsByStep.PushBack(lease);

    // It may happen in the future that LeaseDuration is decreased by this
    // commit, in which case new leader might read and use it, and may not wait
    // longer than the new LeaseEnd. If there are commits currently in flight
    // make sure to truncate their lease extensions to the new LeaseEnd.
    if (LeaseDurationUpdated) {
        auto it = LeaseCommitsByEnd.upper_bound(lease->LeaseEnd);
        while (it != LeaseCommitsByEnd.end()) {
            TLeaseCommit* other = it->second;
            it = LeaseCommitsByEnd.erase(it);
            other->LeaseEnd = lease->LeaseEnd;
            other->ByEndIterator = LeaseCommitsByEnd.emplace(other->LeaseEnd, other);
        }
        // Currently confirmed lease may become truncated as well
        LeaseEnd = Min(LeaseEnd, lease->LeaseEnd);
        LeaseDurationUpdated = false;
    }

    lease->ByEndIterator = LeaseCommitsByEnd.emplace(lease->LeaseEnd, lease);

    return lease;
}

TExecutor::TLeaseCommit* TExecutor::EnsureReadOnlyLease(TMonotonic at) {
    Y_ENSURE(Stats->IsActive && !Stats->IsFollower());
    Y_ENSURE(at >= LeaseEnd);

    if (!LeaseEnabled) {
        // Automatically enable leases
        LeaseEnabled = true;
        LeaseDuration = Owner->ReadOnlyLeaseDuration();
        Y_ENSURE(LeaseDuration);
        LeaseDurationUpdated = true;
    }

    TLeaseCommit* lease = nullptr;

    // Try to find a suitable commit that is already in flight
    // This would be the first commit where at < LeaseEnd
    auto itAfter = LeaseCommitsByEnd.upper_bound(at);
    if (itAfter != LeaseCommitsByEnd.end()) {
        lease = itAfter->second;
    } else if (!LeaseDropped) {
        if (LeaseDurationUpdated || !LeasePersisted) {
            // We need to make a real commit
            LogicRedo->FlushBatchedLog();

            auto commit = CommitManager->Begin(true, ECommit::Misc, {});

            lease = AttachLeaseCommit(commit.Get(), /* force */ true);

            CommitManager->Commit(commit);

            if (LogicSnap->MayFlush(false)) {
                MakeLogSnapshot();
            }
        } else {
            // We want a lightweight confirmation
            lease = AddLeaseConfirm();
        }
    }

    return lease;
}

void TExecutor::ConfirmReadOnlyLease(TMonotonic at) {
    Y_ENSURE(Stats->IsActive && !Stats->IsFollower());
    LeaseUsed = true;

    if (LeaseEnabled && at < LeaseEnd) {
        return;
    }

    EnsureReadOnlyLease(at);
}

void TExecutor::ConfirmReadOnlyLease(TMonotonic at, std::function<void()> callback) {
    Y_ENSURE(Stats->IsActive && !Stats->IsFollower());
    LeaseUsed = true;

    if (LeaseEnabled && at < LeaseEnd) {
        callback();
        return;
    }

    if (auto* lease = EnsureReadOnlyLease(at)) {
        lease->Callbacks.push_back(std::move(callback));
    }
}

void TExecutor::ConfirmReadOnlyLease(std::function<void()> callback) {
    ConfirmReadOnlyLease(AppData()->MonotonicTimeProvider->Now(), std::move(callback));
}

bool TExecutor::CanExecuteTransaction() const {
    return Stats->IsActive && (Stats->IsFollower() || PendingPartSwitches.empty()) && !BrokenTransaction;
}

ui64 TExecutor::DoExecute(TAutoPtr<ITransaction> self, ETxMode mode) {
    ui64 uniqId = ++TransactionUniqCounter;
    TSeat* seat = (Transactions[uniqId] = std::make_unique<TSeat>(uniqId, self)).get();
    seat->LowPriority = mode == ETxMode::LowPriority;
    seat->Self->SetupTxSpanName();

    LWTRACK(TransactionBegin, seat->Self->Orbit, seat->UniqID, Owner->TabletID(), TypeName(*seat->Self));

    ++Stats->TxInFly;
    Counters->Simple()[TExecutorCounters::DB_TX_IN_FLY] = Stats->TxInFly;
    if (AppTxCounters && seat->TxType != UnknownTxType) {
        AppTxCounters->TxSimple(seat->TxType, COUNTER_TT_INFLY) += 1;
    }
    Counters->Cumulative()[TExecutorCounters::TX_COUNT_ALL].Increment(1); //Deprecated
    Counters->Cumulative()[TExecutorCounters::TX_QUEUED].Increment(1);

    if (auto logl = Logger->Log(ELnLev::Debug)) {
        logl
            << NFmt::Do(*this) << " " << NFmt::Do(*seat)
            << " queued, type " << NFmt::Do(*seat->Self);
    }

    ui64 staticRemain = Memory->RemainedStatic(*seat);

    // Submit resource broker task if there is no enough memory to start
    // new transaction.
    seat->CurrentTxDataLimit = Memory->Profile->GetInitialTxMemory();
    if (staticRemain < seat->CurrentTxDataLimit) {
        LWTRACK(TransactionNeedMemory, seat->Self->Orbit, seat->UniqID);
        Memory->RequestLimit(*seat, seat->CurrentTxDataLimit);
        seat->State = ESeatState::Postponed;
        PostponedTransactions.PushBack(seat);
        return uniqId;
    }

    Memory->AllocStatic(*seat, Memory->Profile->GetInitialTxMemory());

    if (!CanExecuteTransaction()
            || Scheme().Executor.LimitInFlyTx && Stats->TxInFly > Scheme().Executor.LimitInFlyTx)
    {
        LWTRACK(TransactionPending, seat->Self->Orbit, seat->UniqID,
                CanExecuteTransaction() ? "tx limit reached" : "transactions paused");
        seat->CreatePendingSpan();
        seat->State = ESeatState::Pending;
        PendingQueue.PushBack(seat);
        ++Stats->TxPending;
        return uniqId;
    }

    if (mode == ETxMode::Execute && (ActiveTransaction || ActivateTransactionWaiting)) {
        mode = ETxMode::Enqueue;
    }

    switch (mode) {
        case ETxMode::Execute:
            ExecuteTransaction(seat);
            return uniqId;

        case ETxMode::Enqueue:
        case ETxMode::LowPriority:
            EnqueueActivation(seat, true);
            return uniqId;
    }

    Y_TABLET_ERROR("Unimplemented transaction mode");
}

void TExecutor::Execute(TAutoPtr<ITransaction> self, const TActorContext &ctx) {
    Y_UNUSED(ctx);
    DoExecute(self, ETxMode::Execute);
}

ui64 TExecutor::Enqueue(TAutoPtr<ITransaction> self) {
    return DoExecute(self, ETxMode::Enqueue);
}

ui64 TExecutor::EnqueueLowPriority(TAutoPtr<ITransaction> self) {
    return DoExecute(self, ETxMode::LowPriority);
}

bool TExecutor::CancelTransaction(ui64 id) {
    auto it = Transactions.find(id);
    if (it == Transactions.end()) {
        return false;
    }

    TSeat* seat = it->second.get();
    switch (seat->State) {
        case ESeatState::None:
            // Transaction is not paused in any way
            Y_DEBUG_ABORT_UNLESS(false,
                "Tablet %" PRIu64 " CancelTransaction(%" PRIu64 ") from inside transaction?",
                TabletId(), id);
            return false;

        case ESeatState::Active:
            ActivationQueue.Remove(seat);
            Y_ENSURE(ActivateTransactionWaiting > 0);
            --ActivateTransactionWaiting;
            break;

        case ESeatState::ActiveLow:
            ActivationLowQueue.Remove(seat);
            Y_ENSURE(ActivateLowTransactionWaiting > 0);
            --ActivateLowTransactionWaiting;
            break;

        case ESeatState::Pending:
            PendingQueue.Remove(seat);
            Y_ENSURE(Stats->TxPending > 0);
            --Stats->TxPending;
            break;

        case ESeatState::Postponed:
        case ESeatState::Waiting:
            if (seat->Cancelled) {
                return false;
            }
            // Cannot safely remove now, wait until later
            seat->Cancelled = true;
            return true;

        default:
            Y_DEBUG_ABORT_UNLESS(false,
                "Tablet %" PRIu64 " CancelTransaction(% " PRIu64 ") for a finished transaction",
                TabletId(), id);
            return false;
    }

    seat->State = ESeatState::None;
    FinishCancellation(seat);
    return true;
}

void TExecutor::ExecuteTransaction(TSeat* seat) {
    TActiveTransactionZone activeTransaction(this);
    ++seat->Retries;

    THPTimer cpuTimer;

    PrivatePageCache->ResetTouchesAndToLoad(true);
    TPageCollectionTxEnv env(*Database, *PrivatePageCache);

    TTransactionContext txc(Owner->TabletID(), Generation(), Step(), *Database, env, seat->CurrentTxDataLimit, seat->TaskId, seat->Self->TxSpan);
    txc.NotEnoughMemory(seat->NotEnoughMemoryCount);

    Database->Begin(Stamp(), env);

    LWTRACK(TransactionExecuteBegin, seat->Self->Orbit, seat->UniqID);

    txc.StartExecutionSpan();
    const bool done = seat->Self->Execute(txc, OwnerCtx());
    txc.FinishExecutionSpan();

    LWTRACK(TransactionExecuteEnd, seat->Self->Orbit, seat->UniqID, done);

    seat->CPUExecTime += cpuTimer.PassedReset();

    if (done) {
        Counters->Percentile()[TExecutorCounters::TX_PERCENTILE_COMMIT_REDO_BYTES].IncrementFor(Database->GetCommitRedoBytes());
    }

    bool failed = false;
    if (done) {
        ui64 commitRedoBytes = Database->GetCommitRedoBytes();
        ui64 maxCommitRedoBytes = ui64(MaxCommitRedoMB) << 20; // MB to bytes
        if (commitRedoBytes > maxCommitRedoBytes) {
            if (auto logl = Logger->Log(ELnLev::Crit)) {
                logl
                    << NFmt::Do(*this) << " " << NFmt::Do(*seat)
                    << " fatal commit failure: Redo commit of " << commitRedoBytes
                    << " bytes is more than the allowed limit";
            }
            failed = true;
        }
    }

    auto *annex = CommitManager ? CommitManager->Annex.Get() : nullptr;
    auto prod = Database->Commit(Stamp(), done && !failed, annex);

    if (auto logl = Logger->Log(ELnLev::Debug)) {
        logl
            << NFmt::Do(*this) << " " << NFmt::Do(*seat)
            << " hope " << seat->Retries << " ->"
            << " " << (failed ? "failed" : done ? "done" : "retry")
            << " " << NFmt::If(prod.Change.Get());
    }

    seat->AttachedMemory = txc.ExtractMemoryToken();
    seat->RequestedMemory = txc.GetRequestedMemory();
    seat->CapturedMemory = txc.GetMemoryGCToken();
    seat->NotEnoughMemoryCount = txc.GetNotEnoughMemoryCount();

    if (seat->AttachedMemory)
        Counters->Cumulative()[TExecutorCounters::TX_MEM_ATTACHES].Increment(1);
    if (seat->RequestedMemory)
        Counters->Cumulative()[TExecutorCounters::TX_MEM_REQUESTS].Increment(1);
    if (seat->CapturedMemory) {
        Counters->Cumulative()[TExecutorCounters::TX_MEM_CAPTURES].Increment(1);
        Memory->ScheduleGC();
    }

    const auto& txStats = prod.Change->Stats;
    Counters->Cumulative()[TExecutorCounters::TX_CHARGE_WEEDED].Increment(txStats.ChargeWeeded);
    Counters->Cumulative()[TExecutorCounters::TX_CHARGE_SIEVED].Increment(txStats.ChargeSieved);
    Counters->Cumulative()[TExecutorCounters::TX_SELECT_WEEDED].Increment(txStats.SelectWeeded);
    Counters->Cumulative()[TExecutorCounters::TX_SELECT_SIEVED].Increment(txStats.SelectSieved);
    Counters->Cumulative()[TExecutorCounters::TX_SELECT_NO_KEY].Increment(txStats.SelectNoKey);

    if (failed) {
        // Block new transactions from executing
        BrokenTransaction = true;

        // It may not be safe to call Broken right now, call it later
        Send(SelfId(), new TEvPrivate::TEvBrokenTransaction());

        // Make sure transaction is properly destroyed
        RemoveTransaction(seat->UniqID);
    } else if (done) {
        Y_ENSURE(!txc.IsRescheduled());
        Y_ENSURE(!seat->RequestedMemory);
        seat->OnPersistent = std::move(prod.OnPersistent);
        CommitTransactionLog(RemoveTransaction(seat->UniqID), env, prod.Change, cpuTimer);
    } else {
        Y_ENSURE(!seat->CapturedMemory);
        if (!PrivatePageCache->GetStats().CurrentCacheMisses && !seat->RequestedMemory && !txc.IsRescheduled()) {
            Y_TABLET_ERROR(NFmt::Do(*this) << " " << NFmt::Do(*seat) << " type "
                    << NFmt::Do(*seat->Self) << " postponed w/o demands");
        }
        PostponeTransaction(seat, env, prod.Change, cpuTimer);
    }
    PrivatePageCache->ResetTouchesAndToLoad(false);

    activeTransaction.Done();
    PlanTransactionActivation();
}

void TExecutor::UnpinTransactionPages(TSeat &seat) {
    Y_ENSURE(TransactionPagesMemory >= seat.MemoryTouched);
    TransactionPagesMemory -= seat.MemoryTouched;

    size_t unpinnedPages = 0;
    PrivatePageCache->UnpinPages(seat.Pinned, unpinnedPages);
    seat.Pinned.clear();
    seat.MemoryTouched = 0;

    Counters->Simple()[TExecutorCounters::CACHE_PINNED_SET] = PrivatePageCache->GetStats().PinnedSetSize;
    Counters->Simple()[TExecutorCounters::CACHE_PINNED_LOAD] = PrivatePageCache->GetStats().PinnedLoadSize;
    Counters->Simple()[TExecutorCounters::CACHE_TOTAL_USED] = TransactionPagesMemory;
}

void TExecutor::ReleaseTxData(TSeat &seat, ui64 requested)
{
    if (auto logl = Logger->Log(ELnLev::Debug))
        logl << NFmt::Do(*this) << " " << NFmt::Do(seat) << " release tx data";

    TTxMemoryProvider provider(seat.CurrentTxDataLimit - requested, seat.TaskId);
    static_cast<TTxMemoryProviderBase&>(provider).RequestMemory(requested);
    seat.Self->ReleaseTxData(provider, OwnerCtx());

    Counters->Cumulative()[TExecutorCounters::TX_DATA_RELEASES].Increment(1);

    if (seat.CapturedMemory = provider.GetMemoryGCToken())
        Counters->Cumulative()[TExecutorCounters::TX_MEM_CAPTURES].Increment(1);

    Memory->ReleaseTxData(seat);
}

void TExecutor::PostponeTransaction(TSeat* seat, TPageCollectionTxEnv &env,
                                    TAutoPtr<NTable::TChange> change,
                                    THPTimer &bookkeepingTimer)
{
    TTxType txType = seat->TxType;

    ui32 touchedPages = 0;
    ui32 newPinnedPages = 0;
    ui64 prevTouched = seat->MemoryTouched;

    PrivatePageCache->PinTouches(seat->Pinned, touchedPages, newPinnedPages, seat->MemoryTouched);
    TransactionPagesMemory += seat->MemoryTouched - prevTouched;

    ui32 newTouchedPages = newPinnedPages;
    ui64 newTouchedBytes = seat->MemoryTouched - prevTouched;
    prevTouched = seat->MemoryTouched;

    PrivatePageCache->PinToLoad(seat->Pinned, newPinnedPages, seat->MemoryTouched);
    ui64 loadBytes = seat->MemoryTouched - prevTouched;
    TransactionPagesMemory += seat->MemoryTouched - prevTouched;

    if (seat->AttachedMemory)
        Memory->AttachMemory(*seat);

    const ui64 requestedMemory = std::exchange(seat->RequestedMemory, 0);
    seat->CurrentTxDataLimit += requestedMemory;

    if (auto logl = Logger->Log(ELnLev::Debug)) {
        logl
            << NFmt::Do(*this) << " " << NFmt::Do(*seat)
            << " touch new " << newTouchedBytes << "b"
            << ", " << (seat->MemoryTouched - prevTouched) << "b lo load"
            << " (" << seat->MemoryTouched << "b in total)"
            << ", " << requestedMemory << "b requested for data"
            << " (" << seat->CurrentTxDataLimit << "b in total)";
    }

    // Check if additional resources should be requested.
    ui64 totalMemory = seat->MemoryTouched + seat->CurrentTxDataLimit;
    auto limit = Memory->Profile->GetTxMemoryLimit();
    if (limit && totalMemory > limit) {

        if (auto logl = Logger->Log(ELnLev::Error)) {
            logl
                << NFmt::Do(*this) << " " << NFmt::Do(*seat)
                << " mem " << totalMemory << "b terminated"
                << ", limit " << limit << "b is exceeded";
        }

        seat->TerminationReason = ETerminationReason::MemoryLimitExceeded;
        CommitTransactionLog(RemoveTransaction(seat->UniqID), env, change, bookkeepingTimer);
        return;
    } else if (totalMemory > seat->CurrentMemoryLimit) {

        // We usually try to at least double allocated memory. But it's OK to use less
        // to avoid resource broker request.
        ui64 desired = Max(totalMemory, seat->CurrentMemoryLimit * 2);
        bool allocated = false;

        // Try to allocate static memory.
        if (!seat->TaskId) {
            ui64 staticRemain = Memory->RemainedStatic(*seat);
            if (staticRemain >= totalMemory - seat->CurrentMemoryLimit) {
                ui64 limit = Min(staticRemain + seat->CurrentMemoryLimit, desired);
                Memory->AllocStatic(*seat, limit);
                allocated = true;
            }
        }

        // Submit or resubmit task with new resource requirements.
        if (!allocated) {
            LWTRACK(TransactionNeedMemory, seat->Self->Orbit, seat->UniqID);
            Memory->FreeStatic(*seat, 0);
            UnpinTransactionPages(*seat);
            ReleaseTxData(*seat, requestedMemory);

            Memory->RequestLimit(*seat, desired);
            seat->State = ESeatState::Postponed;
            PostponedTransactions.PushBack(seat);

            // todo: counters
            return;
        }
    }

    // If memory was allocated and there is nothing to load
    // then tx may be re-activated.
    if (!PrivatePageCache->GetStats().CurrentCacheMisses) {
        EnqueueActivation(seat, CanExecuteTransaction());
        return;
    }

    LWTRACK(TransactionPageFault, seat->Self->Orbit, seat->UniqID);
    seat->State = ESeatState::Waiting;
    auto waitPad = MakeIntrusive<TTransactionWaitPad>(seat);
    TransactionWaitPads[waitPad.Get()] = waitPad;

    ui32 loadPages = 0;
    auto toLoad = PrivatePageCache->GetToLoad();
    for (auto &[pageCollectionInfo, pages] : toLoad) {
        Y_DEBUG_ABORT_UNLESS(pages);
        loadPages += pages.size();

        if (auto logl = Logger->Log(ELnLev::Dbg03)) {
            logl
                << NFmt::Do(*this) << " " << NFmt::Do(*seat) << " request page collection " << pageCollectionInfo->PageCollection->Label()
                << " pages [ ";
            for (auto pageId : pages) {
                logl << pageId << " ";
            }
            logl << "]";
        }

        auto *req = new NPageCollection::TFetch(0, pageCollectionInfo->PageCollection, std::move(pages));
        req->TraceId = waitPad->GetWaitingTraceId();
        req->WaitPad = waitPad;
        ++waitPad->PendingRequests;

        RequestFromSharedCache(req, NBlockIO::EPriority::Fast, ESharedCacheRequestType::Transaction);
    }

    if (auto logl = Logger->Log(ELnLev::Debug)) {
        logl
            << NFmt::Do(*this) << " " << NFmt::Do(*seat) << " postponed"
            << ", loading " << loadPages << " pages, " << loadBytes << " bytes"
            << ", freshly touched " << newPinnedPages << " pages";
    }

    seat->CPUBookkeepingTime += bookkeepingTimer.PassedReset();
    Counters->Cumulative()[TExecutorCounters::TX_POSTPONED].Increment(1);

    if (AppTxCounters && txType != UnknownTxType)
        AppTxCounters->TxCumulative(txType, COUNTER_TT_POSTPONED).Increment(1);

    // Note: count all new touched pages (were obtained from cache), even not on the first attempt
    Counters->Cumulative()[TExecutorCounters::TX_CACHE_HITS].Increment(newTouchedPages);
    Counters->Cumulative()[TExecutorCounters::TX_BYTES_CACHED].Increment(newTouchedBytes);
    if (seat->Retries == 1) {
        Counters->Cumulative()[TExecutorCounters::TX_RETRIED].Increment(1);
    }

    Counters->Cumulative()[TExecutorCounters::TX_CACHE_MISSES].Increment(loadPages);
    Counters->Cumulative()[TExecutorCounters::TX_BYTES_READ].Increment(loadBytes);
    if (AppTxCounters && txType != UnknownTxType) {
        AppTxCounters->TxCumulative(txType, COUNTER_TT_LOADED_BLOCKS).Increment(loadPages);
        AppTxCounters->TxCumulative(txType, COUNTER_TT_BYTES_READ).Increment(loadBytes);
    }

    Counters->Simple()[TExecutorCounters::CACHE_PINNED_SET] = PrivatePageCache->GetStats().PinnedSetSize;
    Counters->Simple()[TExecutorCounters::CACHE_PINNED_LOAD] = PrivatePageCache->GetStats().PinnedLoadSize;
    Counters->Simple()[TExecutorCounters::CACHE_TOTAL_USED] = TransactionPagesMemory;
}

void TExecutor::CommitTransactionLog(std::unique_ptr<TSeat> seat, TPageCollectionTxEnv &env,
                    TAutoPtr<NTable::TChange> change, THPTimer &bookkeepingTimer) {
    const bool isReadOnly = !(change->HasAny() || env.HasChanges());
    const TTxType txType = seat->TxType;

    size_t touchedBlocks = PrivatePageCache->GetStats().CurrentCacheHits;
    Counters->Percentile()[TExecutorCounters::TX_PERCENTILE_TOUCHED_BLOCKS].IncrementFor(touchedBlocks);
    if (AppTxCounters && txType != UnknownTxType)
        AppTxCounters->TxCumulative(txType, COUNTER_TT_TOUCHED_BLOCKS).Increment(touchedBlocks);

    // Note: count all new touched pages (were obtained from cache), even not on the first attempt
    ui32 newTouchedPages = 0;
    ui64 newTouchedBytes = 0, pinnedTouchedBytes = 0;
    PrivatePageCache->CountTouches(seat->Pinned, newTouchedPages, newTouchedBytes, pinnedTouchedBytes);
    Counters->Cumulative()[TExecutorCounters::TX_CACHE_HITS].Increment(newTouchedPages);
    Counters->Cumulative()[TExecutorCounters::TX_BYTES_CACHED].Increment(newTouchedBytes);
    if (seat->MemoryTouched >= pinnedTouchedBytes) {
        // memory that was pinned (for instance by Precharge) but wasn't used during the last successful execution
        Counters->Cumulative()[TExecutorCounters::TX_BYTES_WASTED].Increment(seat->MemoryTouched - pinnedTouchedBytes);
    } else {
        Y_DEBUG_ABORT("Cache counters are out of sync");
    }

    UnpinTransactionPages(*seat);

    Memory->ReleaseMemory(*seat);

    const double currentBookkeepingTime = seat->CPUBookkeepingTime;
    const double currentExecTime = seat->CPUExecTime;

    if (isReadOnly) {
        // Note: per-tx InFly is decremented in txloglogic
        if (Stats->IsFollower()) {
            // todo: extract completion counters from txloglogic
            --Stats->TxInFly;
            Counters->Simple()[TExecutorCounters::DB_TX_IN_FLY] = Stats->TxInFly;
            CompleteRoTransaction(std::move(seat), OwnerCtx(), Counters.Get(), AppTxCounters);
        } else if (LogicRedo->CommitROTransaction(std::move(seat), OwnerCtx())) {
            --Stats->TxInFly;
            Counters->Simple()[TExecutorCounters::DB_TX_IN_FLY] = Stats->TxInFly;
        }
    } else {
        Y_ENSURE(!Stats->IsFollower());
        Y_ENSURE(!seat->IsTerminated(), "Read-write transactions cannot be terminated");

        const bool allowBatching = Scheme().Executor.AllowLogBatching;
        const bool force = !allowBatching
            || change->Scheme
            || change->Annex  /* Required for replication to followers */
            || change->RemovedRowVersions  /* Required for replication to followers */
            || env.MakeSnap
            || env.DropSnap
            || env.LoanBundle
            || env.LoanTxStatus
            || env.LoanConfirmation
            || env.BorrowUpdates;

        auto commitResult = LogicRedo->CommitRWTransaction(std::move(seat), *change, force);

        Y_ENSURE(!force || commitResult.Commit);
        auto *commit = commitResult.Commit.Get(); // could be nullptr

        for (auto& pr : env.MakeSnap) {
            const ui32 table = pr.first;
            auto& snap = pr.second;

            Y_ENSURE(snap.Epoch, "Table was not snapshotted");

            for (auto &context: snap.Context) {
                auto edge = NTable::TSnapEdge(change->Stamp - 1, *snap.Epoch);

                if (!context->Impl)
                    context->Impl.Reset(new TTableSnapshotContext::TImpl);

                context->Impl->Prepare(table, edge);
                CompactionLogic->PrepareTableSnapshot(table, edge, context.Get());
                WaitingSnapshots.insert(std::make_pair(context.Get(), context));
            }
        }

        if (auto alter = std::move(change->Scheme)) {
            LogicAlter->WriteLog(*commit, std::move(alter));
            auto reflectResult = CompactionLogic->ReflectSchemeChanges();

            ReadResourceProfile();
            ReflectSchemeSettings();
            RequestInMemPagesForDatabase(/* pendingOnly */ true);

            // For every table that changed strategy we need to generate a
            // special part switch that notifies bootlogic about new strategy
            // type and a cleared compaction state.
            for (auto &change : reflectResult.StrategyChanges) {
                const auto tableId = change.Table;
                const auto strategy = change.Strategy;

                NKikimrExecutorFlat::TTablePartSwitch proto;
                proto.SetTableId(tableId);

                auto *changesProto = proto.MutableCompactionChanges();
                changesProto->SetTable(tableId);
                changesProto->SetStrategy(strategy);

                auto body = proto.SerializeAsString();
                auto glob = CommitManager->Turns.One(commit->Refs, std::move(body), true);

                Y_UNUSED(glob);
            }
        }

        // Generate a special part switch for removed row versions
        for (auto& xpair : change->RemovedRowVersions) {
            const auto tableId = xpair.first;

            CompactionLogic->ReflectRemovedRowVersions(tableId);

            NKikimrExecutorFlat::TTablePartSwitch proto;
            proto.SetTableId(tableId);

            auto *changesProto = proto.MutableRowVersionChanges();
            changesProto->SetTable(tableId);

            for (auto& range : xpair.second) {
                auto *rangeProto = changesProto->AddRemovedRanges();

                auto *lower = rangeProto->MutableLower();
                lower->SetStep(range.Lower.Step);
                lower->SetTxId(range.Lower.TxId);

                auto *upper = rangeProto->MutableUpper();
                upper->SetStep(range.Upper.Step);
                upper->SetTxId(range.Upper.TxId);
            }

            auto body = proto.SerializeAsString();
            auto glob = CommitManager->Turns.One(commit->Refs, std::move(body), true);

            Y_UNUSED(glob);
        }

        for (auto num : xrange(change->Deleted.size())) {
            /* Wipe and table deletion happens before any data updates, so
                edge should be put before the current redo log step and table
                head epoch. Now this code is used only for flushing redo log
                to gc, but edge in switch record may turn deletion into table
                wipe feature.
             */

            auto head = change->Garbage[num]->Head;
            if (head > NTable::TEpoch::Zero()) {
                --head;
            } else {
                head = NTable::TEpoch::Zero();
            }

            NTable::TSnapEdge edge(change->Stamp - 1, head);

            for (auto& snapshot : Scans->Drop(change->Deleted[num])) {
                ReleaseScanLocks(std::move(snapshot->Barrier), *snapshot->Subset);
            }
            LogicRedo->CutLog(change->Deleted[num], edge, commit->GcDelta);
        }

        if (auto garbage = std::move(change->Garbage)) {
            commit->WaitFollowerGcAck = true; // as we could collect some page collections

            for (auto &subset: garbage) {
                ui64 total = 0;
                TDeque<NTable::NFwd::TSieve> sieve(subset->Flatten.size() + 1);

                for (auto seq: xrange(subset->Flatten.size())) {
                    sieve[seq] = {
                        subset->Flatten[seq]->Blobs,
                        subset->Flatten[seq]->Large,
                        subset->Flatten[seq].Slices,
                        { }
                    };

                    total += sieve[seq].Total();
                }

                { /* the last sieve corresponds to all TMemTable tables blobs */
                    sieve.back() = {
                        NTable::TMemTable::MakeBlobsPage(subset->Frozen),
                        nullptr,
                        nullptr,
                        { }
                    };

                    total += sieve.back().Total();
                }

                UtilizeSubset(*subset, { total, 0, std::move(sieve) }, { }, commit);
            }

            TIntrusivePtr<TBarrier> barrier(new TBarrier(commit->Step));
            Y_ENSURE(InFlyCompactionGcBarriers.emplace(commit->Step, barrier).second);
            GcLogic->HoldBarrier(barrier->Step);
        }

        NKikimrExecutorFlat::TFollowerPartSwitchAux aux;

        if (auto *snap = env.DropSnap.Get()) {
            auto result = snap->SnapContext->Impl->Release();

            if (result.Step != commit->Step && result.Bundles) {
                /* It is possible to make a valid borrow snapshot only on the
                    last Execute(..) call of tx having ClearSnapshot(..) before
                    any desired BorrowSnapshot(..). The other ways are unsafe
                    due to races with compaction commits which eventually drops
                    blobs of compacted bundles.
                 */

                 Y_TABLET_ERROR("Dropping snapshot in step " << result.Step << " is"
                    << " unsafe, final tx Execute() step is " << commit->Step
                    << ", borrowed " << result.Bundles.size() << " bundles");
            }

            for (auto &bundle: result.Bundles)
                BorrowLogic->BorrowBundle(bundle.first, bundle.second, commit);

            if (result.Moved) {
                for (const auto& [src, dst] : result.Moved) {
                    auto srcSubset = Database->Subset(src, snap->SnapContext->Impl->Edge(src).Head, { }, { });
                    auto dstSubset = Database->Subset(dst, NTable::TEpoch::Max(), { }, { });

                    Y_ENSURE(srcSubset && dstSubset, "Unexpected failure to grab subsets");
                    Y_ENSURE(srcSubset->Frozen.empty(), "Unexpected frozen parts in src subset");

                    // Check scheme compatibility (it may have changed due to alter)
                    auto tableInfo = Database->GetScheme().Tables.FindPtr(src);
                    srcSubset->Scheme->CheckCompatibility(tableInfo ? tableInfo->Name : "", *dstSubset->Scheme);

                    // Don't do anything if there's nothing to move
                    if (srcSubset->Flatten.empty()) {
                        continue;
                    }

                    // We need to sort source parts by their epoch in descending order
                    std::sort(srcSubset->Flatten.begin(), srcSubset->Flatten.end(),
                        [](const NTable::TPartView& a, const NTable::TPartView& b) {
                            if (a->Epoch != b->Epoch) {
                                return b->Epoch < a->Epoch;
                            } else {
                                return a->Label < b->Label;
                            }
                        });

                    // Find the minimum available epoch that would correspond to source maximum
                    NTable::TEpoch srcEpoch = srcSubset->Flatten[0]->Epoch;
                    NTable::TEpoch dstEpoch = NTable::TEpoch::Zero();
                    for (const NTable::TPartView& partView : dstSubset->Flatten) {
                        dstEpoch = Min(dstEpoch, partView->Epoch);
                    }
                    --dstEpoch;

                    // Rebase source parts to new epochs (from newest to oldest)
                    TVector<TLogoBlobID> labels;
                    TVector<NTable::TPartView> rebased(Reserve(srcSubset->Flatten.size()));
                    for (const NTable::TPartView& partView : srcSubset->Flatten) {
                        Y_ENSURE(!partView->TxIdStats, "Cannot move parts with uncommitted deltas");
                        if (srcEpoch != partView->Epoch) {
                            srcEpoch = partView->Epoch;
                            --dstEpoch;
                        }
                        labels.push_back(partView->Label);
                        rebased.push_back(partView.CloneWithEpoch(dstEpoch));
                    }

                    // Remove source parts from the source table
                    Database->Replace(src, *srcSubset, { }, { });

                    const auto logicResult = CompactionLogic->RemovedParts(src, labels);

                    Y_ENSURE(!logicResult.Changes.SliceChanges, "Unexpected slice changes when removing parts");

                    if (logicResult.Changes.StateChanges) {
                        NKikimrExecutorFlat::TTablePartSwitch proto;

                        proto.SetTableId(src);
                        auto* x = proto.MutableCompactionChanges();
                        x->SetTable(src);
                        x->SetStrategy(logicResult.Strategy);
                        x->MutableKeyValues()->Reserve(logicResult.Changes.StateChanges.size());
                        for (const auto& kv : logicResult.Changes.StateChanges) {
                            auto* p = x->AddKeyValues();
                            p->SetKey(kv.first);
                            if (kv.second) {
                                p->SetValue(kv.second);
                            }
                        }

                        auto body = proto.SerializeAsString();
                        auto glob = CommitManager->Turns.One(commit->Refs, std::move(body), true);
                        Y_UNUSED(glob);
                    }

                    // Add rebased parts to the destination table
                    for (auto& partView : rebased) {
                        Database->Merge(dst, partView);
                        CompactionLogic->BorrowedPart(dst, partView);
                    }
                    Database->MergeDone(dst);

                    // Serialize rebased parts as moved from the source table
                    NKikimrExecutorFlat::TTablePartSwitch proto;
                    proto.SetTableId(dst);

                    auto *snap = proto.MutableIntroducedParts();
                    auto *bySwitchAux = aux.AddBySwitchAux();

                    snap->SetTable(dst);
                    snap->SetCompactionLevel(CompactionLogic->BorrowedPartLevel());

                    for (const auto& partView : rebased) {
                        auto* x = proto.AddBundleMoves();
                        LogoBlobIDFromLogoBlobID(partView->Label, x->MutableLabel());
                        x->SetRebasedEpoch(partView->Epoch.ToProto());
                        x->SetSourceTable(src);
                    }

                    auto body = proto.SerializeAsString();
                    auto glob = CommitManager->Turns.One(commit->Refs, std::move(body), true);

                    LogoBlobIDFromLogoBlobID(glob.Logo, bySwitchAux->MutablePartSwitchRef());
                }
            }

            InFlySnapCollectionBarriers.emplace(commit->Step, std::move(result.Barriers));
        }

        bool hadPendingPartSwitches = bool(PendingPartSwitches);

        aux.MutableBySwitchAux()->Reserve(aux.BySwitchAuxSize() + env.LoanBundle.size() + env.LoanTxStatus.size());
        for (auto &loaned : env.LoanBundle) {
            auto& partSwitch = PendingPartSwitches.emplace_back();
            partSwitch.TableId = loaned->LocalTableId;
            partSwitch.Step = commit->Step;

            Y_ENSURE(loaned->PartComponents.PageCollectionComponents, "Loaned PartComponents without any page collections");

            BorrowLogic->LoanBundle(
                loaned->PartComponents.PageCollectionComponents.front().LargeGlobId.Lead, *loaned, commit);

            {
                NKikimrExecutorFlat::TTablePartSwitch proto;

                proto.SetTableId(partSwitch.TableId);

                {
                    TGCBlobDelta dummy; /* this isn't real cut log operation */

                    auto epoch = Max(loaned->PartComponents.GetEpoch(), NTable::TEpoch::Zero()) + 1;
                    auto stamp = MakeGenStepPair(Generation(), commit->Step);

                    LogicRedo->CutLog(loaned->LocalTableId, { stamp, epoch }, dummy);

                    Y_ENSURE(!dummy.Deleted && !dummy.Created);

                    auto *sx = proto.MutableTableSnapshoted();
                    sx->SetTable(loaned->LocalTableId);
                    sx->SetGeneration(Generation());
                    sx->SetStep(commit->Step);
                    sx->SetHead(epoch.ToProto());
                }

                auto *snap = proto.MutableIntroducedParts();
                auto *bySwitchAux = aux.AddBySwitchAux();

                TPageCollectionProtoHelper::Snap(snap, loaned->PartComponents, partSwitch.TableId, CompactionLogic->BorrowedPartLevel());
                TPageCollectionProtoHelper(true).Do(bySwitchAux->AddHotBundles(), loaned->PartComponents);

                auto body = proto.SerializeAsString();
                auto glob = CommitManager->Turns.One(commit->Refs, std::move(body), true);

                LogoBlobIDFromLogoBlobID(glob.Logo, bySwitchAux->MutablePartSwitchRef());
            }

            PrepareExternalPart(partSwitch, std::move(loaned->PartComponents));
        }
        for (auto &loaned : env.LoanTxStatus) {
            auto& partSwitch = PendingPartSwitches.emplace_back();
            partSwitch.TableId = loaned->LocalTableId;
            partSwitch.Step = commit->Step;

            BorrowLogic->LoanTxStatus(
                loaned->DataId.Lead, *loaned, commit);

            {
                NKikimrExecutorFlat::TTablePartSwitch proto;

                proto.SetTableId(partSwitch.TableId);

                {
                    TGCBlobDelta dummy; /* this isn't real cut log operation */

                    auto epoch = Max(loaned->Epoch, NTable::TEpoch::Zero()) + 1;
                    auto stamp = MakeGenStepPair(Generation(), commit->Step);

                    LogicRedo->CutLog(loaned->LocalTableId, { stamp, epoch }, dummy);

                    Y_ENSURE(!dummy.Deleted && !dummy.Created);

                    auto *sx = proto.MutableTableSnapshoted();
                    sx->SetTable(loaned->LocalTableId);
                    sx->SetGeneration(Generation());
                    sx->SetStep(commit->Step);
                    sx->SetHead(epoch.ToProto());
                }

                auto *snap = proto.MutableIntroducedTxStatus();
                auto *bySwitchAux = aux.AddBySwitchAux();

                snap->SetTable(partSwitch.TableId);
                {
                    auto *x = snap->AddTxStatus();
                    TLargeGlobIdProto::Put(*x->MutableDataId(), loaned->DataId);
                    x->SetEpoch(loaned->Epoch.ToProto());
                }

                {
                    auto *x = bySwitchAux->AddHotTxStatus();
                    TLargeGlobIdProto::Put(*x->MutableDataId(), loaned->DataId);
                    x->SetEpoch(loaned->Epoch.ToProto());
                }

                auto body = proto.SerializeAsString();
                auto glob = CommitManager->Turns.One(commit->Refs, std::move(body), true);

                LogoBlobIDFromLogoBlobID(glob.Logo, bySwitchAux->MutablePartSwitchRef());
            }

            PrepareExternalTxStatus(partSwitch, loaned->DataId, loaned->Epoch, loaned->Data);
        }

        if (!hadPendingPartSwitches) {
            ApplyReadyPartSwitches(); // safe to apply switches right now
        }

        if (aux.BySwitchAuxSize()) {
            commit->FollowerAux = NPageCollection::TSlicer::Lz4()->Encode(aux.SerializeAsString());
        }

        if (env.BorrowUpdates) {
            commit->WaitFollowerGcAck = true;
            for (auto &borrowUpdate : env.BorrowUpdates) {
                BorrowLogic->UpdateBorrow(
                    borrowUpdate.first,
                    borrowUpdate.second,
                    commit);
            }

            TIntrusivePtr<TBarrier> barrier(new TBarrier(commit->Step));
            Y_ENSURE(InFlyCompactionGcBarriers.emplace(commit->Step, barrier).second);
            GcLogic->HoldBarrier(barrier->Step);
        }

        if (env.LoanConfirmation) {
            commit->WaitFollowerGcAck = true;
            for (auto &xupd : env.LoanConfirmation) {
                BorrowLogic->ConfirmUpdateLoan(
                    xupd.first,
                    xupd.second.BorrowId,
                    commit);
            }
            TIntrusivePtr<TBarrier> barrier(new TBarrier(commit->Step));
            Y_ENSURE(InFlyCompactionGcBarriers.emplace(commit->Step, barrier).second);
            GcLogic->HoldBarrier(barrier->Step);
        }

        if (commitResult.Commit) {
            AttachLeaseCommit(commitResult.Commit.Get());
            CommitManager->Commit(commitResult.Commit);
        }

        for (auto &affectedTable : change->Affects)
            CompactionLogic->UpdateInMemStatsStep(affectedTable, 1, Database->GetTableMemSize(affectedTable));

        if (commitResult.NeedFlush && !LogBatchFlushScheduled) {
            LogBatchFlushScheduled = true;

            auto delay = Scheme().Executor.LogFlushPeriod;
            if (LogFlushDelayOverrideUsec != -1) {
                delay = TDuration::MicroSeconds(LogFlushDelayOverrideUsec);
            }
            if (delay.MicroSeconds() == 0) {
                Send(SelfId(), new TEvents::TEvFlushLog());
            } else {
                Y_DEBUG_ABORT_UNLESS(delay < TDuration::Minutes(1));
                delay = Min(delay, TDuration::Seconds(59));
                Schedule(delay, new TEvents::TEvFlushLog());
            }
        }

        if (NeedFollowerSnapshot || LogicSnap->MayFlush(false))
            MakeLogSnapshot();

        CompactionLogic->UpdateLogUsage(LogicRedo->GrabLogUsage());
    }

    const ui64 bookkeepingTimeuS = ui64(1000000. * (currentBookkeepingTime + bookkeepingTimer.PassedReset()));
    const ui64 execTimeuS = ui64(1000000. * currentExecTime);

    Counters->Cumulative()[TExecutorCounters::TX_FINISHED].Increment(1);
    Counters->Percentile()[TExecutorCounters::TX_PERCENTILE_EXECUTE_CPUTIME].IncrementFor(execTimeuS);
    Counters->Percentile()[TExecutorCounters::TX_PERCENTILE_BOOKKEEPING_CPUTIME].IncrementFor(bookkeepingTimeuS);
    Counters->Cumulative()[TExecutorCounters::CONSUMED_CPU].Increment(execTimeuS + bookkeepingTimeuS);
    if (AppTxCounters && txType != UnknownTxType) {
        AppTxCounters->TxCumulative(txType, COUNTER_TT_EXECUTE_CPUTIME).Increment(execTimeuS);
        AppTxCounters->TxCumulative(txType, COUNTER_TT_BOOKKEEPING_CPUTIME).Increment(bookkeepingTimeuS);
    }

    if (ResourceMetrics) {
        ResourceMetrics->CPU.Increment(bookkeepingTimeuS + execTimeuS, Time->Now());
        ResourceMetrics->TryUpdate(SelfCtx());
    }

    MaybeRelaxRejectProbability();
}

void TExecutor::MakeLogSnapshot() {
    if (!LogicSnap->MayFlush(true) || PendingPartSwitches)
        return;

    NeedFollowerSnapshot = false;
    THPTimer makeLogSnapTimer;

    LogicRedo->FlushBatchedLog();

    auto commit = CommitManager->Begin(true, ECommit::Snap, {});

    NKikimrExecutorFlat::TLogSnapshot snap;

    snap.SetSerial(Database->Head(Max<ui32>()).Serial);

    if (auto *version = snap.MutableVersion()) {
        version->SetTail(ui32(NTable::ECompatibility::Head));
        version->SetHead(ui32(NTable::ECompatibility::Edge));
    }

    LogicAlter->SnapToLog(snap);
    LogicRedo->SnapToLog(snap);

    bool haveTxStatus = false;

    for (const auto& kvTable : Scheme().Tables) {
        const ui32 tableId = kvTable.first;
        auto state = CompactionLogic->SnapToLog(tableId);

        // Save state snapshot first (parts are merged into this)
        auto *change = snap.AddCompactionStates();
        change->SetTable(tableId);
        change->SetStrategy(state.Strategy);
        change->MutableKeyValues()->Reserve(state.State.StateSnapshot.size());
        for (const auto& kvState : state.State.StateSnapshot) {
            if (kvState.second) {
                auto *kvStateProto = change->AddKeyValues();
                kvStateProto->SetKey(kvState.first);
                kvStateProto->SetValue(kvState.second);
            }
        }

        auto dump = [&](const NTable::TPartView& partView) {
            ui32 level = state.State.PartLevels.Value(partView->Label, 255);

            TPageCollectionProtoHelper::Snap(snap.AddDbParts(), partView, tableId, level);
        };

        Database->EnumerateTableParts(tableId, std::move(dump));

        auto dumpCold = [&](const TIntrusiveConstPtr<NTable::TColdPart>& part) {
            ui32 level = state.State.PartLevels.Value(part->Label, 255);

            TPageCollectionProtoHelper::Snap(snap.AddDbParts(), part, tableId, level);
        };

        Database->EnumerateTableColdParts(tableId, std::move(dumpCold));

        auto dumpTxStatus = [&](const TIntrusiveConstPtr<NTable::TTxStatusPart>& part) {
            const auto* txStatus = dynamic_cast<const NTable::TTxStatusPartStore*>(part.Get());
            Y_ENSURE(txStatus);
            auto* p = snap.AddTxStatusParts();
            p->SetTable(tableId);
            auto* x = p->AddTxStatus();
            TLargeGlobIdProto::Put(*x->MutableDataId(), txStatus->GetDataId());
            x->SetEpoch(txStatus->Epoch.ToProto());
            haveTxStatus = true;
        };

        Database->EnumerateTableTxStatusParts(tableId, std::move(dumpTxStatus));
    }

    if (haveTxStatus) {
        // Make sure older versions won't try loading an incomplete snapshot
        ui32 tail = Max(ui32(28), snap.GetVersion().GetTail());
        snap.MutableVersion()->SetTail(tail);
    }

    for (const auto& kvTable : Scheme().Tables) {
        const ui32 tableId = kvTable.first;

        if (const auto& ranges = Database->GetRemovedRowVersions(tableId)) {
            auto *change = snap.AddRowVersionStates();
            change->SetTable(tableId);

            for (const auto& range : ranges) {
                auto *rangeProto = change->AddRemovedRanges();

                auto *lower = rangeProto->MutableLower();
                lower->SetStep(range.Lower.Step);
                lower->SetTxId(range.Lower.TxId);

                auto *upper = rangeProto->MutableUpper();
                upper->SetStep(range.Upper.Step);
                upper->SetTxId(range.Upper.TxId);
            }
        }
    }

    BorrowLogic->SnapToLog(snap, *commit);
    GcLogic->SnapToLog(snap, commit->Step);
    LogicSnap->MakeSnap(snap, *commit, Logger.Get());
    VacuumLogic->OnMakeLogSnapshot(Generation(), commit->Step);

    AttachLeaseCommit(commit.Get(), /* force */ true);
    CommitManager->Commit(commit);

    CompactionLogic->UpdateLogUsage(LogicRedo->GrabLogUsage());

    const ui64 makeLogSnapTimeuS = ui64(1000000. * makeLogSnapTimer.Passed());
    Counters->Percentile()[TExecutorCounters::TX_PERCENTILE_LOGSNAP_CPUTIME].IncrementFor(makeLogSnapTimeuS);
}

void TExecutor::Handle(TEvPrivate::TEvActivateExecution::TPtr &ev, const TActorContext &ctx) {
    Y_UNUSED(ev);
    Y_UNUSED(ctx);
    Y_ENSURE(ActivateTransactionInFlight > 0);
    ActivateTransactionInFlight--;

    if (!CanExecuteTransaction())
        return;

    if (ActivationQueue) {
        TSeat* seat = ActivationQueue.PopFront();
        Y_ENSURE(seat->State == ESeatState::Active);
        seat->State = ESeatState::None;
        Y_ENSURE(ActivateTransactionWaiting > 0);
        ActivateTransactionWaiting--;
        seat->FinishEnqueuedSpan();
        ExecuteTransaction(seat);
    } else {
        // N.B. it should actually never happen, since ActivationQueue size
        // is always exactly equal to ActivateTransactionWaiting and we never
        // have more ActivateTransactionInFlight events that these waiting
        // transactions, so when we handle this event we must have at least
        // one transaction in queue.
        Y_ENSURE(ActivateTransactionWaiting == 0);
    }
}

void TExecutor::Handle(TEvPrivate::TEvActivateLowExecution::TPtr &ev, const TActorContext &ctx) {
    Y_UNUSED(ev);
    Y_UNUSED(ctx);
    Y_ENSURE(ActivateLowTransactionInFlight > 0);
    ActivateLowTransactionInFlight--;

    if (!CanExecuteTransaction())
        return;

    if (ActivationLowQueue) {
        TSeat* seat = ActivationLowQueue.PopFront();
        Y_ENSURE(seat->State == ESeatState::ActiveLow);
        seat->State = ESeatState::None;
        Y_ENSURE(ActivateLowTransactionWaiting > 0);
        ActivateLowTransactionWaiting--;
        seat->FinishEnqueuedSpan();

        // Activate the next transaction
        if (ActivateLowTransactionWaiting > 0 && ActivateLowTransactionInFlight < 1) {
            Send(SelfId(), new TEvPrivate::TEvActivateLowExecution());
            ++ActivateLowTransactionInFlight;
        }

        ExecuteTransaction(seat);
    } else {
        Y_ENSURE(ActivateLowTransactionWaiting == 0);
    }
}

void TExecutor::Handle(TEvPrivate::TEvBrokenTransaction::TPtr &ev, const TActorContext &ctx) {
    Y_UNUSED(ev);
    Y_UNUSED(ctx);
    Y_ENSURE(BrokenTransaction);

    return Broken();
}

void TExecutor::Wakeup(TEvents::TEvWakeup::TPtr &ev, const TActorContext&) {
    if (ev->Get()->Tag == ui64(EWakeTag::Memory)) {
        Memory->RunMemoryGC();
    } else {
        Y_TABLET_ERROR("Unknown TExecutor module wakeup tag " << ev->Get()->Tag);
    }
}

void TExecutor::Handle(TEvents::TEvFlushLog::TPtr &ev) {
    Y_UNUSED(ev);
    LogBatchFlushScheduled = false;
    LogicRedo->FlushBatchedLog();
    CompactionLogic->UpdateLogUsage(LogicRedo->GrabLogUsage());
}

void TExecutor::Handle(NSharedCache::TEvResult::TPtr &ev) {
    NSharedCache::TEvResult *msg = ev->Get();
    const bool failed = (msg->Status != NKikimrProto::OK);
    const auto requestType = ESharedCacheRequestType(ev->Cookie);

    if (auto logl = Logger->Log(failed ? ELnLev::Info : ELnLev::Debug)) {
        logl
            << NFmt::Do(*this) << " got result " << NFmt::Do(*ev->Get())
            << ", type " << ui64(requestType);
    }

    switch (requestType) {
    case ESharedCacheRequestType::Transaction:
    case ESharedCacheRequestType::InMemPages:
        {
            TPrivatePageCache::TInfo *collectionInfo = PrivatePageCache->Info(msg->PageCollection->Label());
            if (!collectionInfo) {
                if (requestType == ESharedCacheRequestType::Transaction) {
                    TryActivateWaitingTransaction(std::move(msg->WaitPad));
                }
                return;
            }

            if (msg->Status != NKikimrProto::OK) { // collection is still active but we got bs error. no choice then die
                if (auto logl = Logger->Log(ELnLev::Error)) {
                    logl << NFmt::Do(*this) << " Broken on page collection request error " << NFmt::Do(*ev->Get());
                }

                if (msg->Status == NKikimrProto::NODATA) {
                    GetServiceCounters(AppData()->Counters, "tablets")->GetCounter("alerts_req_nodata", true)->Inc();
                }

                return Broken();
            }

            if (requestType == ESharedCacheRequestType::InMemPages) {
                StickInMemPages(msg);
            }
            for (auto& loaded : msg->Pages) {
                PrivatePageCache->ProvideBlock(std::move(loaded), collectionInfo);
            }
            if (requestType == ESharedCacheRequestType::Transaction) {
                TryActivateWaitingTransaction(std::move(msg->WaitPad));
            }
        }
        return;

    case ESharedCacheRequestType::PendingInit:
        {
            const auto *pageCollection = msg->PageCollection.Get();
            TPendingPartSwitch *foundSwitch = nullptr;
            TPendingPartSwitch::TNewBundle *foundBundle = nullptr;
            TPendingPartSwitch::TLoaderStage *foundStage = nullptr;
            for (auto &p : PendingPartSwitches) {
                for (auto &bundle : p.NewBundles) {
                    if (auto *stage = bundle.GetStage<TPendingPartSwitch::TLoaderStage>()) {
                        if (stage->Fetching == pageCollection) {
                            foundSwitch = &p;
                            foundBundle = &bundle;
                            foundStage = stage;
                            break;
                        }
                    }
                }
                if (foundStage)
                    break;
            }

            // nope. just ignore.
            if (!foundStage)
                return;

            foundStage->Fetching = nullptr;

            if (msg->Status != NKikimrProto::OK) {
                if (auto logl = Logger->Log(ELnLev::Error)) {
                    logl << NFmt::Do(*this) << " Broken while pending part init" << NFmt::Do(*ev->Get());
                }

                if (msg->Status == NKikimrProto::NODATA) {
                    GetServiceCounters(AppData()->Counters, "tablets")->GetCounter("alerts_pending_nodata", true)->Inc();
                }

                return Broken();
            }

            foundStage->Loader.Save(msg->Cookie, msg->Pages);
            foundSwitch->PendingLoads--;

            if (PrepareExternalPart(*foundSwitch, *foundBundle)) {
                // Waiting for more pages
                return;
            }

            AdvancePendingPartSwitches();
        }
        return;

    default:
        Y_DEBUG_ABORT_S("Unexpected request " << ev->Cookie);
        break;
    }
}

void TExecutor::Handle(NSharedCache::TEvUpdated::TPtr &ev) {
    const auto *msg = ev->Get();

    for (auto &kv : msg->DroppedPages) {
        if (auto *info = PrivatePageCache->Info(kv.first)) {
            for (ui32 pageId : kv.second) {
                PrivatePageCache->DropSharedBody(info, pageId);
            }
        }
    }
}

void TExecutor::Handle(TEvTablet::TEvDropLease::TPtr &ev, const TActorContext &ctx) {
    TMonotonic ts = AppData(ctx)->MonotonicTimeProvider->Now();

    LeaseDropped = true;
    LeaseEnd = Min(LeaseEnd, ts);

    for (auto& l : LeaseCommits) {
        if (l.LeaseEnd > ts) {
            LeaseCommitsByEnd.erase(l.ByEndIterator);
            l.LeaseEnd = ts;
            l.ByEndIterator = LeaseCommitsByEnd.emplace(l.LeaseEnd, &l);
        }
    }

    ctx.Send(ev->Sender, new TEvTablet::TEvLeaseDropped);
    Owner->ReadOnlyLeaseDropped();
}

void TExecutor::Handle(TEvPrivate::TEvLeaseExtend::TPtr &, const TActorContext &) {
    Y_ENSURE(LeaseExtendPending);
    LeaseExtendPending = false;

    if (!LeaseCommits.empty() || !LeaseEnabled || LeaseDropped) {
        return;
    }

    // It is possible lease was extended while this event was pending
    TMonotonic now = TActivationContext::Monotonic();
    TMonotonic deadline = LeaseEnd - LeaseDuration / 3;
    if (now < deadline) {
        Schedule(deadline, new TEvPrivate::TEvLeaseExtend);
        LeaseExtendPending = true;
        return;
    }

    if (LeaseUsed) {
        LeaseUsed = false;
        UnusedLeaseExtensions = 0;
    } else if (UnusedLeaseExtensions >= 5) {
        return;
    } else {
        ++UnusedLeaseExtensions;
    }

    // Start a new lease extension commit
    EnsureReadOnlyLease(LeaseEnd);
}

void TExecutor::Handle(TEvTablet::TEvConfirmLeaderResult::TPtr &ev) {
    auto *msg = ev->Get();

    // Note: lease confirmation error will currenly cause tablet to stop anyway
    if (msg->Status != NKikimrProto::OK) {
        if (auto logl = Logger->Log(ELnLev::Error)) {
            logl << NFmt::Do(*this) << " Broken on lease confirmation";
        }
        return Broken();
    }

    LeaseConfirmed(ev->Cookie);
}

void TExecutor::Handle(TEvTablet::TEvCommitResult::TPtr &ev, const TActorContext &ctx) {
    TEvTablet::TEvCommitResult *msg = ev->Get();

    if (msg->Status != NKikimrProto::OK) {
        if (auto logl = Logger->Log(ELnLev::Error)) {
            logl << NFmt::Do(*this) << " Broken on commit error for step " << msg->Step;
        }
        return Broken();
    }

    Y_ENSURE(msg->Generation == Generation());
    const ui32 step = msg->Step;

    TActiveTransactionZone activeTransaction(this);

    GcLogic->OnCommitLog(step, msg->ConfirmedOnSend, ctx);
    CommitManager->Confirm(step);

    const auto cookie = static_cast<ECommit>(ev->Cookie);

    if (auto logl = Logger->Log(ELnLev::Debug)) {
        logl
            << NFmt::Do(*this) << " commited cookie " << int(cookie)
            << " for step " << step;
    }

    if (!LeaseCommitsByStep.Empty()) {
        auto* l = LeaseCommitsByStep.Front();
        Y_ENSURE(step <= l->Step);
        if (step == l->Step) {
            LeasePersisted = true;
            LeaseConfirmed(l->Cookie);
        }
    }

    switch (cookie) {
    case ECommit::Redo:
        {
            // Note: per-tx InFly is decrememnted in txloglogic
            const ui64 confirmedTransactions = LogicRedo->Confirm(step, ctx, OwnerActorId);
            Stats->TxInFly -= confirmedTransactions;
            Counters->Simple()[TExecutorCounters::DB_TX_IN_FLY] = Stats->TxInFly;

            auto snapCollectionIt = InFlySnapCollectionBarriers.find(step);
            if (snapCollectionIt != InFlySnapCollectionBarriers.end()) {
                for (auto &x : snapCollectionIt->second)
                    CheckCollectionBarrier(x);
                InFlySnapCollectionBarriers.erase(snapCollectionIt);
            }
        }
        break;
    case ECommit::Snap:
        LogicSnap->Confirm(msg->Step);

        VacuumLogic->OnSnapshotCommited(Generation(), step);
        if (NeedFollowerSnapshot || VacuumLogic->NeedLogSnaphot())
            MakeLogSnapshot();

        break;
    case ECommit::Data:
        {
            auto it = InFlyCompactionGcBarriers.find(step);
            Y_ENSURE(it != InFlyCompactionGcBarriers.end());
            // just check, real barrier release on follower gc ack
        }

        // any action on snapshot commit?
        break;
    case ECommit::Misc:
        break;
    default:
        Y_TABLET_ERROR("unknown event cookie");
    }

    Database->UpdateApproximateFreeSharesByChannel(msg->ApproximateFreeSpaceShareByChannel);

    CheckYellow(std::move(msg->YellowMoveChannels), std::move(msg->YellowStopChannels));

    ProcessIoStats(
        NBlockIO::EDir::Write, NBlockIO::EPriority::Fast,
        std::move(msg->GroupWrittenBytes),
        std::move(msg->GroupWrittenOps),
        ctx);

    activeTransaction.Done();
    PlanTransactionActivation();

    MaybeRelaxRejectProbability();
}

void TExecutor::Handle(TEvBlobStorage::TEvCollectGarbageResult::TPtr &ev) {
    if (auto retryDelay = GcLogic->OnCollectGarbageResult(ev)) {
        Schedule(retryDelay, new TEvPrivate::TEvRetryGcRequest(ev->Get()->Channel));
    }
    VacuumLogic->OnCollectedGarbage(OwnerCtx());
}

void TExecutor::Handle(TEvPrivate::TEvRetryGcRequest::TPtr &ev, const TActorContext &ctx) {
    GcLogic->RetryGcRequests(ev->Get()->Channel, ctx);
}

void TExecutor::Handle(TEvResourceBroker::TEvResourceAllocated::TPtr &ev) {
    auto *msg = ev->Get();
    if (!msg->Cookie.Get()) {
        // Generic broker is not using cookies
        Broker->OnResourceAllocated(msg->TaskId);
        return;
    }

    auto *cookie = CheckedCast<TResource*>(msg->Cookie.Get());

    switch (cookie->Source) {
    case TResource::ESource::Seat:
        return StartSeat(msg->TaskId, cookie);
    case TResource::ESource::Scan:
        return StartScan(msg->TaskId, cookie);
    default:
        Y_TABLET_ERROR("unexpected resource source");
    }
}

void TExecutor::StartSeat(ui64 task, TResource *cookie_)
{
    auto *cookie = CheckedCast<TMemory::TCookie*>(cookie_);
    TSeat* seat = cookie->Seat;
    Y_ENSURE(seat->State == ESeatState::Postponed);
    PostponedTransactions.Remove(seat);
    seat->State = ESeatState::None;
    Memory->AcquiredMemory(*seat, task);

    if (seat->Cancelled) {
        FinishCancellation(seat);
        return;
    }

    EnqueueActivation(seat, CanExecuteTransaction());
}

THolder<TScanSnapshot> TExecutor::PrepareScanSnapshot(ui32 table, const NTable::TCompactionParams *params, TRowVersion snapshot)
{
    LogicRedo->FlushBatchedLog();

    auto commit = CommitManager->Begin(true, ECommit::Misc, {});

    if (params && params->Edge.Head == NTable::TEpoch::Max()) {
        auto redo = Database->SnapshotToLog(table, { Generation(), commit->Step });
        LogicRedo->MakeLogEntry(*commit, std::move(redo), { table }, true);
    }

    TIntrusivePtr<TBarrier> barrier = new TBarrier(commit->Step);

    AttachLeaseCommit(commit.Get());
    CommitManager->Commit(commit);

    TAutoPtr<NTable::TSubset> subset;

    if (params) {
        subset = Database->CompactionSubset(table, params->Edge.Head, { });

        if (params->Parts) {
            subset->Flatten.insert(subset->Flatten.end(), params->Parts.begin(), params->Parts.end());
        }

        if (params->ColdParts) {
            subset->ColdParts.insert(subset->ColdParts.end(), params->ColdParts.begin(), params->ColdParts.end());
        }

        if (*subset) {
            Y_ENSURE(subset->IsStickedToHead(),
                "Got table subset with unexpected head " << subset->Head
                << " and epoch " << subset->Epoch());
        }
    } else {
        // This grabs a volatile snapshot of the mutable table state
        subset = Database->ScanSnapshot(table, snapshot);
    }

    GcLogic->HoldBarrier(barrier->Step);
    CompactionLogic->UpdateLogUsage(LogicRedo->GrabLogUsage());

    if (LogicSnap->MayFlush(false)) {
        MakeLogSnapshot();
    }

    return THolder<TScanSnapshot>(new TScanSnapshot{table, std::move(barrier), subset, snapshot});
}

void TExecutor::StartScan(ui64 serial, ui32 table)
{
    Y_UNUSED(table);
    Scans->Start(serial);
}

void TExecutor::StartScan(ui64 task, TResource *cookie)
{
    if (auto acquired = Scans->Acquired(task, cookie)) {
        StartScan(acquired.Serial, acquired.Table);
    }
}

void TExecutor::ProcessIoStats(
        NBlockIO::EDir dir, NBlockIO::EPriority priority,
        ui64 bytes, ui64 ops,
        NBlockIO::TEvStat::TByCnGr&& groupBytes,
        NBlockIO::TEvStat::TByCnGr&& groupOps,
        const TActorContext& ctx)
{
    if (auto *metrics = ResourceMetrics.Get()) {
        auto &bandBytes = dir == NBlockIO::EDir::Read ? metrics->ReadThroughput : metrics->WriteThroughput;

        for (auto &it: groupBytes)
            bandBytes[it.first].Increment(it.second, Time->Now());

        auto &bandOps = dir == NBlockIO::EDir::Read ? metrics->ReadIops : metrics->WriteIops;

        for (auto &it: groupOps)
            bandOps[it.first].Increment(it.second, Time->Now());

        metrics->TryUpdate(ctx);
    }

    if (priority == NBlockIO::EPriority::Bulk) {
        switch (dir) {
            case NBlockIO::EDir::Read:
                Counters->Cumulative()[TExecutorCounters::COMP_BYTES_READ].Increment(bytes);
                Counters->Cumulative()[TExecutorCounters::COMP_BLOBS_READ].Increment(ops);
                break;
            case NBlockIO::EDir::Write:
                Counters->Cumulative()[TExecutorCounters::COMP_BYTES_WRITTEN].Increment(bytes);
                Counters->Cumulative()[TExecutorCounters::COMP_BLOBS_WRITTEN].Increment(ops);
                break;
        }
    } else {
        switch (dir) {
            case NBlockIO::EDir::Read:
                Counters->Cumulative()[TExecutorCounters::TABLET_BYTES_READ].Increment(bytes);
                Counters->Cumulative()[TExecutorCounters::TABLET_BLOBS_READ].Increment(ops);
                break;
            case NBlockIO::EDir::Write:
                Counters->Cumulative()[TExecutorCounters::TABLET_BYTES_WRITTEN].Increment(bytes);
                Counters->Cumulative()[TExecutorCounters::TABLET_BLOBS_WRITTEN].Increment(ops);
                break;
        }
    }
}

void TExecutor::ProcessIoStats(
        NBlockIO::EDir dir, NBlockIO::EPriority priority,
        NBlockIO::TEvStat::TByCnGr&& groupBytes,
        NBlockIO::TEvStat::TByCnGr&& groupOps,
        const TActorContext& ctx)
{
    ui64 totalBytes = 0;
    for (auto& kv : groupBytes) {
        totalBytes += kv.second;
    }

    ui64 totalOps = 0;
    for (auto& kv : groupOps) {
        totalOps += kv.second;
    }

    ProcessIoStats(
        dir, priority,
        totalBytes, totalOps,
        std::move(groupBytes),
        std::move(groupOps),
        ctx);
}

void TExecutor::Handle(NBlockIO::TEvStat::TPtr &ev, const TActorContext &ctx) {
    auto *msg = ev->Get();

    ProcessIoStats(
        msg->Dir, msg->Priority,
        msg->Bytes, msg->Ops,
        std::move(msg->GroupBytes),
        std::move(msg->GroupOps),
        ctx);
}

void TExecutor::UtilizeSubset(const NTable::TSubset &subset,
        const NTable::NFwd::TSeen &seen,
        THashSet<TLogoBlobID> reusedBundles,
        TLogCommit *commit)
{
    if (seen.Sieve.size() == subset.Flatten.size() + 1) {
        /* The last TSieve, if present, corresponds to external blobs of all
            compacted TMemTable tables, this pseudo NPage::TBlobs is generated by
            NFwd blobs tracer for this GC logic and may bypass borrow logic
            since TMemTable cannot be borrowed.
         */

        seen.Sieve.back().MaterializeTo(commit->GcDelta.Deleted);
    } else if (seen.Sieve.size() != subset.Flatten.size()) {
        Y_TABLET_ERROR("Got an unexpected TSieve items count after compaction");
    }

    for (auto it : xrange(subset.Flatten.size())) {
        auto *partStore = subset.Flatten[it].As<const NTable::TPartStore>();

        Y_ENSURE(seen.Sieve[it].Blobs.Get() == partStore->Blobs.Get());

        if (reusedBundles.contains(partStore->Label)) {
            // Delete only compacted large blobs at this moment
            if (BorrowLogic->BundlePartiallyCompacted(*partStore, seen.Sieve[it], commit)) {
                seen.Sieve[it].MaterializeTo(commit->GcDelta.Deleted);
            }

            continue;
        }

        if (BorrowLogic->BundleCompacted(*partStore, seen.Sieve[it], commit)) {
            partStore->SaveAllBlobIdsTo(commit->GcDelta.Deleted);

            seen.Sieve[it].MaterializeTo(commit->GcDelta.Deleted);
        }

        DropCachesOfBundle(*partStore);
    }

    for (auto it : xrange(subset.ColdParts.size())) {
        auto *part = subset.ColdParts[it].Get();

        Y_ENSURE(!reusedBundles.contains(part->Label));

        BorrowLogic->BundleCompacted(part->Label, commit);
    }

    for (auto it : xrange(subset.TxStatus.size())) {
        auto *partStore = dynamic_cast<const NTable::TTxStatusPartStore*>(subset.TxStatus[it].Get());
        Y_ENSURE(partStore, "Unexpected failure to cast TxStatus to an implementation type");

        if (BorrowLogic->BundleCompacted(*partStore, commit)) {
            partStore->SaveAllBlobIdsTo(commit->GcDelta.Deleted);
        }
    }

    Counters->Cumulative()[TExecutorCounters::DB_ELOBS_ITEMS_GONE].Increment(seen.Total - seen.Seen);
}

void TExecutor::ReleaseScanLocks(TIntrusivePtr<TBarrier> barrier, const NTable::TSubset &subset)
{
    Y_UNUSED(subset);

    CheckCollectionBarrier(barrier);
}

void TExecutor::Handle(NOps::TEvScanStat::TPtr &ev, const TActorContext &ctx) {
    auto *msg = ev->Get();

    if (ResourceMetrics) {
        ResourceMetrics->CPU.Increment(msg->ElapsedUs, Time->Now());
        ResourceMetrics->TryUpdate(ctx);
    }
}

void TExecutor::Handle(NOps::TEvResult::TPtr &ev) {
    auto *msg = ev->Get();

    const auto outcome = Scans->Release(msg->Serial, msg->Status, msg->Result);
    if (outcome.System) {
        /* System scans are used for compactions and specially handled */
        Handle(msg, CheckedCast<TProdCompact*>(msg->Result.Get()), outcome.Cancelled);
    }

    ReleaseScanLocks(std::move(msg->Barrier), *msg->Subset);
}

void TExecutor::Handle(NOps::TEvResult *ops, TProdCompact *msg, bool cancelled) {
    THPTimer partSwitchCpuTimer;

    if (msg->Params->TaskId != 0) {
        // We have taken over this task, mark it as finished in the broker
        auto status = cancelled ? EResourceStatus::Cancelled : EResourceStatus::Finished;
        Broker->FinishTask(msg->Params->TaskId, status);
    }

    const ui32 tableId = msg->Params->Table;

    const bool abandoned = cancelled || !Scheme().GetTableInfo(tableId);

    TProdCompact::TResults results = std::move(msg->Results);
    TVector<TIntrusiveConstPtr<NTable::TTxStatusPart>> newTxStatus = std::move(msg->TxStatus);

    if (auto logl = Logger->Log(msg->Success ? ELnLev::Info : ELnLev::Error)) {
        logl
            << NFmt::Do(*this) << " Compact " << ops->Serial
            << " on " << NFmt::Do(*msg->Params) << " step " << msg->Step
            << ", product {"
            << (newTxStatus ? "tx status + " : "")
            << results.size() << " parts"
            << " epoch " << ops->Subset->Head << "} ";

        if (abandoned) {
            logl << "thrown";
        } else if (!msg->Success) {
            logl << "failed";
        } else {
            logl << "done";
        }
    }

    if (abandoned) {
        if (cancelled && Scheme().GetTableInfo(tableId)) {
            CompactionLogic->CancelledCompaction(ops->Serial, std::move(msg->Params));
        }
        return;
    } else if (!msg->Success) {
        if (auto logl = Logger->Log(ELnLev::Error)) {
            logl << NFmt::Do(*this) << " Broken on compaction error";
        }

        if (msg->Exception) {
            std::rethrow_exception(msg->Exception);
        }

        CheckYellow(std::move(msg->YellowMoveChannels), std::move(msg->YellowStopChannels), /* terminal */ true);
        return Broken();
    }

    TActiveTransactionZone activeTransaction(this);

    const ui64 snapStamp = msg->Params->Edge.TxStamp ? msg->Params->Edge.TxStamp
        : MakeGenStepPair(Generation(), msg->Step);

    LogicRedo->FlushBatchedLog();

    // now apply effects
    NKikimrExecutorFlat::TTablePartSwitch proto;
    proto.SetTableId(tableId);

    NKikimrExecutorFlat::TFollowerPartSwitchAux aux;

    auto commit = CommitManager->Begin(true, ECommit::Data, {});

    commit->WaitFollowerGcAck = true;

    const bool hadFrozen = bool(ops->Subset->Frozen);
    if (ops->Subset->Head > NTable::TEpoch::Zero()) {
        // Some compactions (e.g. triggered by log overhead after many scans)
        // may have no TMemTable inputs, we still want to cut log since it's
        // effectively a snapshot.
        Y_ENSURE(msg->Params->Edge.Head > NTable::TEpoch::Zero());
        LogicRedo->CutLog(tableId, { snapStamp, ops->Subset->Head }, commit->GcDelta);
        auto *sx = proto.MutableTableSnapshoted();
        sx->SetTable(tableId);
        sx->SetGeneration(ExpandGenStepPair(snapStamp).first);
        sx->SetStep(ExpandGenStepPair(snapStamp).second);
        sx->SetHead(ops->Subset->Head.ToProto());
    } else {
        Y_ENSURE(!hadFrozen, "Compacted frozen parts without correct head epoch");
    }

    if (results) {
        auto &gcDiscovered = commit->GcDelta.Created;

        for (const auto &result : results) {
            const auto &newPart = result.Part;

            AddCachesOfBundle(newPart);

            auto *partStore = newPart.As<NTable::TPartStore>();

            { /*_ enum all new blobs (include external) to gc logic */
                partStore->SaveAllBlobIdsTo(commit->GcDelta.Created);

                for (auto &hole: result.Growth)
                    for (auto seq: xrange(hole.Begin, hole.End))
                        gcDiscovered.push_back(partStore->Blobs->Glob(seq).Logo);
            }
        }
    }

    if (newTxStatus) {
        for (const auto &txStatus : newTxStatus) {
            auto *partStore = dynamic_cast<const NTable::TTxStatusPartStore*>(txStatus.Get());
            Y_ENSURE(partStore);
            partStore->SaveAllBlobIdsTo(commit->GcDelta.Created);
        }
    }

    { /*_ Check that all external blobs will be accounted in GC logic */
        ui64 totalBlobs = 0;
        ui64 totalGrow = 0;
        for (const auto &result : results) {
            totalBlobs += result.Part->Blobs ? result.Part->Blobs->Total() : 0;
            totalGrow += NTable::TScreen::Sum(result.Growth);
        }

        Y_ENSURE(ops->Trace->Seen + totalGrow == totalBlobs);

        Counters->Cumulative()[TExecutorCounters::DB_ELOBS_ITEMS_GROW].Increment(totalGrow);
    }

    THashMap<TLogoBlobID, NKikimrExecutorFlat::TBundleChange*> bundleChanges;

    { /*_ Replace original subset with compacted results */
        TVector<NTable::TPartView> newParts(Reserve(results.size()));
        for (const auto& result : results) {
            newParts.emplace_back(result.Part);
        }

        Database->Replace(tableId, *ops->Subset, newParts, newTxStatus);

        TVector<TLogoBlobID> bundles(Reserve(ops->Subset->Flatten.size() + ops->Subset->ColdParts.size()));
        for (auto &part: ops->Subset->Flatten) {
            bundles.push_back(part->Label);
        }
        for (auto &part: ops->Subset->ColdParts) {
            bundles.push_back(part->Label);
        }

        auto updatedSlices = Database->LookupSlices(tableId, bundles);

        THashSet<TLogoBlobID> reusedBundles;
        for (auto &part: ops->Subset->Flatten) {
            if (updatedSlices.contains(part->Label)) {
                reusedBundles.insert(part->Label);
            }
        }
        for (auto &part: ops->Subset->ColdParts) {
            Y_ENSURE(!updatedSlices.contains(part->Label));
        }

        UtilizeSubset(*ops->Subset, *ops->Trace, std::move(reusedBundles), commit.Get());

        for (auto &gone: ops->Subset->Flatten) {
            if (auto *found = updatedSlices.FindPtr(gone->Label)) {
                auto *deltaProto = proto.AddBundleDeltas();
                LogoBlobIDFromLogoBlobID(gone->Label, deltaProto->MutableLabel());
                deltaProto->SetDelta(NTable::TOverlay::EncodeRemoveSlices(gone.Slices));
            } else {
                LogoBlobIDFromLogoBlobID(gone->Label, proto.AddLeavingBundles());
            }
        }
        for (auto &gone: ops->Subset->ColdParts) {
            LogoBlobIDFromLogoBlobID(gone->Label, proto.AddLeavingBundles());
        }
        for (auto &gone: ops->Subset->TxStatus) {
            LogoBlobIDFromLogoBlobID(gone->Label, proto.AddLeavingTxStatus());
        }
    }

    // We have applied all effects, time to notify compaction of completion

    auto compactionResult = MakeHolder<NTable::TCompactionResult>(
        results ? results.front().Part.Epoch() : NTable::TEpoch::Max(),
        results.size());
    for (const auto& result : results) {
        compactionResult->Parts.emplace_back(result.Part);
    }

    const auto logicResult = CompactionLogic->CompleteCompaction(
        ops->Serial,
        std::move(msg->Params),
        std::move(compactionResult));

    // Compaction applied effects too, time to serialize part switches

    TCompactionChangesCtx changesCtx(proto, &results);
    ApplyCompactionChanges(changesCtx, logicResult.Changes, logicResult.Strategy);

    NKikimrExecutorFlat::TFollowerPartSwitchAux::TBySwitch *bySwitchAux = nullptr;
    if (results || newTxStatus) {
        bySwitchAux = aux.AddBySwitchAux();
    }

    if (results) {
        auto *snap = proto.MutableIntroducedParts();

        for (const auto &result : results) {
            const auto &newPart = result.Part;

            TPageCollectionProtoHelper::Snap(snap, newPart, tableId, logicResult.Changes.NewPartsLevel);
            TPageCollectionProtoHelper(true).Do(bySwitchAux->AddHotBundles(), newPart);
        }
    }

    if (newTxStatus) {
        auto *p = proto.MutableIntroducedTxStatus();
        p->SetTable(tableId);
        for (const auto &txStatus : newTxStatus) {
            auto *partStore = dynamic_cast<const NTable::TTxStatusPartStore*>(txStatus.Get());
            Y_ENSURE(partStore);
            {
                auto *x = p->AddTxStatus();
                TLargeGlobIdProto::Put(*x->MutableDataId(), partStore->GetDataId());
                x->SetEpoch(partStore->Epoch.ToProto());
            }
            {
                auto *x = bySwitchAux->AddHotTxStatus();
                TLargeGlobIdProto::Put(*x->MutableDataId(), partStore->GetDataId());
                x->SetEpoch(partStore->Epoch.ToProto());
                // Send small tx status data together with the aux message
                if (partStore->TxStatusPage->GetRaw().size() <= 131072) {
                    x->SetData(partStore->TxStatusPage->GetRaw().ToString());
                }
            }
        }
    }

    { /*_ Finalize switch (turn) blob and attach it to commit */
        auto body = proto.SerializeAsString();
        auto glob = CommitManager->Turns.One(commit->Refs, std::move(body), true);

        if (bySwitchAux)
            LogoBlobIDFromLogoBlobID(glob.Logo, bySwitchAux->MutablePartSwitchRef());
    }

    commit->FollowerAux = NPageCollection::TSlicer::Lz4()->Encode(aux.SerializeAsString());

    Y_ENSURE(InFlyCompactionGcBarriers.emplace(commit->Step, ops->Barrier).second);

    VacuumLogic->OnCompleteCompaction(tableId, CompactionLogic->GetFinishedCompactionInfo(tableId));

    AttachLeaseCommit(commit.Get());
    CommitManager->Commit(commit);

    if (hadFrozen || logicResult.MemCompacted)
        CompactionLogic->UpdateInMemStatsStep(tableId, 0, Database->GetTableMemSize(tableId));

    CompactionLogic->UpdateLogUsage(LogicRedo->GrabLogUsage());

    const ui64 partSwitchCpuUs = ui64(1000000. * partSwitchCpuTimer.Passed());
    Counters->Percentile()[TExecutorCounters::TX_PERCENTILE_PARTSWITCH_CPUTIME].IncrementFor(partSwitchCpuUs);

    if (msg->YellowMoveChannels || msg->YellowStopChannels) {
        CheckYellow(std::move(msg->YellowMoveChannels), std::move(msg->YellowStopChannels));
    }

    for (auto &snap : logicResult.CompleteSnapshots) {
        if (snap->Impl->Complete(tableId, ops->Barrier)) {
            auto snapIt = WaitingSnapshots.find(snap.Get());
            Y_ENSURE(snapIt != WaitingSnapshots.end());
            TIntrusivePtr<TTableSnapshotContext> snapCtxPtr = snapIt->second;
            WaitingSnapshots.erase(snapIt);

            Owner->SnapshotComplete(snapCtxPtr, OwnerCtx());
        }
    }

    Owner->CompactionComplete(tableId, OwnerCtx());
    MaybeRelaxRejectProbability();

    activeTransaction.Done();

    if (LogicSnap->MayFlush(false) || VacuumLogic->NeedLogSnaphot()) {
        MakeLogSnapshot();
    }
}

void TExecutor::UpdateUsedTabletMemory() {
    // Estimate memory usage for internal executor structures:
    UsedTabletMemory = 50_KB;

    // Count the number of bytes that can't be offloaded right now:
    UsedTabletMemory += StickyPagesMemory;
    UsedTabletMemory += TransactionPagesMemory;

    // Estimate memory used by internal database structures:
    auto &counters = Database->Counters();
    UsedTabletMemory += counters.MemTableWaste;
    UsedTabletMemory += counters.MemTableBytes;
    UsedTabletMemory += counters.Parts.OtherBytes;
    UsedTabletMemory += Stats->PacksMetaBytes;

    // Add tablet memory usage:
    UsedTabletMemory += Owner->GetMemoryUsage();
}

void TExecutor::UpdateCounters(const TActorContext &ctx) {
    TAutoPtr<TTabletCountersBase> executorCounters;
    TAutoPtr<TTabletCountersBase> externalTabletCounters;

    if (CounterEventsInFlight.RefCount() == 1) {
        UpdateUsedTabletMemory();

        if (Counters) {

            const auto& dbCounters = Database->Counters();

            { /* Memory consumption of common for leader and follower components */
                Counters->Simple()[TExecutorCounters::DB_WARM_BYTES].Set(dbCounters.MemTableBytes);
                Counters->Simple()[TExecutorCounters::DB_META_BYTES].Set(Stats->PacksMetaBytes);
                Counters->Simple()[TExecutorCounters::DB_FLAT_INDEX_BYTES].Set(dbCounters.Parts.FlatIndexBytes);
                Counters->Simple()[TExecutorCounters::DB_B_TREE_INDEX_BYTES].Set(dbCounters.Parts.BTreeIndexBytes);
                Counters->Simple()[TExecutorCounters::DB_INDEX_BYTES].Set(dbCounters.Parts.FlatIndexBytes + dbCounters.Parts.BTreeIndexBytes);
                Counters->Simple()[TExecutorCounters::DB_OTHER_BYTES].Set(dbCounters.Parts.OtherBytes);
                Counters->Simple()[TExecutorCounters::DB_BYKEY_BYTES].Set(dbCounters.Parts.ByKeyBytes);
                Counters->Simple()[TExecutorCounters::USED_TABLET_MEMORY].Set(UsedTabletMemory);
            }

            // Runtime stats related to uncommitted changes
            auto runtimeCounters = Database->RuntimeCounters();
            {
                Counters->Simple()[TExecutorCounters::DB_OPEN_TX_COUNT].Set(runtimeCounters.OpenTxCount);
                Counters->Simple()[TExecutorCounters::DB_TXS_WITH_DATA_COUNT].Set(runtimeCounters.TxsWithDataCount);
                Counters->Simple()[TExecutorCounters::DB_COMMITTED_TX_COUNT].Set(runtimeCounters.CommittedTxCount);
                Counters->Simple()[TExecutorCounters::DB_REMOVED_TX_COUNT].Set(runtimeCounters.RemovedTxCount);
                Counters->Simple()[TExecutorCounters::DB_REMOVED_COMMITTED_TXS].Set(runtimeCounters.RemovedCommittedTxs);
            }

            if (CommitManager) /* exists only on leader, mostly storage usage data */ {
                auto redo = LogicRedo->LogStats();
                Counters->Simple()[TExecutorCounters::LOG_REDO_COUNT].Set(redo.Items);
                Counters->Simple()[TExecutorCounters::LOG_REDO_MEMORY].Set(redo.Memory);
                Counters->Simple()[TExecutorCounters::LOG_REDO_SOLIDS].Set(redo.LargeGlobIds);
                Counters->Simple()[TExecutorCounters::LOG_SNAP_BYTES].Set(LogicSnap->LogBytes());
                Counters->Simple()[TExecutorCounters::LOG_ALTER_BYTES].Set(LogicAlter->LogBytes());
                Counters->Simple()[TExecutorCounters::LOG_RIVER_LEVEL].Set(Max(LogicSnap->Waste().Level, i64(0)));
                Counters->Simple()[TExecutorCounters::DB_DATA_BYTES].Set(CompactionLogic->GetBackingSize());
                Counters->Simple()[TExecutorCounters::DB_WARM_OPS].Set(dbCounters.MemTableOps);
                Counters->Simple()[TExecutorCounters::DB_ROWS_TOTAL].Set(dbCounters.Parts.RowsTotal);
                Counters->Simple()[TExecutorCounters::DB_ROWS_ERASE].Set(dbCounters.Parts.RowsErase);
                Counters->Simple()[TExecutorCounters::DB_PARTS_COUNT].Set(dbCounters.Parts.PartsCount);
                Counters->Simple()[TExecutorCounters::DB_PLAIN_BYTES].Set(dbCounters.Parts.PlainBytes);
                Counters->Simple()[TExecutorCounters::DB_CODED_BYTES].Set(dbCounters.Parts.CodedBytes);
                Counters->Simple()[TExecutorCounters::DB_ELOBS_BYTES].Set(dbCounters.Parts.LargeBytes);
                Counters->Simple()[TExecutorCounters::DB_ELOBS_ITEMS].Set(dbCounters.Parts.LargeItems);
                Counters->Simple()[TExecutorCounters::DB_OUTER_BYTES].Set(dbCounters.Parts.SmallBytes);
                Counters->Simple()[TExecutorCounters::DB_OUTER_ITEMS].Set(dbCounters.Parts.SmallItems);
                Counters->Simple()[TExecutorCounters::DB_UNIQUE_DATA_BYTES].Set(CompactionLogic->GetBackingSize(TabletId()));
                if (const auto* privateStats = dbCounters.PartsPerTablet.FindPtr(TabletId())) {
                    Counters->Simple()[TExecutorCounters::DB_UNIQUE_PARTS_COUNT].Set(privateStats->PartsCount);
                    Counters->Simple()[TExecutorCounters::DB_UNIQUE_ROWS_TOTAL].Set(privateStats->RowsTotal);
                    Counters->Simple()[TExecutorCounters::DB_UNIQUE_ROWS_ERASE].Set(privateStats->RowsErase);
                    Counters->Simple()[TExecutorCounters::DB_UNIQUE_PLAIN_BYTES].Set(privateStats->PlainBytes);
                    Counters->Simple()[TExecutorCounters::DB_UNIQUE_CODED_BYTES].Set(privateStats->CodedBytes);
                    Counters->Simple()[TExecutorCounters::DB_UNIQUE_ELOBS_BYTES].Set(privateStats->LargeBytes);
                    Counters->Simple()[TExecutorCounters::DB_UNIQUE_ELOBS_ITEMS].Set(privateStats->LargeItems);
                    Counters->Simple()[TExecutorCounters::DB_UNIQUE_OUTER_BYTES].Set(privateStats->SmallBytes);
                    Counters->Simple()[TExecutorCounters::DB_UNIQUE_OUTER_ITEMS].Set(privateStats->SmallItems);
                } else {
                    Counters->Simple()[TExecutorCounters::DB_UNIQUE_PARTS_COUNT].Set(0);
                    Counters->Simple()[TExecutorCounters::DB_UNIQUE_ROWS_TOTAL].Set(0);
                    Counters->Simple()[TExecutorCounters::DB_UNIQUE_ROWS_ERASE].Set(0);
                    Counters->Simple()[TExecutorCounters::DB_UNIQUE_PLAIN_BYTES].Set(0);
                    Counters->Simple()[TExecutorCounters::DB_UNIQUE_CODED_BYTES].Set(0);
                    Counters->Simple()[TExecutorCounters::DB_UNIQUE_ELOBS_BYTES].Set(0);
                    Counters->Simple()[TExecutorCounters::DB_UNIQUE_ELOBS_ITEMS].Set(0);
                    Counters->Simple()[TExecutorCounters::DB_UNIQUE_OUTER_BYTES].Set(0);
                    Counters->Simple()[TExecutorCounters::DB_UNIQUE_OUTER_ITEMS].Set(0);
                }
                Counters->Simple()[TExecutorCounters::DB_UNIQUE_KEEP_BYTES].Set(BorrowLogic->GetKeepBytes());
            }

            if (GcLogic) {
                auto gcInfo = GcLogic->IntrospectStateSize();
                Counters->Simple()[TExecutorCounters::GC_BLOBS_UNCOMMITTED].Set(gcInfo.UncommitedBlobIds);
                Counters->Simple()[TExecutorCounters::GC_BLOBS_CREATED].Set(gcInfo.CommitedBlobIdsKnown);
                Counters->Simple()[TExecutorCounters::GC_BLOBS_DELETED].Set(gcInfo.CommitedBlobIdsLeft);
                Counters->Simple()[TExecutorCounters::GC_BARRIERS_ACTIVE].Set(gcInfo.BarriersSetSize);
            }

            if (PrivatePageCache) {
                const auto &stats = PrivatePageCache->GetStats();
                Counters->Simple()[TExecutorCounters::CACHE_TOTAL_COLLECTIONS].Set(stats.TotalCollections);
                Counters->Simple()[TExecutorCounters::CACHE_TOTAL_SHARED_BODY].Set(stats.TotalSharedBody);
                Counters->Simple()[TExecutorCounters::CACHE_TOTAL_PINNED_BODY].Set(stats.TotalPinnedBody);
                Counters->Simple()[TExecutorCounters::CACHE_TOTAL_EXCLUSIVE].Set(stats.TotalExclusive);
            }
            Counters->Simple()[TExecutorCounters::CACHE_TOTAL_STICKY].Set(StickyPagesMemory);
            Counters->Simple()[TExecutorCounters::CACHE_TOTAL_USED].Set(TransactionPagesMemory);

            const auto &memory = Memory->Stats();

            Counters->Simple()[TExecutorCounters::USED_TABLET_TX_MEMORY].Set(memory.Static);
            Counters->Simple()[TExecutorCounters::USED_DYNAMIC_TX_MEMORY].Set(memory.Dynamic);

            executorCounters = Counters->MakeDiffForAggr(*CountersBaseline);
            Counters->RememberCurrentStateAsBaseline(*CountersBaseline);

            if (ResourceMetrics && !Stats->IsFollower()) {
                // N.B. DB_UNIQUE_OUTER_BYTES is already part of DB_UNIQUE_DATA_BYTES, due to how BackingSize works
                // We also include DB_UNIQUE_KEEP_BYTES as unreferenced data that cannot be deleted
                ui64 storageSize = Counters->Simple()[TExecutorCounters::DB_UNIQUE_DATA_BYTES].Get()
                        + Counters->Simple()[TExecutorCounters::DB_UNIQUE_ELOBS_BYTES].Get()
                        + Counters->Simple()[TExecutorCounters::DB_UNIQUE_KEEP_BYTES].Get();

                ResourceMetrics->StorageSystem.Set(storageSize);

                auto limit = Memory->Profile->GetStaticTabletTxMemoryLimit();
                auto memorySize = limit ? (UsedTabletMemory + limit) : (UsedTabletMemory + memory.Static);
                ResourceMetrics->Memory.Set(memorySize);
                Counters->Simple()[TExecutorCounters::CONSUMED_STORAGE].Set(storageSize);
                Counters->Simple()[TExecutorCounters::CONSUMED_MEMORY].Set(memorySize);
            }
        }

        if (AppCounters) {
            externalTabletCounters = AppCounters->MakeDiffForAggr(*AppCountersBaseline);
            AppCounters->RememberCurrentStateAsBaseline(*AppCountersBaseline);
        }

        // tablet id + tablet type
        ui64 tabletId = Owner->TabletID();
        auto tabletType = Owner->TabletType();
        auto tenantPathId = Owner->Info()->TenantPathId;

        TActorId countersAggregator = MakeTabletCountersAggregatorID(SelfId().NodeId(), Stats->IsFollower());
        Send(countersAggregator, new TEvTabletCounters::TEvTabletAddCounters(
            CounterEventsInFlight, tabletId, tabletType, tenantPathId, executorCounters, externalTabletCounters));

        if (ResourceMetrics) {
            ResourceMetrics->TryUpdate(ctx);
        }
    }
    Schedule(TDuration::Seconds(15), new TEvPrivate::TEvUpdateCounters());
}

float TExecutor::GetRejectProbability() const {
    // Limit number of in-flight TXs
    // TODO: make configurable
    if (Stats->TxInFly > MaxTxInFly) {
        HadRejectProbabilityByTxInFly = true;
        return 1.0;
    }

    // Followers do not control compaction so let's always allow to read the data from follower
    if (Stats->IsFollower())
        return 0.0;

    auto sigmoid = [](float x) -> float {
        auto ex = exp(x);
        return ex / (ex + 1.0); // N.B. better precision than 1 / (1 + exp(-x))
    };

    // Maps overload [0,1] to reject probability [0,1]
    auto calcProbability = [&sigmoid](float x) -> float {
        if (x < 0.0f) return 0.0f;
        if (x > 1.0f) return 1.0f;
        // map [0,1] to [-6,6] and apply logistic function
        auto value = sigmoid(x * 12.0f - 6.0f);
        // logistic function gives 0 < value < 1, rescale to [0,1]
        auto scale = sigmoid(6.0f);
        return 0.5f + 0.5f * (value - 0.5f) / (scale - 0.5f);
    };

    const float overloadFactor = CompactionLogic->GetOverloadFactor();
    const float rejectProbability = calcProbability(overloadFactor);

    if (rejectProbability > 0.0f) {
        HadRejectProbabilityByOverload = true;
    }

    return rejectProbability;
}

void TExecutor::MaybeRelaxRejectProbability() {
    if (HadRejectProbabilityByTxInFly && Stats->TxInFly <= MaxTxInFly ||
        HadRejectProbabilityByOverload)
    {
        HadRejectProbabilityByTxInFly = false;
        HadRejectProbabilityByOverload = false;
        GetRejectProbability();
        if (!HadRejectProbabilityByTxInFly &&
            !HadRejectProbabilityByOverload)
        {
            Owner->OnRejectProbabilityRelaxed();
        }
    }
}


TString TExecutor::BorrowSnapshot(ui32 table, const TTableSnapshotContext &snap, TRawVals from, TRawVals to, ui64 loaner) const
{
    auto subset = Database->Subset(table, snap.Edge(table).Head, from, to);

    if (subset == nullptr) {
        return { }; /* Lack of required pages in cache, retry later */
    }

    Y_ENSURE(!subset->Frozen, "Don't know how to borrow frozen parts");

    NKikimrExecutorFlat::TDatabaseBorrowPart proto;

    proto.SetSourceTable(table);
    proto.SetLenderTablet(TabletId());
    proto.MutableParts()->Reserve(subset->Flatten.size());

    for (const auto &partView : subset->Flatten) {
        auto *x = proto.AddParts();

        TPageCollectionProtoHelper(false).Do(x->MutableBundle(), partView);
        snap.Impl->Borrowed(Step(), table, partView->Label, loaner);
    }

    for (const auto &part : subset->ColdParts) {
        auto *x = proto.AddParts();

        TPageCollectionProtoHelper(false).Do(x->MutableBundle(), part);
        snap.Impl->Borrowed(Step(), table, part->Label, loaner);
    }

    for (const auto &part : subset->TxStatus) {
        const auto *txStatus = dynamic_cast<const NTable::TTxStatusPartStore*>(part.Get());
        Y_ENSURE(txStatus);
        auto *x = proto.AddTxStatusParts();
        TLargeGlobIdProto::Put(*x->MutableDataId(), txStatus->GetDataId());
        x->SetEpoch(txStatus->Epoch.ToProto());
        snap.Impl->Borrowed(Step(), table, txStatus->Label, loaner);
    }

    return proto.SerializeAsString();
}

ui64 TExecutor::MakeScanSnapshot(ui32 table)
{
    if (auto snapshot = PrepareScanSnapshot(table, nullptr)) {
        ScanSnapshots.emplace(++ScanSnapshotId, std::move(snapshot));

        return ScanSnapshotId;
    } else {
        return 0;
    }
}

void TExecutor::DropScanSnapshot(ui64 snap)
{
    auto it = ScanSnapshots.find(snap);
    if (it != ScanSnapshots.end()) {
        ReleaseScanLocks(std::move(it->second->Barrier), *it->second->Subset);
        ScanSnapshots.erase(it);
    }
}

ui64 TExecutor::QueueScan(ui32 tableId, TAutoPtr<NTable::IScan> scan, ui64 cookie, const TScanOptions& options)
{
    THolder<TScanSnapshot> snapshot;

    if (const auto* byId = std::get_if<TScanOptions::TSnapshotById>(&options.Snapshot)) {
        auto snapshotId = byId->SnapshotId;
        auto it = ScanSnapshots.find(snapshotId);
        Y_ENSURE(it != ScanSnapshots.end(),
            NFmt::Do(*this)
                << " QueueScan on table " << tableId
                << " with unknown snapshot " << snapshotId);
        snapshot = std::move(it->second);
        ScanSnapshots.erase(it);
    } else {
        TRowVersion rowVersion;
        if (const auto* byVersion = std::get_if<TScanOptions::TSnapshotByRowVersion>(&options.Snapshot)) {
            rowVersion = byVersion->RowVersion;
        } else {
            rowVersion = TRowVersion::Max();
        }
        snapshot = PrepareScanSnapshot(tableId, nullptr, rowVersion);
    }

    ui64 serial = Scans->Queue(tableId, scan, cookie, options, std::move(snapshot));

    if (options.IsResourceBrokerDisabled()) {
        StartScan(serial, tableId);
    }

    return serial;
}

bool TExecutor::CancelScan(ui32, ui64 serial) {
    if (auto cancelled = Scans->Cancel(serial)) {
        if (cancelled.Snapshot) {
            ReleaseScanLocks(std::move(cancelled.Snapshot->Barrier), *cancelled.Snapshot->Subset);
        }
        return true;
    }

    return false;
}

TFinishedCompactionInfo TExecutor::GetFinishedCompactionInfo(ui32 tableId) const {
    if (CompactionLogic) {
        return CompactionLogic->GetFinishedCompactionInfo(tableId);
    } else {
        return TFinishedCompactionInfo();
    }
}

ui64 TExecutor::CompactBorrowed(ui32 tableId) {
    if (CompactionLogic) {
        return CompactionLogic->PrepareForceCompaction(tableId, EForceCompaction::Borrowed);
    } else {
        return 0;
    }
}

ui64 TExecutor::CompactMemTable(ui32 tableId) {
    if (CompactionLogic) {
        return CompactionLogic->PrepareForceCompaction(tableId, EForceCompaction::Mem);
    } else {
        return 0;
    }
}

ui64 TExecutor::CompactTable(ui32 tableId) {
    if (CompactionLogic) {
        return CompactionLogic->PrepareForceCompaction(tableId);
    } else {
        return 0;
    }
}

bool TExecutor::CompactTables() {
    if (CompactionLogic) {
        return CompactionLogic->PrepareForceCompaction();
    } else {
        return false;
    }
}

void TExecutor::StartVacuum(ui64 vacuumGeneration) {
    if (VacuumLogic->TryStartVacuum(vacuumGeneration, OwnerCtx())) {
        for (const auto& [tableId, _] : Scheme().Tables) {
            auto compactionId = CompactionLogic->PrepareForceCompaction(tableId);
            VacuumLogic->OnCompactionPrepared(tableId, compactionId);
        }
        VacuumLogic->WaitCompaction();
        if (VacuumLogic->NeedLogSnaphot()) {
            MakeLogSnapshot();
        }
    }
}

void TExecutor::Handle(NMemory::TEvMemTableRegistered::TPtr &ev) {
    const auto *msg = ev->Get();

    if (CompactionLogic) {
        CompactionLogic->ProvideMemTableMemoryConsumer(msg->Table, std::move(msg->Consumer));
    }
}

void TExecutor::Handle(NMemory::TEvMemTableCompact::TPtr &ev) {
    const auto *msg = ev->Get();

    if (CompactionLogic) {
        CompactionLogic->TriggerSharedPageCacheMemTableCompaction(msg->Table, msg->ExpectedSize);
    }
}

void TExecutor::AllowBorrowedGarbageCompaction(ui32 tableId) {
    if (CompactionLogic) {
        return CompactionLogic->AllowBorrowedGarbageCompaction(tableId);
    }
}

STFUNC(TExecutor::StateInit) {
    Y_UNUSED(ev);
    Y_TABLET_ERROR("must be no events before boot processing");
}

STFUNC(TExecutor::StateBoot) {
    Y_ENSURE(BootLogic);
    switch (ev->GetTypeRewrite()) {
        // N.B. must work during follower promotion to leader
        HFunc(TEvPrivate::TEvActivateExecution, Handle);
        HFunc(TEvPrivate::TEvActivateLowExecution, Handle);
        HFunc(TEvPrivate::TEvBrokenTransaction, Handle);
        HFunc(TEvents::TEvWakeup, Wakeup);
        hFunc(TEvResourceBroker::TEvResourceAllocated, Handle);
    default:
        return TranscriptBootOpResult(BootLogic->Receive(*ev), this->ActorContext());
    }
}

STFUNC(TExecutor::StateWork) {
    switch (ev->GetTypeRewrite()) {
        HFunc(TEvPrivate::TEvActivateExecution, Handle);
        HFunc(TEvPrivate::TEvActivateLowExecution, Handle);
        HFunc(TEvPrivate::TEvBrokenTransaction, Handle);
        HFunc(TEvPrivate::TEvActivateCompactionChanges, Handle);
        CFunc(TEvPrivate::EvUpdateCounters, UpdateCounters);
        cFunc(TEvPrivate::EvCheckYellow, UpdateYellow);
        cFunc(TEvPrivate::EvUpdateCompactions, UpdateCompactions);
        HFunc(TEvPrivate::TEvLeaseExtend, Handle);
        HFunc(TEvPrivate::TEvRetryGcRequest, Handle);
        HFunc(TEvents::TEvWakeup, Wakeup);
        hFunc(TEvents::TEvFlushLog, Handle);
        hFunc(NSharedCache::TEvResult, Handle);
        hFunc(NSharedCache::TEvUpdated, Handle);
        HFunc(TEvTablet::TEvDropLease, Handle);
        HFunc(TEvTablet::TEvCommitResult, Handle);
        hFunc(TEvTablet::TEvConfirmLeaderResult, Handle);
        hFunc(TEvTablet::TEvCheckBlobstorageStatusResult, Handle);
        hFunc(TEvBlobStorage::TEvCollectGarbageResult, Handle);
        HFunc(TEvBlobStorage::TEvGetResult, Handle);
        hFunc(TEvResourceBroker::TEvResourceAllocated, Handle);
        HFunc(NOps::TEvScanStat, Handle);
        hFunc(NOps::TEvResult, Handle);
        HFunc(NBlockIO::TEvStat, Handle);
        hFunc(NMemory::TEvMemTableRegistered, Handle);
        hFunc(NMemory::TEvMemTableCompact, Handle);
        hFunc(TEvTablet::TEvGcForStepAckResponse, Handle);
    default:
        break;
    }

    TranslateCacheTouchesToSharedCache();
}

STFUNC(TExecutor::StateFollower) {
    switch (ev->GetTypeRewrite()) {
        HFunc(TEvPrivate::TEvActivateExecution, Handle);
        HFunc(TEvPrivate::TEvActivateLowExecution, Handle);
        HFunc(TEvPrivate::TEvBrokenTransaction, Handle);
        CFunc(TEvPrivate::EvUpdateCounters, UpdateCounters);
        HFunc(TEvents::TEvWakeup, Wakeup);
        hFunc(NSharedCache::TEvResult, Handle);
        hFunc(NSharedCache::TEvUpdated, Handle);
        HFunc(TEvBlobStorage::TEvGetResult, Handle);
        hFunc(TEvResourceBroker::TEvResourceAllocated, Handle);
        HFunc(NOps::TEvScanStat, Handle);
        hFunc(NOps::TEvResult, Handle);
        HFunc(NBlockIO::TEvStat, Handle);
    default:
        break;
    }

    TranslateCacheTouchesToSharedCache();
}

STFUNC(TExecutor::StateFollowerBoot) {
    Y_ENSURE(BootLogic);
    switch (ev->GetTypeRewrite()) {
        // N.B. must handle activities started before resync
        HFunc(TEvPrivate::TEvActivateExecution, Handle);
        HFunc(TEvPrivate::TEvActivateLowExecution, Handle);
        HFunc(TEvPrivate::TEvBrokenTransaction, Handle);
        HFunc(TEvents::TEvWakeup, Wakeup);
        hFunc(TEvResourceBroker::TEvResourceAllocated, Handle);
    default:
        return TranscriptFollowerBootOpResult(BootLogic->Receive(*ev), this->ActorContext());
    }
}

THashMap<TLogoBlobID, TVector<ui64>> TExecutor::GetBorrowedParts() const {
    if (BorrowLogic) {
        return BorrowLogic->GetBorrowedParts();
    }

    return { };
}

bool TExecutor::HasLoanedParts() const {
    if (BorrowLogic)
        return BorrowLogic->HasLoanedParts();
    return false;
}

bool TExecutor::HasBorrowed(ui32 table, ui64 selfTabletId) const {
    Y_ENSURE(Database, "Checking borrowers of table# " << table << " for tablet# " << selfTabletId);
    return Database->HasBorrowed(table, selfTabletId);
}

const TExecutorStats& TExecutor::GetStats() const {
    return *Stats;
}

void TExecutor::RenderHtmlCounters(NMon::TEvRemoteHttpInfo::TPtr &ev) const {
    TStringStream str;

    if (Database) {
        HTML(str) {
            str << "<style>";
            str << "table.metrics { margin-bottom: 20px; }";
            str << "table.metrics td { text-align: right; padding-right: 10px; }";
            str << "table.metrics td:nth-child(3) { text-align: left; }";
            str << "</style>";
            if (Counters) {
                TAG(TH3) {str << "Executor counters";}
                Counters->OutputHtml(str);
            }

            if (AppCounters) {
                TAG(TH3) {str << "App counters";}
                AppCounters->OutputHtml(str);
            }

            if (ResourceMetrics) {
                str << NMetrics::AsHTML(*ResourceMetrics);
            }
        }
    } else {
        HTML(str) {str << "loading...";} // todo: populate from bootlogic
    }

    Send(ev->Sender, new NMon::TEvRemoteHttpInfoRes(str.Str()));
}

void TExecutor::RenderHtmlPage(NMon::TEvRemoteHttpInfo::TPtr &ev) const {
    auto cgi = ev->Get()->Cgi();
    TStringStream str;

    if (cgi.Has("force_compaction")) {
        bool ok;
        bool allTables = false;
        ui32 tableId = 0;
        if (cgi.Get("force_compaction") == "all") {
            ok = true;
            allTables = true;
        } else {
            ok = TryFromString<ui32>(cgi.Get("force_compaction"), tableId);
        }
        cgi.EraseAll("force_compaction");
        TString message;
        if (ok) {
            if (allTables) {
                ok = const_cast<TExecutor*>(this)->CompactTables();
            } else {
                ok = const_cast<TExecutor*>(this)->CompactTable(tableId);
            }
            if (ok) {
                message = "Table will be compacted in the near future";
            } else {
                message = "ERROR: cannot compact the specified table";
            }
        } else {
            message = "ERROR: cannot parse table id";
        }
        HTML(str) {
            DIV_CLASS("row") {
                DIV_CLASS("col-md-12") {str << message; }
            }
            DIV_CLASS("row") {
                DIV_CLASS("col-md-12") {str << "<a href=\"executorInternals?" << cgi.Print() << "\">Back</a>"; }
            }
        }
    } else if (auto *scheme = Database ? &Database->GetScheme() : nullptr) {
        HTML(str) {
            TAG(TH3) { str << NFmt::Do(*this) << " tablet synopsis"; }

            if (auto *logic = BootLogic.Get()) {
                 DIV_CLASS("row") {str << NFmt::Do(*logic); }
            } else if (auto *dbase = Database.Get()) {
                if (CommitManager) /* Leader tablet, commit manager owner */ {
                    DIV_CLASS("row") { str << NFmt::Do(*CommitManager); }
                    DIV_CLASS("row") { str << NFmt::Do(LogicSnap->Waste(), true);}
                    DIV_CLASS("row") { str << NFmt::Do(*LogicSnap); }
                    DIV_CLASS("row") { str << NFmt::Do(*LogicRedo); }
                    DIV_CLASS("row") { str << NFmt::Do(*LogicAlter); }
                } else {
                    DIV_CLASS("row") { str << "Sync{on step " << Step0 << "}"; }
                }

                DIV_CLASS("row") { str << NFmt::Do(dbase->Counters()); }
                DIV_CLASS("row") { str << NFmt::Do(*Scans); }
                DIV_CLASS("row") { str << NFmt::Do(Memory->Stats()); }
            } else {
                DIV_CLASS("row") { str << "Booted tablet without dbase"; }
            }

            TAG(TH3) {str << "Scheme:";}
            TVector<ui32> tables;
            for (const auto &xtable : scheme->Tables)
                tables.push_back(xtable.first);
            Sort(tables);
            for (auto itable : tables) {
                const auto &tinfo = scheme->Tables.find(itable)->second;
                TAG(TH4) {str << "<a href='db?TabletID=" << Owner->TabletID() << "&TableID=" << tinfo.Id << "'>Table: \"" << tinfo.Name << "\" id: " << tinfo.Id << "</a>";}
                TABLE_SORTABLE_CLASS("table") {
                    TABLEHEAD() {
                        TABLER() {
                            TABLEH() {str << "Name";}
                            TABLEH() {str << "Id";}
                            TABLEH() {str << "Type";}
                            TABLEH() {str << "Key order";}
                         }
                    }
                    TABLEBODY() {
                        TVector<ui32> columns;
                        for (const auto &xcol : tinfo.Columns)
                            columns.push_back(xcol.first);
                        Sort(columns);
                        for (auto icol : columns) {
                            const auto &col = tinfo.Columns.find(icol)->second;
                            const bool isKey = (tinfo.KeyColumns.end() != std::find(tinfo.KeyColumns.begin(), tinfo.KeyColumns.end(), col.Id));
                            TABLER() {
                                TABLED() {str << col.Name;}
                                TABLED() {str << col.Id;}
                                TABLED() {str << NScheme::TypeName(col.PType, col.PTypeMod);}
                                TABLED() {str << (isKey ? ToString(col.KeyOrder) : "");}
                            }
                        }
                    }
                }
            }

            TAG(TH3) {str << "Storage:";}
            DIV_CLASS("row") {str << "Bytes pinned in cache: " << PrivatePageCache->GetStats().PinnedSetSize << Endl; }
            DIV_CLASS("row") {str << "Bytes pinned to load: " << PrivatePageCache->GetStats().PinnedLoadSize << Endl; }

            TAG(TH3) {str << "Resource usage:";}
            DIV_CLASS("row") {str << "used tablet memory: " << UsedTabletMemory; }
            Memory->DumpStateToHTML(str);

            if (CompactionLogic)
                CompactionLogic->OutputHtml(str, *scheme, cgi);

            TAG(TH3) {str << "Page collection cache:";}
            DIV_CLASS("row") {str << "Total collections: " << PrivatePageCache->GetStats().TotalCollections; }
            DIV_CLASS("row") {str << "Total bytes in shared cache: " << PrivatePageCache->GetStats().TotalSharedBody; }
            DIV_CLASS("row") {str << "Total bytes in local cache: " << PrivatePageCache->GetStats().TotalPinnedBody; }
            DIV_CLASS("row") {str << "Total bytes exclusive to local cache: " << PrivatePageCache->GetStats().TotalExclusive; }
            DIV_CLASS("row") {str << "Total bytes marked as sticky: " << StickyPagesMemory; }
            DIV_CLASS("row") {str << "Total bytes currently in use: " << TransactionPagesMemory; }

            if (GcLogic) {
                TAG(TH3) {str << "Gc logic:";}
                auto gcInfo = GcLogic->IntrospectStateSize();
                DIV_CLASS("row") {str << "uncommited entries: " << gcInfo.UncommitedEntries;}
                DIV_CLASS("row") {str << "uncommited blob ids: " << gcInfo.UncommitedBlobIds; }
                DIV_CLASS("row") {str << "uncommited entries bytes: " << gcInfo.UncommitedEntriesBytes;}
                DIV_CLASS("row") {str << "commited entries: " << gcInfo.CommitedEntries;}
                DIV_CLASS("row") {str << "commited blob ids known: " << gcInfo.CommitedBlobIdsKnown;}
                DIV_CLASS("row") {str << "commited blob ids left: " << gcInfo.CommitedBlobIdsLeft;}
                DIV_CLASS("row") {str << "commited entries bytes: " << gcInfo.CommitedEntriesBytes; }
                DIV_CLASS("row") {str << "active collect barriers: " << gcInfo.BarriersSetSize; }
            }

            if (BorrowLogic) {
                TAG(TH3) {str << "Borrow logic:";}
                BorrowLogic->OutputHtml(str);
            }
        }

    } else {
        HTML(str) {str << "loading...";} // todo: populate from bootlogic
    }

    Send(ev->Sender, new NMon::TEvRemoteHttpInfoRes(str.Str()));
}

const NTable::TScheme& TExecutor::Scheme() const {
    Y_DEBUG_ABORT_UNLESS(Database);
    return Database->GetScheme();
}

void TExecutor::RegisterExternalTabletCounters(TAutoPtr<TTabletCountersBase> appCounters) {
    AppCounters = appCounters;
    AppCountersBaseline = MakeHolder<TTabletCountersBase>();
    AppCounters->RememberCurrentStateAsBaseline(*AppCountersBaseline);

    AppTxCounters = dynamic_cast<TTabletCountersWithTxTypes*>(AppCounters.Get());

    if (LogicRedo) {
        LogicRedo->InstallCounters(Counters.Get(), AppTxCounters);
    }
}

void TExecutor::GetTabletCounters(TEvTablet::TEvGetCounters::TPtr &ev) {
    TAutoPtr<TEvTablet::TEvGetCountersResponse> response = new TEvTablet::TEvGetCountersResponse();
    Counters->OutputProto(*response->Record.MutableTabletCounters()->MutableExecutorCounters());
    AppCounters->OutputProto(*response->Record.MutableTabletCounters()->MutableAppCounters());
    Send(ev->Sender, response.Release(), 0, ev->Cookie);
}

void TExecutor::UpdateConfig(TEvTablet::TEvUpdateConfig::TPtr &ev) {
    Memory->SetProfiles(ev->Get()->ResourceProfiles);
    ReadResourceProfile();
}

void TExecutor::SendUserAuxUpdateToFollowers(TString upd, const TActorContext &ctx) {
    Y_ENSURE(Stats->IsActive && !Stats->IsFollower());

    NKikimrExecutorFlat::TFollowerAux proto;
    proto.SetUserAuxUpdate(std::move(upd));

    auto coded = NPageCollection::TSlicer::Lz4()->Encode(proto.SerializeAsString());

    ctx.Send(Owner->Tablet(), new TEvTablet::TEvAux(std::move(coded)));
}

NMetrics::TResourceMetrics* TExecutor::GetResourceMetrics() const {
    return ResourceMetrics.Get();
}

TExecutorCounters* TExecutor::GetCounters() {
    return Counters.Get();
}

void TExecutor::ReadResourceProfile() {
    if (Database) {
        auto type = static_cast<TMemory::ETablet>(Owner->TabletType());
        Memory->UseProfile(type, Scheme().Executor.ResourceProfile);
    }
}

TString TExecutor::CheckBorrowConsistency() {
    THashSet<TLogoBlobID> knownBundles;
    for (auto& kv : Scheme().Tables) {
        const ui32 tableId = kv.first;
        Database->EnumerateTableParts(tableId,
            [&](const NTable::TPartView& partView) {
                knownBundles.insert(partView->Label);
            });
        Database->EnumerateTableColdParts(tableId,
            [&](const TIntrusiveConstPtr<NTable::TColdPart>& part) {
                knownBundles.insert(part->Label);
            });
        Database->EnumerateTableTxStatusParts(tableId,
            [&](const TIntrusiveConstPtr<NTable::TTxStatusPart>& part) {
                knownBundles.insert(part->Label);
            });
    }
    return BorrowLogic->DebugCheckBorrowConsistency(knownBundles);
}

TTransactionWaitPad::TTransactionWaitPad(TSeat* seat)
    : Seat(seat)
    , WaitingSpan(NWilson::TSpan(TWilsonTablet::TabletDetailed, Seat->GetTxTraceId(), "Tablet.Transaction.Wait"))
{}

TTransactionWaitPad::~TTransactionWaitPad()
{}

NWilson::TTraceId TTransactionWaitPad::GetWaitingTraceId() const {
    return WaitingSpan.GetTraceId();
}

// ICompactionBackend implementation

ui64 TExecutor::OwnerTabletId() const
{
    return Owner->TabletID();
}

const NTable::TScheme& TExecutor::DatabaseScheme()
{
    return Scheme();
}

TIntrusiveConstPtr<NTable::TRowScheme> TExecutor::RowScheme(ui32 table) const
{
    return Database->GetRowScheme(table);
}

const NTable::TScheme::TTableInfo* TExecutor::TableScheme(ui32 table)
{
    auto* info = Scheme().GetTableInfo(table);
    Y_ENSURE(info, "Unexpected request for schema of table " << table);
    return info;
}

ui64 TExecutor::TableMemSize(ui32 table, NTable::TEpoch epoch)
{
    return Database->GetTableMemSize(table, epoch);
}

NTable::TPartView TExecutor::TablePart(ui32 table, const TLogoBlobID& label)
{
    auto partView = Database->GetPartView(table, label);
    if (!partView) {
        Y_TABLET_ERROR("Unexpected request for missing part " << label << " in table " << table);
    }
    return partView;
}

TVector<NTable::TPartView> TExecutor::TableParts(ui32 table)
{
    return Database->GetTableParts(table);
}

TVector<TIntrusiveConstPtr<NTable::TColdPart>> TExecutor::TableColdParts(ui32 table)
{
    return Database->GetTableColdParts(table);
}

const NTable::TRowVersionRanges& TExecutor::TableRemovedRowVersions(ui32 table)
{
    return Database->GetRemovedRowVersions(table);
}

bool TExecutor::HasSchemaChanges(ui32 table) const {
    auto *tableInfo = Scheme().GetTableInfo(table);
    auto rowScheme = RowScheme(table);
    if (!tableInfo || !rowScheme) {
        return false;
    }

    auto subset = Database->Subset(table, NTable::TEpoch::Max(), { } , { });
    for (const auto& partView : subset->Flatten) {
        if (HasSchemaChanges(partView, *tableInfo, *rowScheme)) {
            return true;
        }
    }

    return false;
}

bool TExecutor::HasSchemaChanges(const NTable::TPartView& partView, const NTable::TScheme::TTableInfo& tableInfo, const NTable::TRowScheme& rowScheme) const {
    if (partView.Part->Stat.Rows == 0) {
        return false;
    }

    { // Check by key filter existence
        bool partByKeyFilter = bool(partView->ByKey);
        bool schemeByKeyFilter = tableInfo.ByKeyFilter;
        if (partByKeyFilter != schemeByKeyFilter) {
            return true;
        }
    }

    { // Check B-Tree index existence
        if (AppData()->FeatureFlags.GetEnableLocalDBBtreeIndex() && !partView->IndexPages.HasBTree()) {
            return true;
        }
    }

    { // Check families
        size_t partFamiliesCount = partView->GroupsCount;
        size_t schemeFamiliesCount = rowScheme.Families.size();
        if (partFamiliesCount != schemeFamiliesCount) {
            return true;
        }

        for (size_t index : xrange(rowScheme.Families.size())) {
            auto familyId = rowScheme.Families[index];
            static const NTable::TScheme::TFamily defaultFamilySettings;
            const auto& family = tableInfo.Families.ValueRef(familyId, defaultFamilySettings); // Workaround for KIKIMR-17222

            const auto* schemeGroupRoom = tableInfo.Rooms.FindPtr(family.Room);
            Y_ENSURE(schemeGroupRoom, "Cannot find room " << family.Room << " in table " << tableInfo.Id);

            ui32 partGroupChannel = partView.Part->GetGroupChannel(NTable::NPage::TGroupId(index));
            if (partGroupChannel != schemeGroupRoom->Main) {
                return true;
            }
        }
    }

    { // Check columns
        THashMap<NTable::TTag, ui32> partColumnGroups, schemeColumnGroups;
        for (const auto& column : partView->Scheme->AllColumns) {
            partColumnGroups[column.Tag] = column.Group;
        }
        for (const auto& col : rowScheme.Cols) {
            schemeColumnGroups[col.Tag] = col.Group;
        }
        if (partColumnGroups != schemeColumnGroups) {
            return true;
        }
    }

    return false;
}

ui64 TExecutor::BeginCompaction(THolder<NTable::TCompactionParams> params)
{
    if (auto logl = Logger->Log(ELnLev::Info))
        logl << NFmt::Do(*this) << " starting compaction";

    using NTable::NPage::ECache;

    auto table = params->Table;
    auto snapshot = PrepareScanSnapshot(table, params.Get());

    auto rowScheme = RowScheme(table);
    auto *tableInfo = Scheme().GetTableInfo(table);
    auto *policy = tableInfo->CompactionPolicy.Get();

    const ECache cache = params->KeepInCache ? ECache::Once : ECache::None;

    TAutoPtr<TCompactCfg> comp = new TCompactCfg(std::move(params));

    comp->Epoch = snapshot->Subset->Epoch(); /* narrows requested to actual */
    comp->Layout.Final = comp->Params->IsFinal;
    comp->Layout.WriteBTreeIndex = AppData()->FeatureFlags.GetEnableLocalDBBtreeIndex();
    comp->Layout.WriteFlatIndex = AppData()->FeatureFlags.GetEnableLocalDBFlatIndex();
    comp->Writer.StickyFlatIndex = !comp->Layout.WriteBTreeIndex;
    comp->Layout.MaxRows = snapshot->Subset->MaxRows();
    comp->Layout.ByKeyFilter = tableInfo->ByKeyFilter;
    comp->Layout.UnderlayMask = comp->Params->UnderlayMask.Get();
    comp->Layout.SplitKeys = comp->Params->SplitKeys.Get();
    comp->Layout.MinRowVersion = snapshot->Subset->MinRowVersion();
    comp->Layout.Groups.resize(rowScheme->Families.size());
    comp->Writer.Groups.resize(rowScheme->Families.size());

    auto addChannel = [&](ui8 channel) {
        auto group = Owner->Info()->GroupFor(channel, Generation());

        comp->Writer.Slots.emplace_back(channel, group);
    };

    auto addChannels = [&](const std::vector<ui8>& channels) {
        for (auto channel : channels) {
            addChannel(channel);
        }
    };

    for (size_t group : xrange(rowScheme->Families.size())) {
        auto familyId = rowScheme->Families[group];
        static const NTable::TScheme::TFamily defaultFamilySettings;
        const auto& family = tableInfo->Families.ValueRef(familyId, defaultFamilySettings); // Workaround for KIKIMR-17222

        auto* room = tableInfo->Rooms.FindPtr(family.Room);
        Y_ENSURE(room, "Cannot find room " << family.Room << " in table " << table);

        auto& pageGroup = comp->Layout.Groups.at(group);
        auto& writeGroup = comp->Writer.Groups.at(group);

        pageGroup.Codec = family.Codec;
        pageGroup.PageSize = policy->MinDataPageSize;
        pageGroup.BTreeIndexNodeTargetSize = policy->MinBTreeIndexNodeSize;
        pageGroup.BTreeIndexNodeKeysMin = policy->MinBTreeIndexNodeKeys;

        writeGroup.Cache = Max(family.Cache, cache);
        writeGroup.MaxBlobSize = NBlockIO::BlockSize;
        writeGroup.Channel = room->Main;
        addChannel(room->Main);

        if (group == 0) {
            // Small/Large edges are taken from the leader family
            comp->Layout.SmallEdge = family.Small;
            comp->Layout.LargeEdge = family.Large;

            // Small/Large channels are taken from the leader family
            comp->Writer.BlobsChannels = room->Blobs;
            comp->Writer.OuterChannel = room->Outer;
            addChannels(room->Blobs);
            addChannel(room->Outer);

            comp->Writer.ChannelsShares = NUtil::TChannelsShares(Database->Counters().NormalizedFreeSpaceShareByChannel);
        }
    }

    if (const auto& ranges = Database->GetRemovedRowVersions(table)) {
        // Make a copy of removed versions for compacted table
        // Version removal cannot be undone, so it's still valid at commit time
        comp->RemovedRowVersions = ranges.Snapshot();

        // We have to adjust MinRowVersion, so it correctly expects an adjusted version
        comp->Layout.MinRowVersion = comp->RemovedRowVersions.AdjustDown(comp->Layout.MinRowVersion);
    }

    bool compactTxStatus = false;
    for (const auto& memTableSnapshot : snapshot->Subset->Frozen) {
        if (!memTableSnapshot->GetCommittedTransactions().empty() || !memTableSnapshot->GetRemovedTransactions().empty()) {
            // We must compact tx status when mem table has changes
            compactTxStatus = true;
            break;
        }
    }
    for (const auto& txStatus : snapshot->Subset->TxStatus) {
        if (txStatus->Label.TabletID() != Owner->TabletID()) {
            // We want to compact borrowed tx status
            compactTxStatus = true;
            break;
        }
    }
    if (snapshot->Subset->TxStatus && snapshot->Subset->GarbageTransactions) {
        // We want to remove garbage transactions
        compactTxStatus = true;
    }

    if (compactTxStatus) {
        comp->Frozen.reserve(snapshot->Subset->Frozen.size());
        for (auto& memTableSnapshot : snapshot->Subset->Frozen) {
            comp->Frozen.push_back(memTableSnapshot.MemTable);
        }
        comp->TxStatus = snapshot->Subset->TxStatus;
        comp->GarbageTransactions = snapshot->Subset->GarbageTransactions;
    } else {
        // We are not compacting tx status, avoid deleting current blobs
        snapshot->Subset->TxStatus.clear();
    }

    TLogoBlobID mask(Owner->TabletID(), Generation(),
                    snapshot->Barrier->Step, Max<ui8>(), 0, 0);

    auto *scan = new TOpsCompact(SelfId(), mask, comp);

    NOps::TConf conf;

    conf.Trace = true; /* Need for tracking gone blobs in GC */
    conf.Tablet = Owner->TabletID();

    auto result = Scans->StartSystem(table, scan, conf, std::move(snapshot));
    if (auto logl = Logger->Log(ELnLev::Info))
        logl << NFmt::Do(*this) << " started compaction " << result;
    return result;
}

bool TExecutor::CancelCompaction(ui64 compactionId)
{
    if (auto logl = Logger->Log(ELnLev::Info))
        logl << NFmt::Do(*this) << " cancelling compaction " << compactionId;

    return Scans->CancelSystem(compactionId);
}

void TExecutor::RequestChanges(ui32 table)
{
    Y_ENSURE(CompactionLogic);

    CompactionLogic->RequestChanges(table);
    PlanCompactionChangesActivation();
}

void TExecutor::PlanCompactionChangesActivation()
{
    if (!CompactionChangesActivating) {
        CompactionChangesActivating = true;
        Send(SelfId(), new TEvPrivate::TEvActivateCompactionChanges());
    }
}

void TExecutor::Handle(TEvPrivate::TEvActivateCompactionChanges::TPtr& ev, const TActorContext& ctx)
{
    Y_UNUSED(ev);
    Y_UNUSED(ctx);

    CompactionChangesActivating = false;

    for (auto& logicResult : CompactionLogic->ApplyChanges()) {
        CommitCompactionChanges(logicResult.Table, logicResult.Changes, logicResult.Strategy);
    }

    if (LogicSnap->MayFlush(false)) {
        MakeLogSnapshot();
    }
}

void TExecutor::CommitCompactionChanges(
        ui32 tableId,
        const NTable::TCompactionChanges& changes,
        NKikimrCompaction::ECompactionStrategy strategy)
{
    if (!changes.SliceChanges && !changes.StateChanges) {
        // Don't bother unless there's something to do
        return;
    }

    LogicRedo->FlushBatchedLog();

    auto commit = CommitManager->Begin(true, ECommit::Misc, {});

    NKikimrExecutorFlat::TTablePartSwitch proto;
    proto.SetTableId(tableId);

    TCompactionChangesCtx ctx(proto);
    ApplyCompactionChanges(ctx, changes, strategy);

    { /*_ Finalize switch (turn) blob and attach it to commit */
        auto body = proto.SerializeAsString();
        auto glob = CommitManager->Turns.One(commit->Refs, std::move(body), true);

        Y_UNUSED(glob);
    }

    AttachLeaseCommit(commit.Get());
    CommitManager->Commit(commit);
}

void TExecutor::ApplyCompactionChanges(
        TCompactionChangesCtx& ctx,
        const NTable::TCompactionChanges& changes,
        NKikimrCompaction::ECompactionStrategy strategy)
{
    const ui32 tableId = ctx.Proto.GetTableId();

    if (changes.StateChanges) {
        auto *changesProto = ctx.Proto.MutableCompactionChanges();
        changesProto->SetTable(tableId);
        changesProto->SetStrategy(strategy);
        for (const auto& kv : changes.StateChanges) {
            auto *kvProto = changesProto->AddKeyValues();
            kvProto->SetKey(kv.first);
            if (kv.second) {
                kvProto->SetValue(kv.second);
            }
        }
    }

    // Apply any slice changes that compaction has requested
    if (changes.SliceChanges) {
        // Changes may be to compaction results
        THashMap<TLogoBlobID, TProdCompact::TResult*> results;
        if (ctx.Results) {
            for (auto &result : *ctx.Results) {
                results[result.Part->Label] = &result;
            }
        }

        TVector<TLogoBlobID> labels(Reserve(changes.SliceChanges.size()));
        for (auto &sliceChange : changes.SliceChanges) {
            labels.push_back(sliceChange.Label);
        }

        auto pendingChanges = Database->LookupSlices(tableId, labels);

        for (const auto &sliceChange : changes.SliceChanges) {
            auto* current = pendingChanges.FindPtr(sliceChange.Label);
            Y_ENSURE(current, "[" << TabletId() << "] cannot apply changes to table " << tableId
                << " part " << sliceChange.Label << ": not found");

            *current = NTable::TSlices::Replace(std::move(*current), sliceChange.NewSlices);

            if (auto *result = results.Value(sliceChange.Label, nullptr)) {
                result->Part.Slices = *current;
            } else {
                auto *deltaProto = ctx.Proto.AddBundleDeltas();
                LogoBlobIDFromLogoBlobID(sliceChange.Label, deltaProto->MutableLabel());
                deltaProto->SetDelta(NTable::TOverlay::EncodeChangeSlices(sliceChange.NewSlices));
            }
        }

        Database->ReplaceSlices(tableId, pendingChanges);
    }
}

void TExecutor::SetPreloadTablesData(THashSet<ui32> tables) {
    PreloadTablesData = std::move(tables);
}

}
}
