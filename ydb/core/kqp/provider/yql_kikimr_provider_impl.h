#pragma once

#include "yql_kikimr_provider.h"

#include <ydb/core/external_sources/external_source_factory.h>
#include <ydb/core/kqp/provider/yql_kikimr_expr_nodes.h>
#include <ydb/core/kqp/provider/yql_kikimr_results.h>

#include <yql/essentials/providers/common/provider/yql_provider.h>


namespace NYql {

class TKiSourceVisitorTransformer: public TSyncTransformerBase {
public:
    TStatus DoTransform(TExprNode::TPtr input, TExprNode::TPtr& output, TExprContext& ctx) override;

    void Rewind() override {
    }
private:
    virtual TStatus HandleKiRead(NNodes::TKiReadBase node, TExprContext& ctx) = 0;
    virtual TStatus HandleRead(NNodes::TExprBase node, TExprContext& ctx) = 0;
    virtual TStatus HandleLength(NNodes::TExprBase node, TExprContext& ctx) = 0;
    virtual TStatus HandleConfigure(NNodes::TExprBase node, TExprContext& ctx) = 0;
};

class TKiSinkVisitorTransformer : public TSyncTransformerBase {
public:
    TStatus DoTransform(TExprNode::TPtr input, TExprNode::TPtr& output, TExprContext& ctx) final;

    void Rewind() override {
    }
private:
    virtual TStatus HandleAlterDatabase(NNodes::TKiAlterDatabase node, TExprContext& ctx) = 0;

    virtual TStatus HandleWriteTable(NNodes::TKiWriteTable node, TExprContext& ctx) = 0;
    virtual TStatus HandleUpdateTable(NNodes::TKiUpdateTable node, TExprContext& ctx) = 0;
    virtual TStatus HandleDeleteTable(NNodes::TKiDeleteTable node, TExprContext& ctx) = 0;
    virtual TStatus HandleCreateTable(NNodes::TKiCreateTable node, TExprContext& ctx) = 0;
    virtual TStatus HandleAlterTable(NNodes::TKiAlterTable node, TExprContext& ctx) = 0;
    virtual TStatus HandleDropTable(NNodes::TKiDropTable node, TExprContext& ctx) = 0;

    virtual TStatus HandleCreateTopic(NNodes::TKiCreateTopic node, TExprContext& ctx) = 0;
    virtual TStatus HandleAlterTopic(NNodes::TKiAlterTopic node, TExprContext& ctx) = 0;
    virtual TStatus HandleDropTopic(NNodes::TKiDropTopic node, TExprContext& ctx) = 0;

    virtual TStatus HandleCreateReplication(NNodes::TKiCreateReplication node, TExprContext& ctx) = 0;
    virtual TStatus HandleAlterReplication(NNodes::TKiAlterReplication node, TExprContext& ctx) = 0;
    virtual TStatus HandleDropReplication(NNodes::TKiDropReplication node, TExprContext& ctx) = 0;

    virtual TStatus HandleCreateTransfer(NNodes::TKiCreateTransfer node, TExprContext& ctx) = 0;
    virtual TStatus HandleAlterTransfer(NNodes::TKiAlterTransfer node, TExprContext& ctx) = 0;
    virtual TStatus HandleDropTransfer(NNodes::TKiDropTransfer node, TExprContext& ctx) = 0;

    virtual TStatus HandleCreateUser(NNodes::TKiCreateUser node, TExprContext& ctx) = 0;
    virtual TStatus HandleAlterUser(NNodes::TKiAlterUser node, TExprContext& ctx) = 0;
    virtual TStatus HandleDropUser(NNodes::TKiDropUser node, TExprContext& ctx) = 0;

    virtual TStatus HandleUpsertObject(NNodes::TKiUpsertObject node, TExprContext& ctx) = 0;
    virtual TStatus HandleCreateObject(NNodes::TKiCreateObject node, TExprContext& ctx) = 0;
    virtual TStatus HandleAlterObject(NNodes::TKiAlterObject node, TExprContext& ctx) = 0;
    virtual TStatus HandleDropObject(NNodes::TKiDropObject node, TExprContext& ctx) = 0;
    virtual TStatus HandleCreateGroup(NNodes::TKiCreateGroup node, TExprContext& ctx) = 0;
    virtual TStatus HandleAlterGroup(NNodes::TKiAlterGroup node, TExprContext& ctx) = 0;
    virtual TStatus HandleRenameGroup(NNodes::TKiRenameGroup node, TExprContext& ctx) = 0;
    virtual TStatus HandleDropGroup(NNodes::TKiDropGroup node, TExprContext& ctx) = 0;
    virtual TStatus HandleWrite(NNodes::TExprBase node, TExprContext& ctx) = 0;
    virtual TStatus HandleCommit(NNodes::TCoCommit node, TExprContext& ctx) = 0;
    virtual TStatus HandleExecDataQuery(NNodes::TKiExecDataQuery node, TExprContext& ctx) = 0;
    virtual TStatus HandleDataQueryBlocks(NNodes::TKiDataQueryBlocks node, TExprContext& ctx) = 0;
    virtual TStatus HandleDataQueryBlock(NNodes::TKiDataQueryBlock node, TExprContext& ctx) = 0;
    virtual TStatus HandleEffects(NNodes::TKiEffects node, TExprContext& ctx) = 0;
    virtual TStatus HandlePgDropObject(NNodes::TPgDropObject node, TExprContext& ctx) = 0;

    virtual TStatus HandleCreateSequence(NNodes::TKiCreateSequence node, TExprContext& ctx) = 0;
    virtual TStatus HandleDropSequence(NNodes::TKiDropSequence node, TExprContext& ctx) = 0;
    virtual TStatus HandleAlterSequence(NNodes::TKiAlterSequence node, TExprContext& ctx) = 0;

    virtual TStatus HandleModifyPermissions(NNodes::TKiModifyPermissions node, TExprContext& ctx) = 0;

    virtual TStatus HandleReturningList(NNodes::TKiReturningList node, TExprContext& ctx) = 0;

    virtual TStatus HandleAnalyze(NNodes::TKiAnalyzeTable node, TExprContext& ctx) = 0;

    virtual TStatus HandleCreateBackupCollection(NNodes::TKiCreateBackupCollection node, TExprContext& ctx) = 0;
    virtual TStatus HandleAlterBackupCollection(NNodes::TKiAlterBackupCollection node, TExprContext& ctx) = 0;
    virtual TStatus HandleDropBackupCollection(NNodes::TKiDropBackupCollection node, TExprContext& ctx) = 0;
    virtual TStatus HandleBackup(NNodes::TKiBackup node, TExprContext& ctx) = 0;
    virtual TStatus HandleBackupIncremental(NNodes::TKiBackupIncremental node, TExprContext& ctx) = 0;
    virtual TStatus HandleRestore(NNodes::TKiRestore node, TExprContext& ctx) = 0;
};

class TKikimrKey {
public:
    enum class Type {
        Database,
        Table,
        TableList,
        TableScheme,
        Role,
        Object,
        Topic,
        Permission,
        PGObject,
        Replication,
        BackupCollection,
        Sequence,
        Transfer,
    };

    struct TViewDescription {
        TString Name;
        bool PrimaryFlag = false;
    };

    struct TBackupCollectionDescription {
        TString Prefix;
        TString Name;
    };

public:
    TKikimrKey(TExprContext& ctx)
        : Ctx(ctx) {}

    Type GetKeyType() const {
        Y_DEBUG_ABORT_UNLESS(KeyType.Defined());
        return *KeyType;
    }

    TString GetTablePath() const {
        Y_DEBUG_ABORT_UNLESS(KeyType.Defined());
        Y_DEBUG_ABORT_UNLESS(KeyType == Type::Table || KeyType == Type::TableScheme);
        return Target;
    }

    TString GetDatabasePath() const {
        Y_DEBUG_ABORT_UNLESS(KeyType.Defined());
        Y_DEBUG_ABORT_UNLESS(KeyType == Type::Database);
        return Target;
    }

    TString GetTopicPath() const {
        Y_DEBUG_ABORT_UNLESS(KeyType.Defined());
        Y_DEBUG_ABORT_UNLESS(KeyType == Type::Topic);
        return Target;
    }

    TString GetSequencePath() const {
        Y_DEBUG_ABORT_UNLESS(KeyType.Defined());
        Y_DEBUG_ABORT_UNLESS(KeyType == Type::Sequence);
        return Target;
    }

    TString GetReplicationPath() const {
        Y_DEBUG_ABORT_UNLESS(KeyType.Defined());
        Y_DEBUG_ABORT_UNLESS(KeyType == Type::Replication);
        return Target;
    }

    TString GetTransferPath() const {
        Y_DEBUG_ABORT_UNLESS(KeyType.Defined());
        Y_DEBUG_ABORT_UNLESS(KeyType == Type::Transfer);
        return Target;
    }

    TString GetFolderPath() const {
        Y_DEBUG_ABORT_UNLESS(KeyType.Defined());
        Y_DEBUG_ABORT_UNLESS(KeyType == Type::TableList);
        return Target;
    }

    TString GetRoleName() const {
        Y_DEBUG_ABORT_UNLESS(KeyType.Defined());
        Y_DEBUG_ABORT_UNLESS(KeyType == Type::Role);
        return Target;
    }

    TString GetObjectId() const {
        Y_DEBUG_ABORT_UNLESS(KeyType.Defined());
        Y_DEBUG_ABORT_UNLESS(KeyType == Type::Object);
        return Target;
    }

    const TMaybe<TViewDescription>& GetView() const {
        return View;
    }

    const TString& GetObjectType() const {
        Y_DEBUG_ABORT_UNLESS(KeyType.Defined());
        Y_DEBUG_ABORT_UNLESS(ObjectType.Defined());
        Y_DEBUG_ABORT_UNLESS(KeyType == Type::Object);
        return *ObjectType;
    }

    const TString& GetPermissionAction() const {
        Y_DEBUG_ABORT_UNLESS(KeyType.Defined());
        Y_DEBUG_ABORT_UNLESS(KeyType == Type::Permission);
        return Target;
    }

    const TString& GetPGObjectId() const {
        Y_DEBUG_ABORT_UNLESS(KeyType.Defined());
        Y_DEBUG_ABORT_UNLESS(KeyType == Type::PGObject);
        return Target;
    }

    const TString& GetPGObjectType() const {
        Y_DEBUG_ABORT_UNLESS(KeyType.Defined());
        Y_DEBUG_ABORT_UNLESS(ObjectType.Defined());
        Y_DEBUG_ABORT_UNLESS(KeyType == Type::PGObject);
        return *ObjectType;
    }

    TBackupCollectionDescription GetBackupCollectionPath() const {
        Y_DEBUG_ABORT_UNLESS(KeyType.Defined());
        Y_DEBUG_ABORT_UNLESS(KeyType == Type::BackupCollection);
        Y_DEBUG_ABORT_UNLESS(ExplicitPrefix.Defined());
        return TBackupCollectionDescription {
                .Prefix = *ExplicitPrefix,
                .Name = Target,
            };
    }

    bool Extract(const TExprNode& key);

private:
    TExprContext& Ctx;
    TMaybe<Type> KeyType;
    TString Target;
    TMaybe<TString> ObjectType;
    TMaybe<TViewDescription> View;
    TMaybe<TString> ExplicitPrefix;
};

struct TKiDataQueryBlockSettings {
    static constexpr std::string_view HasUncommittedChangesReadSettingName = "has_uncommitted_changes_read"sv;
    bool HasUncommittedChangesRead = false;

    static TKiDataQueryBlockSettings Parse(const NNodes::TKiDataQueryBlock& node);
    NNodes::TCoNameValueTupleList BuildNode(TExprContext& ctx, TPositionHandle pos) const;
};

struct TKiExecDataQuerySettings {
    TMaybe<TString> Mode;
    TVector<NNodes::TCoNameValueTuple> Other;

    NNodes::TCoNameValueTupleList BuildNode(TExprContext& ctx, TPositionHandle pos) const;

    static TKiExecDataQuerySettings Parse(NNodes::TKiExecDataQuery exec);
};

struct TWriteBackupCollectionSettings {
    NNodes::TMaybeNode<NNodes::TCoAtom> Mode;
    NNodes::TMaybeNode<NNodes::TKiBackupCollectionEntryList> Entries;
    NNodes::TMaybeNode<NNodes::TCoNameValueTupleList> BackupCollectionSettings;
    NNodes::TCoNameValueTupleList Other;

    TWriteBackupCollectionSettings(const NNodes::TCoNameValueTupleList& other)
        : Other(other)
    {}
};

TAutoPtr<IGraphTransformer> CreateKiSourceTypeAnnotationTransformer(TIntrusivePtr<TKikimrSessionContext> sessionCtx,
    TTypeAnnotationContext& types);
TAutoPtr<IGraphTransformer> CreateKiSinkTypeAnnotationTransformer(TIntrusivePtr<IKikimrGateway> gateway,
    TIntrusivePtr<TKikimrSessionContext> sessionCtx, TTypeAnnotationContext& types);
TAutoPtr<IGraphTransformer> CreateKiLogicalOptProposalTransformer(TIntrusivePtr<TKikimrSessionContext> sessionCtx,
    TTypeAnnotationContext& types);
TAutoPtr<IGraphTransformer> CreateKiPhysicalOptProposalTransformer(TIntrusivePtr<TKikimrSessionContext> sessionCtx);
TAutoPtr<IGraphTransformer> CreateKiSourceLoadTableMetadataTransformer(TIntrusivePtr<IKikimrGateway> gateway,
    TIntrusivePtr<TKikimrSessionContext> sessionCtx,
    TTypeAnnotationContext& types,
    const NKikimr::NExternalSource::IExternalSourceFactory::TPtr& sourceFactory,
    bool isInternalCall);
TAutoPtr<IGraphTransformer> CreateKiSinkIntentDeterminationTransformer(TIntrusivePtr<TKikimrSessionContext> sessionCtx);

TAutoPtr<IGraphTransformer> CreateKiSourceCallableExecutionTransformer(
    TIntrusivePtr<IKikimrGateway> gateway,
    TIntrusivePtr<TKikimrSessionContext> sessionCtx,
    TTypeAnnotationContext& types);

TAutoPtr<IGraphTransformer> CreateKiSinkCallableExecutionTransformer(
    TIntrusivePtr<IKikimrGateway> gateway,
    TIntrusivePtr<TKikimrSessionContext> sessionCtx,
    TIntrusivePtr<IKikimrQueryExecutor> queryExecutor);

TAutoPtr<IGraphTransformer> CreateKiSinkPlanInfoTransformer(TIntrusivePtr<IKikimrQueryExecutor> queryExecutor);

NNodes::TCoAtomList BuildColumnsList(const TKikimrTableDescription& table, TPositionHandle pos,
    TExprContext& ctx, bool withSystemColumns, bool ignoreWriteOnlyColumns);

const TTypeAnnotationNode* GetReadTableRowType(TExprContext& ctx, const TKikimrTablesData& tablesData,
    const TString& cluster, const TString& table, NNodes::TCoAtomList select, bool withSystemColumns = false);

const TTypeAnnotationNode* GetReadTableRowType(TExprContext& ctx, const TKikimrTablesData& tablesData,
    const TString& cluster, const TString& table, TPositionHandle pos, bool withSystemColumns);

TYdbOperation GetTableOp(const NNodes::TKiWriteTable& write);
TVector<NKqpProto::TKqpTableOp> TableOperationsToProto(const NNodes::TCoNameValueTupleList& operations,
    TExprContext& ctx);
TVector<NKqpProto::TKqpTableOp> TableOperationsToProto(const NNodes::TKiOperationList& operations, TExprContext& ctx);

void TableDescriptionToTableInfo(const TKikimrTableDescription& desc, TYdbOperation op,
    NProtoBuf::RepeatedPtrField<NKqpProto::TKqpTableInfo>& infos);
void TableDescriptionToTableInfo(const TKikimrTableDescription& desc, TYdbOperation op,
    TVector<NKqpProto::TKqpTableInfo>& infos);

Ydb::Table::VectorIndexSettings_Metric VectorIndexSettingsParseDistance(std::string_view distance);
Ydb::Table::VectorIndexSettings_Metric VectorIndexSettingsParseSimilarity(std::string_view similarity);
Ydb::Table::VectorIndexSettings_VectorType VectorIndexSettingsParseVectorType(std::string_view vectorType);

bool IsPgNullExprNode(const NNodes::TExprBase& maybeLiteral);
std::optional<TString> FillLiteralProto(NNodes::TExprBase maybeLiteral, const TTypeAnnotationNode* valueType, Ydb::TypedValue& proto);
void FillLiteralProto(const NNodes::TCoDataCtor& literal, Ydb::TypedValue& proto);
// todo gvit switch to ydb typed value.
void FillLiteralProto(const NNodes::TCoDataCtor& literal, NKqpProto::TKqpPhyLiteralValue& proto);
void FillLiteralProto(const NNodes::TCoPgConst& literal, NKqpProto::TKqpPhyLiteralValue& proto);

// Optimizer rules
TExprNode::TPtr KiBuildQuery(NNodes::TExprBase node, TExprContext& ctx, TStringBuf database, TIntrusivePtr<TKikimrTablesData> tablesData,
    TTypeAnnotationContext& types, bool sequentialResults);
TExprNode::TPtr KiBuildResult(NNodes::TExprBase node,  const TString& cluster, TExprContext& ctx);

const THashSet<TStringBuf>& KikimrDataSourceFunctions();
const THashSet<TStringBuf>& KikimrDataSinkFunctions();
const THashSet<TStringBuf>& KikimrSupportedEffects();

const THashSet<TStringBuf>& KikimrCommitModes();
const TStringBuf& KikimrCommitModeFlush();
const TStringBuf& KikimrCommitModeRollback();
const TStringBuf& KikimrCommitModeScheme();

const TMap<TString, NKikimr::NUdf::EDataSlot>& KikimrSystemColumns();
bool IsKikimrSystemColumn(const TStringBuf columnName);

bool ValidateTableHasIndex(TKikimrTableMetadataPtr metadata, TExprContext& ctx, const TPositionHandle& pos);

TExprNode::TPtr BuildExternalTableSettings(TPositionHandle pos, TExprContext& ctx, const TMap<TString, NYql::TKikimrColumnMetadata>& columns, const NKikimr::NExternalSource::IExternalSource::TPtr& source, const TString& content);
TString FillAuthProperties(THashMap<TString, TString>& properties, const TExternalSource& externalSource);

TWriteBackupCollectionSettings ParseWriteBackupCollectionSettings(NNodes::TExprList node, TExprContext& ctx);

} // namespace NYql
