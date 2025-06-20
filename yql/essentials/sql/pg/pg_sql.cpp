#include "../../parser/pg_wrapper/pg_compat.h"

#ifdef _WIN32
#define __restrict
#endif

#define TypeName PG_TypeName // NOLINT(readability-identifier-naming)
#define SortBy PG_SortBy // NOLINT(readability-identifier-naming)
#undef SIZEOF_SIZE_T
extern "C" {
#include "postgres.h"
#include "nodes/pg_list.h"
#include "nodes/parsenodes.h"
#include "nodes/value.h"
#undef Min
#undef Max
#undef TypeName
#undef SortBy

#undef TRACE
#undef INFO
#undef WARNING
#undef ERROR
#undef FATAL
#undef NOTICE
}

#include "util/charset/utf8.h"
#include "utils.h"
#include <yql/essentials/ast/yql_expr.h>
#include <yql/essentials/sql/settings/partitioning.h>
#include <yql/essentials/sql/settings/translator.h>
#include <yql/essentials/parser/pg_wrapper/interface/config.h>
#include <yql/essentials/parser/pg_wrapper/interface/parser.h>
#include <yql/essentials/parser/pg_wrapper/interface/utils.h>
#include <yql/essentials/parser/pg_wrapper/interface/raw_parser.h>
#include <yql/essentials/parser/pg_wrapper/postgresql/src/backend/catalog/pg_type_d.h>
#include <yql/essentials/parser/pg_catalog/catalog.h>
#include <yql/essentials/providers/common/provider/yql_provider_names.h>
#include <yql/essentials/minikql/mkql_type_builder.h>
#include <yql/essentials/core/issue/yql_issue.h>
#include <yql/essentials/core/sql_types/yql_callable_names.h>
#include <yql/essentials/parser/pg_catalog/catalog.h>
#include <yql/essentials/utils/log/log_level.h>
#include <yql/essentials/utils/log/log.h>
#include <util/string/builder.h>
#include <util/string/cast.h>
#include <util/string/join.h>
#include <util/string/split.h>
#include <util/generic/scope.h>
#include <util/generic/stack.h>
#include <util/generic/hash_set.h>

constexpr auto PREPARED_PARAM_PREFIX =  "$p";
constexpr auto AUTO_PARAM_PREFIX =  "a";
constexpr auto DEFAULT_PARAM_TYPE = "unknown";

namespace NSQLTranslationPG {

using namespace NYql;

static const THashSet<TString> SystemColumns = { "tableoid", "xmin", "cmin", "xmax", "cmax", "ctid" };

template <typename T>
const T* CastNode(const void* nodeptr, int tag) {
    Y_ENSURE(nodeTag(nodeptr) == tag);
    return static_cast<const T*>(nodeptr);
}

const Node* Expr2Node(const Expr* e) {
    return reinterpret_cast<const Node*>(e);
}

int NodeTag(const Node* node) {
    return nodeTag(node);
}

int NodeTag(const ValUnion& val) {
    return NodeTag(&val.node);
}

int IntVal(const ValUnion& val) {
    Y_ENSURE(val.node.type == T_Integer);
    return intVal(&val.node);
}

bool BoolVal(const ValUnion& val) {
    Y_ENSURE(val.node.type == T_Boolean);
    return boolVal(&val.node);
}

const char* StrFloatVal(const ValUnion& val) {
    Y_ENSURE(val.node.type == T_Float);
    return strVal(&val.node);
}

const char* StrVal(const ValUnion& val) {
    Y_ENSURE(val.node.type == T_String || val.node.type == T_BitString);
    return strVal(&val.node);
}

int BoolVal(const Node* node) {
    Y_ENSURE(node->type == T_Boolean);
    return boolVal(node);
}

int IntVal(const Node* node) {
    Y_ENSURE(node->type == T_Integer);
    return intVal(node);
}

double FloatVal(const Node* node) {
    Y_ENSURE(node->type == T_Float);
    return floatVal(node);
}

const char* StrFloatVal(const Node* node) {
    Y_ENSURE(node->type == T_Float);
    return strVal(node);
}

const char* StrVal(const Node* node) {
    Y_ENSURE(node->type == T_String || node->type == T_BitString);
    return strVal(node);
}

bool ValueAsString(const ValUnion& val, bool isNull, TString& ret) {
    if (isNull) {
        ret = "NULL";
        return true;
    }

    switch (NodeTag(val)) {
    case T_Boolean: {
        ret = BoolVal(val) ? "t" : "f";
        return true;
    }
    case T_Integer: {
        ret = ToString(IntVal(val));
        return true;
    }
    case T_Float: {
        ret = StrFloatVal(val);
        return true;
    }
    case T_String:
    case T_BitString: {
        ret = StrVal(val);
        return true;
    }
    default:
        return false;
    }
}

int ListLength(const List* list) {
    return list_length(list);
}

int StrLength(const char* s) {
    return s ? strlen(s) : 0;
}

int StrCompare(const char* s1, const char* s2) {
    return strcmp(s1 ? s1 : "", s2 ? s2 : "");
}

int StrICompare(const char* s1, const char* s2) {
    return stricmp(s1 ? s1 : "", s2 ? s2 : "");
}

std::shared_ptr<List> ListMake1(void* cell) {
    return std::shared_ptr<List>(list_make1(cell), list_free);
}

#define CAST_NODE(nodeType, nodeptr) CastNode<nodeType>(nodeptr, T_##nodeType)
#define CAST_NODE_EXT(nodeType, tag, nodeptr) CastNode<nodeType>(nodeptr, tag)
#define LIST_CAST_NTH(nodeType, list, index) CAST_NODE(nodeType, list_nth(list, index))
#define LIST_CAST_EXT_NTH(nodeType, tag, list, index) CAST_NODE_EXT(nodeType, tag, list_nth(list, i))

const Node* ListNodeNth(const List* list, int index) {
    return static_cast<const Node*>(list_nth(list, index));
}

const IndexElem* IndexElement(const Node* node) {
    Y_ENSURE(node->type == T_IndexElem);
    return ((const IndexElem*)node);
}

#define AT_LOCATION(node) \
    TLocationGuard guard(this, node->location);

#define AT_LOCATION_EX(node, field) \
    TLocationGuard guard(this, node->field);

std::tuple<TStringBuf, TStringBuf> GetSchemaAndObjectName(const List* nameList)   {
    switch (ListLength(nameList)) {
        case 2: {
            const auto clusterName = StrVal(ListNodeNth(nameList, 0));
            const auto tableName = StrVal(ListNodeNth(nameList, 1));
            return {clusterName, tableName};
        }
        case 1: {
            const auto tableName = StrVal(ListNodeNth(nameList, 0));
            return {"", tableName};
        }
        default: {
            return {"", ""};
        }
    }
}

struct TPgConst {
    TMaybe<TString> Value;
    enum class EType {
        boolean,
        int4,
        int8,
        numeric,
        text,
        unknown,
        bit,
        nil,
    };

    static TString ToString(const TPgConst::EType& type) {
        switch (type) {
            case TPgConst::EType::boolean:
                return "bool";
            case TPgConst::EType::int4:
                return "int4";
            case TPgConst::EType::int8:
                return "int8";
            case TPgConst::EType::numeric:
                return "numeric";
            case TPgConst::EType::text:
                return "text";
            case TPgConst::EType::unknown:
                return "unknown";
            case TPgConst::EType::bit:
                return "bit";
            case TPgConst::EType::nil:
                return "unknown";
            }
    }

    EType Type;
};

TMaybe<TPgConst> GetValueNType(const A_Const* value) {
    TPgConst pgConst;
    if (value->isnull) {
        pgConst.Type = TPgConst::EType::nil;
        return pgConst;
    }

    const auto& val = value->val;
    switch (NodeTag(val)) {
        case T_Boolean: {
            pgConst.Value = BoolVal(val) ? "t" : "f";
            pgConst.Type = TPgConst::EType::boolean;
            return pgConst;
        }
        case T_Integer: {
            pgConst.Value = ToString(IntVal(val));
            pgConst.Type = TPgConst::EType::int4;
            return pgConst;
        }
        case T_Float: {
            auto s = StrFloatVal(val);
            i64 v;
            const bool isInt8 = TryFromString<i64>(s, v);
            pgConst.Value = ToString(s);
            pgConst.Type = isInt8 ? TPgConst::EType::int8 : TPgConst::EType::numeric;
            return pgConst;
        }
        case T_String: {
            pgConst.Value = ToString(StrVal(val));
            pgConst.Type = TPgConst::EType::unknown; // to support implicit casts
            return pgConst;
        }
        case T_BitString: {
            pgConst.Value = ToString(StrVal(val));
            pgConst.Type = TPgConst::EType::bit;
            return pgConst;
        }
        default: {
            return {};
        }
    }
}

class TConverter : public IPGParseEvents {
    friend class TLocationGuard;

private:
    class TLocationGuard {
    private:
        TConverter* Owner_;

    public:
        TLocationGuard(TConverter* owner, int location)
            : Owner_(owner)
        {
            Owner_->PushPosition(location);
        }

        ~TLocationGuard() {
            Owner_->PopPosition();
        }
    };

public:
    struct TFromDesc {
        TAstNode* Source = nullptr;
        TString Alias;
        TVector<TString> ColNames;
        bool InjectRead = false;
    };

    struct TReadWriteKeyExprs {
        TAstNode* SinkOrSource = nullptr;
        TAstNode* Key = nullptr;
    };

    struct TExprSettings {
        bool AllowColumns = false;
        bool AllowAggregates = false;
        bool AllowOver = false;
        bool AllowReturnSet = false;
        bool AllowSubLinks = false;
        bool AutoParametrizeEnabled = true;
        TVector<TAstNode*>* WindowItems = nullptr;
        TString Scope;
    };

    struct TView {
        TString Name;
        TVector<TString> ColNames;
        TAstNode* Source = nullptr;
    };

    using TViews = THashMap<TString, TView>;

    struct TState {
        TMaybe<TString> ApplicationName;
        TString CostBasedOptimizer;
        TVector<TAstNode*> Statements;
        ui32 ReadIndex = 0;
        TViews Views;
        TVector<TViews> CTE;
        const TView* CurrentRecursiveView = nullptr;
        TVector<NYql::TPosition> Positions = {NYql::TPosition()};
        THashMap<TString, TString> ParamNameToPgTypeName;
        NYql::IAutoParamBuilderPtr AutoParamValues;
    };

    TConverter(TVector<TAstParseResult>& astParseResults, const NSQLTranslation::TTranslationSettings& settings,
            const TString& query, TVector<TStmtParseInfo>* stmtParseInfo, bool perStatementResult,
            TMaybe<ui32> sqlProcArgsCount)
        : AstParseResults_(astParseResults)
        , Settings_(settings)
        , DqEngineEnabled_(Settings_.DqDefaultAuto->Allow())
        , BlockEngineEnabled_(Settings_.BlockDefaultAuto->Allow())
        , StmtParseInfo_(stmtParseInfo)
        , PerStatementResult_(perStatementResult)
        , SqlProcArgsCount_(sqlProcArgsCount)
    {
        Y_ENSURE(settings.Mode == NSQLTranslation::ESqlMode::QUERY || settings.Mode == NSQLTranslation::ESqlMode::LIMITED_VIEW);
        Y_ENSURE(settings.Mode != NSQLTranslation::ESqlMode::LIMITED_VIEW || !perStatementResult);
        State_.ApplicationName = Settings_.ApplicationName;
        AstParseResults_.push_back({});
        if (StmtParseInfo_) {
            StmtParseInfo_->push_back({});
        }
        ScanRows(query);

        for (auto& flag : Settings_.Flags) {
            if (flag == "DqEngineEnable") {
                DqEngineEnabled_ = true;
            } else if (flag == "DqEngineForce") {
                DqEngineForce_ = true;
            } else if (flag == "BlockEngineEnable") {
                BlockEngineEnabled_ = true;
            } else if (flag == "BlockEngineForce") {
                BlockEngineForce_ = true;
            } if (flag == "UnorderedResult") {
                UnorderedResult_ = true;
            }
        }

        if (Settings_.PathPrefix) {
            TablePathPrefix_ = Settings_.PathPrefix + "/";
        }

        for (const auto& [cluster, provider] : Settings_.ClusterMapping) {
            if (provider != PgProviderName) {
                Provider_ = provider;
                break;
            }
        }
        if (!Provider_) {
            Provider_ = PgProviderName;
        }
        Y_ENSURE(!Provider_.empty());

        for (size_t i = 0; i < Settings_.PgParameterTypeOids.size(); ++i) {
            const auto paramName = PREPARED_PARAM_PREFIX + ToString(i + 1);
            const auto typeOid = Settings_.PgParameterTypeOids[i];
            const auto& typeName =
                typeOid != UNKNOWNOID ? NPg::LookupType(typeOid).Name : DEFAULT_PARAM_TYPE;
            State_.ParamNameToPgTypeName[paramName] = typeName;
        }

    }

    void OnResult(const List* raw) {
        if (!PerStatementResult_) {
            AstParseResults_[StatementId_].Pool = std::make_unique<TMemoryPool>(4096);
            AstParseResults_[StatementId_].Root = ParseResult(raw);
            AstParseResults_[StatementId_].PgAutoParamValues = State_.AutoParamValues;
            return;
        }
        AstParseResults_.resize(ListLength(raw));
        if (StmtParseInfo_) {
            StmtParseInfo_->resize(AstParseResults_.size());
        }
        for (; StatementId_ < AstParseResults_.size(); ++StatementId_) {
            AstParseResults_[StatementId_].Pool = std::make_unique<TMemoryPool>(4096);
            AstParseResults_[StatementId_].Root = ParseResult(raw, StatementId_);
            AstParseResults_[StatementId_].PgAutoParamValues = State_.AutoParamValues;
            State_ = {};
        }
    }

    void OnError(const TIssue& issue) {
        AstParseResults_[StatementId_].Issues.AddIssue(issue);
    }

    void PrepareStatements() {
        auto configSource = L(A("DataSource"), QA(TString(NYql::ConfigProviderName)));
        State_.Statements.push_back(L(A("let"), A("world"), L(A(TString(NYql::ConfigureName)), A("world"), configSource,
            QA("OrderedColumns"))));
    }

    TAstNode* ParseResult(const List* raw, const TMaybe<ui32> statementId = Nothing()) {
        PrepareStatements();

        auto configSource = L(A("DataSource"), QA(TString(NYql::ConfigProviderName)));
        ui32 blockEnginePgmPos = State_.Statements.size();
        State_.Statements.push_back(configSource);
        ui32 costBasedOptimizerPos = State_.Statements.size();
        State_.Statements.push_back(configSource);
        ui32 dqEnginePgmPos = State_.Statements.size();
        State_.Statements.push_back(configSource);

        if (statementId) {
            if (!ParseRawStmt(LIST_CAST_NTH(RawStmt, raw, *statementId))) {
                return nullptr;
            }
        } else {
            for (int i = 0; i < ListLength(raw); ++i) {
                if (!ParseRawStmt(LIST_CAST_NTH(RawStmt, raw, i))) {
                    return nullptr;
                }
            }
        }

        if (!State_.Views.empty()) {
            AddError("Not all views have been dropped");
            return nullptr;
        }

        if (Settings_.EndOfQueryCommit && Settings_.Mode != NSQLTranslation::ESqlMode::LIMITED_VIEW) {
            State_.Statements.push_back(L(A("let"), A("world"), L(A("CommitAll!"),
                A("world"))));
        }

        AddVariableDeclarations();

        if (Settings_.Mode != NSQLTranslation::ESqlMode::LIMITED_VIEW) {
            State_.Statements.push_back(L(A("return"), A("world")));
        }

        if (DqEngineEnabled_) {
            State_.Statements[dqEnginePgmPos] = L(A("let"), A("world"), L(A(TString(NYql::ConfigureName)), A("world"), configSource,
                QA("DqEngine"), QA(DqEngineForce_ ? "force" : "auto")));
        } else {
            State_.Statements.erase(State_.Statements.begin() + dqEnginePgmPos);
        }

        if (State_.CostBasedOptimizer) {
            State_.Statements[costBasedOptimizerPos] = L(A("let"), A("world"), L(A(TString(NYql::ConfigureName)), A("world"), configSource,
                QA("CostBasedOptimizer"), QA(State_.CostBasedOptimizer)));
        } else {
            State_.Statements.erase(State_.Statements.begin() + costBasedOptimizerPos);
        }

        if (BlockEngineEnabled_) {
            State_.Statements[blockEnginePgmPos] = L(A("let"), A("world"), L(A(TString(NYql::ConfigureName)), A("world"), configSource,
                QA("BlockEngine"), QA(BlockEngineForce_ ? "force" : "auto")));
        } else {
            State_.Statements.erase(State_.Statements.begin() + blockEnginePgmPos);
        }

        return FinishStatements();
    }

    TAstNode* FinishStatements() {
        return VL(State_.Statements.data(), State_.Statements.size());
    }

    [[nodiscard]]
    bool ParseRawStmt(const RawStmt* value) {
        AT_LOCATION_EX(value, stmt_location);
        auto node = value->stmt;
        if (Settings_.Mode == NSQLTranslation::ESqlMode::LIMITED_VIEW) {
            if (NodeTag(node) != T_SelectStmt && NodeTag(node) != T_VariableSetStmt) {
                AddError("Unsupported statement in LIMITED_VIEW mode");
                return false;
            }
        }
        if (StmtParseInfo_) {
            (*StmtParseInfo_)[StatementId_].CommandTagName = GetCommandName(node);
        }
        switch (NodeTag(node)) {
        case T_SelectStmt:
            return ParseSelectStmt(CAST_NODE(SelectStmt, node), {.Inner = false}) != nullptr;
        case T_InsertStmt:
            return ParseInsertStmt(CAST_NODE(InsertStmt, node)) != nullptr;
        case T_UpdateStmt:
            return ParseUpdateStmt(CAST_NODE(UpdateStmt, node)) != nullptr;
        case T_ViewStmt:
            return ParseViewStmt(CAST_NODE(ViewStmt, node)) != nullptr;
        case T_CreateStmt:
            return ParseCreateStmt(CAST_NODE(CreateStmt, node)) != nullptr;
        case T_DropStmt:
            return ParseDropStmt(CAST_NODE(DropStmt, node)) != nullptr;
        case T_VariableSetStmt:
            {
                // YQL-16284
                const char* node_name = CAST_NODE(VariableSetStmt, node)->name;
                const char* skip_statements[] = {
                    "extra_float_digits",                   // jdbc
                    "application_name",                     // jdbc
                    "statement_timeout",                    // pg_dump
                    "lock_timeout",                         // pg_dump
                    "idle_in_transaction_session_timeout",  // pg_dump
                    "client_encoding",                      // pg_dump
                    "standard_conforming_strings",          // pg_dump
                    "check_function_bodies",                // pg_dump
                    "xmloption",                            // pg_dump
                    "client_min_messages",                  // pg_dump
                    "row_security",                        // pg_dump
                    "escape_string_warning",               // zabbix
                    "bytea_output",                        // zabbix
                    "datestyle",                           // pgadmin 4
                    "timezone",                            // mediawiki
                    NULL,
                };

                for (int i = 0; skip_statements[i] != NULL; i++){
                    const char *skip_name = skip_statements[i];
                    if (stricmp(node_name, skip_name) == 0){
                        return true;
                    }
                };
            };

            return ParseVariableSetStmt(CAST_NODE(VariableSetStmt, node)) != nullptr;
        case T_DeleteStmt:
            return ParseDeleteStmt(CAST_NODE(DeleteStmt, node)) != nullptr;
        case T_VariableShowStmt:
            return ParseVariableShowStmt(CAST_NODE(VariableShowStmt, node)) != nullptr;
        case T_TransactionStmt:
            return ParseTransactionStmt(CAST_NODE(TransactionStmt, node));
        case T_IndexStmt:
            return ParseIndexStmt(CAST_NODE(IndexStmt, node)) != nullptr;
        case T_CreateSeqStmt:
            return ParseCreateSeqStmt(CAST_NODE(CreateSeqStmt, node)) != nullptr;
        case T_AlterSeqStmt:
            return ParseAlterSeqStmt(CAST_NODE(AlterSeqStmt, node)) != nullptr;
        case T_AlterTableStmt:
            return ParseAlterTableStmt(CAST_NODE(AlterTableStmt, node)) != nullptr;
        default:
            NodeNotImplemented(value, node);
            return false;
        }
    }

    [[nodiscard]]
    bool ExtractPgConstsForAutoParam(List* rawValuesLists, TVector<TPgConst>& pgConsts) {
        YQL_LOG_CTX_SCOPE(TStringBuf("PgSql Autoparametrize"), __FUNCTION__);
        Y_ABORT_UNLESS(rawValuesLists);
        size_t rows = ListLength(rawValuesLists);

        if (rows == 0 || !Settings_.AutoParametrizeEnabled || !Settings_.AutoParametrizeValuesStmt) {
            return false;
        }

        size_t cols = ListLength(CAST_NODE(List, ListNodeNth(rawValuesLists, 0)));
        pgConsts.reserve(rows * cols);

        for (int rowIdx = 0; rowIdx < ListLength(rawValuesLists); ++rowIdx) {
            const auto rawRow = CAST_NODE(List, ListNodeNth(rawValuesLists, rowIdx));

            for (int colIdx = 0; colIdx < ListLength(rawRow); ++colIdx) {
                const auto rawCell = ListNodeNth(rawRow, colIdx);
                if (NodeTag(rawCell) != T_A_Const) {
                    YQL_CLOG(INFO, Default) << "Auto parametrization of " << NodeTag(rawCell) << " is not supported";
                    return false;
                }
                auto pgConst = GetValueNType(CAST_NODE(A_Const, rawCell));
                if (!pgConst) {
                    return false;
                }
                pgConsts.push_back(std::move(pgConst.GetRef()));
            }
        }
        return true;
    }

    TMaybe<TVector<TPgConst::EType>> InferColumnTypesForValuesStmt(const TVector<TPgConst>& values, size_t cols) {
        Y_ABORT_UNLESS((values.size() % cols == 0), "wrong amount of columns for auto param values vector");
        TVector<TMaybe<TPgConst::EType>> maybeColumnTypes(cols);

        for (size_t i = 0; i < values.size(); ++i) {
            const auto& value = values[i];
            size_t col = i % cols;
            auto& columnType = maybeColumnTypes[col];

            if (!columnType || columnType.GetRef() == TPgConst::EType::unknown || columnType.GetRef() == TPgConst::EType::nil) {
                columnType = value.Type;
                continue;
            }

            // should we allow compatible types here?
            if (columnType.GetRef() != value.Type && columnType.GetRef() != TPgConst::EType::unknown && columnType.GetRef() != TPgConst::EType::nil) {
                YQL_CLOG(INFO, Default)
                    << "Failed to auto parametrize: different types: "
                    << TPgConst::ToString(columnType.GetRef()) << " and " << TPgConst::ToString(value.Type)
                    << " in col " << col;
                return {};
            }
        }

        TVector<TPgConst::EType> columnTypes;
        for (auto& maybeColumnType: maybeColumnTypes) {
            if (maybeColumnType.Empty()) {
                YQL_CLOG(INFO, Default) << "Failed to auto parametrize: can't infer PgType for column";
                return {};
            }
            columnTypes.emplace_back(maybeColumnType.GetRef());
        }
        return columnTypes;
    }

    TString AddSimpleAutoParam(TPgConst&& valueNType) {
        if (!State_.AutoParamValues) {
            Y_ENSURE(Settings_.AutoParamBuilderFactory);
            State_.AutoParamValues = Settings_.AutoParamBuilderFactory->MakeBuilder();
        }

        auto nextName = TString(AUTO_PARAM_PREFIX) + ToString(State_.AutoParamValues->Size());
        auto& type = State_.AutoParamValues->Add(nextName);
        type.Pg(TPgConst::ToString(valueNType.Type));
        auto& data = type.FinishType();
        data.Pg(valueNType.Value);
        data.FinishData();
        return nextName;
    }

    TString AddValuesAutoParam(TVector<TPgConst>&& values, TVector<TPgConst::EType>&& columnTypes) {
        if (!State_.AutoParamValues) {
            Y_ENSURE(Settings_.AutoParamBuilderFactory);
            State_.AutoParamValues = Settings_.AutoParamBuilderFactory->MakeBuilder();
        }

        auto nextName = TString(AUTO_PARAM_PREFIX) + ToString(State_.AutoParamValues->Size());
        auto& type = State_.AutoParamValues->Add(nextName);
        type.BeginList();
        type.BeginTuple();
        for (const auto& t : columnTypes) {
            type.BeforeItem();
            type.Pg(TPgConst::ToString(t));
            type.AfterItem();
        }

        type.EndTuple();
        type.EndList();
        auto& data = type.FinishType();
        data.BeginList();
        size_t cols = columnTypes.size();
        for (size_t idx = 0; idx < values.size(); idx += cols){
            data.BeforeItem();
            data.BeginTuple();
            for (size_t delta = 0; delta < cols; ++delta) {
                data.BeforeItem();
                data.Pg(values[idx + delta].Value);
                data.AfterItem();
            }

            data.EndTuple();
            data.AfterItem();
        }

        data.EndList();
        data.FinishData();
        return nextName;
    }

    TAstNode* MakeValuesStmtAutoParam(TVector<TPgConst>&& values, TVector<TPgConst::EType>&& columnTypes) {
        TVector<TAstNode*> autoParamTupleType;
        autoParamTupleType.reserve(columnTypes.size());
        autoParamTupleType.push_back(A("TupleType"));
        for (const auto& type : columnTypes) {
            auto pgType = L(A("PgType"), QA(TPgConst::ToString(type)));
            autoParamTupleType.push_back(pgType);
        }
        const auto paramType = L(A("ListType"), VL(autoParamTupleType));

        const auto paramName = AddValuesAutoParam(std::move(values), std::move(columnTypes));
        State_.Statements.push_back(L(A("declare"), A(paramName), paramType));

        YQL_CLOG(INFO, Default) << "Successfully autoparametrized VALUES at" << State_.Positions.back();

        return A(paramName);
    }

    [[nodiscard]]
    TAstNode* ParseValuesList(List* valuesLists, bool buildCommonType) {
        TVector<TAstNode*> valNames;
        uint64 colIdx = 0;

        TExprSettings settings;
        settings.AllowColumns = false;
        settings.Scope = "VALUES";

        for (int valueIndex = 0; valueIndex < ListLength(valuesLists); ++valueIndex) {
            auto node = ListNodeNth(valuesLists, valueIndex);
            if (NodeTag(node) != T_List) {
                NodeNotImplemented(node);
                return nullptr;
            }

            auto lst = CAST_NODE(List, node);
            if (valueIndex == 0) {
                for (int item = 0; item < ListLength(lst); ++item) {
                    valNames.push_back(QA("column" + ToString(colIdx++)));
                }
            } else {
                if (ListLength(lst) != (int)valNames.size()) {
                    AddError("VALUES lists must all be the same length");
                    return nullptr;
                }
            }
        }

        TVector<TPgConst> pgConsts;
        bool canAutoparametrize = ExtractPgConstsForAutoParam(valuesLists, pgConsts);
        if (canAutoparametrize) {
            auto maybeColumnTypes = InferColumnTypesForValuesStmt(pgConsts, valNames.size());
            if (maybeColumnTypes) {
                auto valuesNode = MakeValuesStmtAutoParam(std::move(pgConsts), std::move(maybeColumnTypes.GetRef()));
                return QL(QA("values"), QVL(valNames.data(), valNames.size()), valuesNode);
            }
        }

        TVector<TAstNode*> valueRows;
        valueRows.reserve(ListLength(valuesLists));
        valueRows.push_back(A(buildCommonType ? "PgValuesList" : "AsList"));
        for (int valueIndex = 0; valueIndex < ListLength(valuesLists); ++valueIndex) {
            auto node = ListNodeNth(valuesLists, valueIndex);
            if (NodeTag(node) != T_List) {
                NodeNotImplemented(node);
                return nullptr;
            }

            auto lst = CAST_NODE(List, node);
            TVector<TAstNode*> row;

            for (int item = 0; item < ListLength(lst); ++item) {
                auto cell = ParseExpr(ListNodeNth(lst, item), settings);
                if (!cell) {
                    return nullptr;
                }

                row.push_back(cell);
            }

            valueRows.push_back(QVL(row.data(), row.size()));
        }

        return QL(QA("values"), QVL(valNames.data(), valNames.size()), VL(valueRows));
    }

    TAstNode* ParseSetConfig(const FuncCall* value) {
        auto length = ListLength(value->args);
        if (length != 3) {
            AddError(TStringBuilder() << "Expected 3 arguments, but got: " << length);
            return nullptr;
        }

        VariableSetStmt config;
        config.kind = VAR_SET_VALUE;
        auto arg0 = ListNodeNth(value->args, 0);
        auto arg1 = ListNodeNth(value->args, 1);
        auto arg2 = ListNodeNth(value->args, 2);
        if (NodeTag(arg2) != T_A_Const) {
            AddError(TStringBuilder() << "Expected AConst node as is_local arg, but got node with tag: " << NodeTag(arg2));
            return nullptr;
        }
        auto isLocalConst = CAST_NODE(A_Const, arg2);
        if (isLocalConst->isnull) {
            AddError(TStringBuilder() << "Expected t/f, but got null");
            return nullptr;
        }
        if (NodeTag(isLocalConst->val) != T_Boolean) {
            AddError(TStringBuilder() << "Expected bool in const, but got something wrong: " << NodeTag(isLocalConst->val));
            return nullptr;
        }
        config.is_local = BoolVal(isLocalConst->val);

        if (NodeTag(arg0) != T_A_Const || NodeTag(arg1) != T_A_Const) {
            AddError(TStringBuilder() << "Expected const with string, but got something else: " << NodeTag(arg0));
            return nullptr;
        }

        if (CAST_NODE(A_Const, arg0)->isnull || CAST_NODE(A_Const, arg1)->isnull) {
            AddError(TStringBuilder() << "Expected string const as name arg, but got null");
            return nullptr;
        }

        auto name = CAST_NODE(A_Const, arg0)->val;
        auto val = CAST_NODE(A_Const, arg1)->val;
        if (NodeTag(name) != T_String || NodeTag(val) != T_String) {
            AddError(TStringBuilder() << "Expected string const as name arg, but got something else: " << NodeTag(name));
            return nullptr;
        }
        config.name = (char*)StrVal(name);
        config.args = list_make1((void*)arg1);
        return ParseVariableSetStmt(&config, true);
    }

    using TTraverseSelectStack = TStack<std::pair<const SelectStmt*, bool>>;
    using TTraverseNodeStack = TStack<std::pair<const Node*, bool>>;

    struct TSelectStmtSettings {
        bool Inner = true;
        mutable TVector<TAstNode*> TargetColumns;
        bool AllowEmptyResSet = false;
        bool EmitPgStar = false;
        bool FillTargetColumns = false;
        bool UnknownsAllowed = false;
        const TView* Recursive = nullptr;
    };

    [[nodiscard]]
    TAstNode* ParseSelectStmt(
        const SelectStmt* value,
        const TSelectStmtSettings& selectSettings
    ) {
        if (Settings_.Mode == NSQLTranslation::ESqlMode::LIMITED_VIEW) {
            if (HasSelectInLimitedView_) {
                AddError("Expected exactly one SELECT in LIMITED_VIEW mode");
                return nullptr;
            }

            HasSelectInLimitedView_ = true;
        }

        bool isValuesClauseOfInsertStmt = selectSettings.FillTargetColumns;

        State_.CTE.emplace_back();
        auto prevRecursiveView = State_.CurrentRecursiveView;
        State_.CurrentRecursiveView = selectSettings.Recursive;
        Y_DEFER {
            State_.CTE.pop_back();
            State_.CurrentRecursiveView = prevRecursiveView;
        };

        if (value->withClause) {
            if (!ParseWithClause(CAST_NODE(WithClause, value->withClause))) {
                return nullptr;
            }
        }

        TTraverseSelectStack traverseSelectStack;
        traverseSelectStack.push({ value, false });

        TVector<const SelectStmt*> setItems;
        TVector<TAstNode*> setOpsNodes;

        while (!traverseSelectStack.empty()) {
            auto& top = traverseSelectStack.top();
            if (top.first->op == SETOP_NONE) {
                // leaf
                setItems.push_back(top.first);
                setOpsNodes.push_back(QA("push"));
                traverseSelectStack.pop();
            } else {
                if (!top.first->larg || !top.first->rarg) {
                    AddError("SelectStmt: expected larg and rarg");
                    return nullptr;
                }

                if (!top.second) {
                    traverseSelectStack.push({ top.first->rarg, false });
                    traverseSelectStack.push({ top.first->larg, false });
                    top.second = true;
                } else {
                    TString op;
                    switch (top.first->op) {
                    case SETOP_UNION:
                        op = "union"; break;
                    case SETOP_INTERSECT:
                        op = "intersect"; break;
                    case SETOP_EXCEPT:
                        op = "except"; break;
                    default:
                        AddError(TStringBuilder() << "SetOperation unsupported value: " << (int)top.first->op);
                        return nullptr;
                    }

                    if (top.first->all) {
                        op += "_all";
                    }

                    setOpsNodes.push_back(QA(op));
                    traverseSelectStack.pop();
                }
            }
        }

        bool hasCombiningQueries = (1 < setItems.size());

        TAstNode* sort = nullptr;
        if (ListLength(value->sortClause) > 0) {
            TVector<TAstNode*> sortItems;
            for (int i = 0; i < ListLength(value->sortClause); ++i) {
                auto node = ListNodeNth(value->sortClause, i);
                if (NodeTag(node) != T_SortBy) {
                    NodeNotImplemented(value, node);
                    return nullptr;
                }

                auto sort = ParseSortBy(CAST_NODE_EXT(PG_SortBy, T_SortBy, node), !hasCombiningQueries, true);
                if (!sort) {
                    return nullptr;
                }

                sortItems.push_back(sort);
            }

            sort = QVL(sortItems.data(), sortItems.size());
        }

        TVector<TAstNode*> setItemNodes;
        for (size_t id = 0; id < setItems.size(); ++id) {
            const auto& x = setItems[id];
            bool hasDistinctAll = false;
            TVector<TAstNode*> distinctOnItems;
            if (x->distinctClause) {
                if (linitial(x->distinctClause) == NULL) {
                    hasDistinctAll = true;
                } else {
                    for (int i = 0; i < ListLength(x->distinctClause); ++i) {
                        auto node = ListNodeNth(x->distinctClause, i);
                        TAstNode* expr;
                        if (NodeTag(node) == T_A_Const && (NodeTag(CAST_NODE(A_Const, node)->val) == T_Integer)) {
                            expr = MakeProjectionRef("DISTINCT ON", CAST_NODE(A_Const, node));
                        } else {
                            TExprSettings settings;
                            settings.AllowColumns = true;
                            settings.Scope = "DISTINCT ON";
                            expr = ParseExpr(node, settings);
                        }

                        if (!expr) {
                            return nullptr;
                        }


                        auto lambda = L(A("lambda"), QL(), expr);
                        distinctOnItems.push_back(L(A("PgGroup"), L(A("Void")), lambda));
                    }
                }
            }

            if (x->intoClause) {
                AddError("SelectStmt: not supported intoClause");
                return nullptr;
            }

            TVector<TAstNode*> fromList;
            TVector<TAstNode*> joinOps;
            for (int i = 0; i < ListLength(x->fromClause); ++i) {
                auto node = ListNodeNth(x->fromClause, i);
                if (NodeTag(node) != T_JoinExpr) {
                    auto p = ParseFromClause(node);
                    if (!p) {
                        return nullptr;
                    }

                    AddFrom(*p, fromList);
                    joinOps.push_back(QL(QL(QA("push"))));
                } else {
                    TTraverseNodeStack traverseNodeStack;
                    traverseNodeStack.push({ node, false });
                    TVector<TAstNode*> oneJoinGroup;

                    while (!traverseNodeStack.empty()) {
                        auto& top = traverseNodeStack.top();
                        if (NodeTag(top.first) != T_JoinExpr) {
                            // leaf
                            auto p = ParseFromClause(top.first);
                            if (!p) {
                                return nullptr;
                            }
                            AddFrom(*p, fromList);
                            traverseNodeStack.pop();
                            oneJoinGroup.push_back(QL(QA("push")));
                        } else {
                            auto join = CAST_NODE(JoinExpr, top.first);
                            if (!join->larg || !join->rarg) {
                                AddError("JoinExpr: expected larg and rarg");
                                return nullptr;
                            }

                            if (join->alias) {
                                AddError("JoinExpr: unsupported alias");
                                return nullptr;
                            }

                            if (join->isNatural) {
                                AddError("JoinExpr: unsupported isNatural");
                                return nullptr;
                            }

                            if (!top.second) {
                                traverseNodeStack.push({ join->rarg, false });
                                traverseNodeStack.push({ join->larg, false });
                                top.second = true;
                            } else {
                                TString op;
                                switch (join->jointype) {
                                case JOIN_INNER:
                                    op = join->quals ? "inner" : "cross"; break;
                                case JOIN_LEFT:
                                    op = "left"; break;
                                case JOIN_FULL:
                                    op = "full"; break;
                                case JOIN_RIGHT:
                                    op = "right"; break;
                                default:
                                    AddError(TStringBuilder() << "jointype unsupported value: " << (int)join->jointype);
                                    return nullptr;
                                }

                                if (ListLength(join->usingClause) > 0) {
                                    if (join->join_using_alias) {
                                        AddError(TStringBuilder() << "join USING: unsupported AS");
                                        return nullptr;
                                    }
                                    if (op == "cross") {
                                        op = "inner";
                                    }
                                    auto len = ListLength(join->usingClause);
                                    TVector<TAstNode*> fields(len);
                                    THashSet<TString> present;
                                    for (decltype(len) i = 0; i < len; ++i) {
                                        auto node = ListNodeNth(join->usingClause, i);
                                        if (NodeTag(node) != T_String) {
                                            AddError("JoinExpr: unexpected non-string constant");
                                            return nullptr;
                                        }
                                        if (present.contains(StrVal(node))) {
                                            AddError(TStringBuilder() << "USING clause: duplicated column " << StrVal(node));
                                            return nullptr;
                                        }
                                        fields[i] = QAX(StrVal(node));
                                    }
                                    oneJoinGroup.push_back(QL(QA(op), QA("using"), QVL(fields)));
                                } else {

                                    if (op != "cross" && !join->quals) {
                                        AddError("join_expr: expected quals for non-cross join");
                                        return nullptr;
                                    }

                                    if (op == "cross") {
                                        oneJoinGroup.push_back(QL(QA(op)));
                                    } else {
                                        TExprSettings settings;
                                        settings.AllowColumns = true;
                                        settings.Scope = "JOIN ON";
                                        auto quals = ParseExpr(join->quals, settings);
                                        if (!quals) {
                                            return nullptr;
                                        }

                                        auto lambda = L(A("lambda"), QL(), quals);
                                        oneJoinGroup.push_back(QL(QA(op), L(A("PgWhere"), L(A("Void")), lambda)));
                                    }
                                }
                                traverseNodeStack.pop();
                            }
                        }
                    }

                    joinOps.push_back(QVL(oneJoinGroup.data(), oneJoinGroup.size()));
                }
            }

            TAstNode* whereFilter = nullptr;
            if (x->whereClause) {
                TExprSettings settings;
                settings.AllowColumns = true;
                settings.AllowSubLinks = true;
                settings.Scope = "WHERE";
                whereFilter = ParseExpr(x->whereClause, settings);
                if (!whereFilter) {
                    return nullptr;
                }
            }

            TAstNode* groupBy = nullptr;
            if (ListLength(x->groupClause) > 0) {
                TVector<TAstNode*> groupByItems;
                for (int i = 0; i < ListLength(x->groupClause); ++i) {
                    auto node = ListNodeNth(x->groupClause, i);
                    TAstNode* expr;
                    if (NodeTag(node) == T_A_Const && (NodeTag(CAST_NODE(A_Const, node)->val) == T_Integer)) {
                        expr = MakeProjectionRef("GROUP BY", CAST_NODE(A_Const, node));
                    } else {
                        TExprSettings settings;
                        settings.AllowColumns = true;
                        settings.Scope = "GROUP BY";
                        if (NodeTag(node) == T_GroupingSet) {
                            expr = ParseGroupingSet(CAST_NODE(GroupingSet, node), settings);
                        } else {
                            expr = ParseExpr(node, settings);
                        }
                    }

                    if (!expr) {
                        return nullptr;
                    }

                    auto lambda = L(A("lambda"), QL(), expr);
                    groupByItems.push_back(L(A("PgGroup"), L(A("Void")), lambda));
                }

                groupBy = QVL(groupByItems.data(), groupByItems.size());
            }

            TAstNode* having = nullptr;
            if (x->havingClause) {
                TExprSettings settings;
                settings.AllowColumns = true;
                settings.Scope = "HAVING";
                settings.AllowAggregates = true;
                settings.AllowSubLinks = true;
                having = ParseExpr(x->havingClause, settings);
                if (!having) {
                    return nullptr;
                }
            }

            TVector<TAstNode*> windowItems;
            if (ListLength(x->windowClause) > 0) {
                for (int i = 0; i < ListLength(x->windowClause); ++i) {
                    auto node = ListNodeNth(x->windowClause, i);
                    if (NodeTag(node) != T_WindowDef) {
                        NodeNotImplemented(x, node);
                        return nullptr;
                    }

                    auto win = ParseWindowDef(CAST_NODE(WindowDef, node));
                    if (!win) {
                        return nullptr;
                    }

                    windowItems.push_back(win);
                }
            }

            if (ListLength(x->valuesLists) && ListLength(x->fromClause)) {
                AddError("SelectStmt: values_lists isn't compatible to from_clause");
                return nullptr;
            }

            if (!selectSettings.AllowEmptyResSet && (ListLength(x->valuesLists) == 0) && (ListLength(x->targetList) == 0)) {
                AddError("SelectStmt: both values_list and target_list are not allowed to be empty");
                return nullptr;
            }

            if (x != value && ListLength(x->sortClause) > 0) {
                AddError("SelectStmt: sortClause should be used only on top");
                return nullptr;
            }

            if (x != value) {
                if (x->limitOption == LIMIT_OPTION_COUNT || x->limitOption == LIMIT_OPTION_DEFAULT) {
                    if (x->limitCount || x->limitOffset) {
                        AddError("SelectStmt: limit should be used only on top");
                        return nullptr;
                    }
                } else {
                    AddError(TStringBuilder() << "LimitOption unsupported value: " << (int)x->limitOption);
                    return nullptr;
                }

                if (ListLength(x->lockingClause) > 0) {
                    AddWarning(TIssuesIds::PG_NO_LOCKING_SUPPORT, "SelectStmt: lockingClause is ignored");
                }
            }

            TVector<TAstNode*> res;
            ui32 i = 0;
            if (selectSettings.EmitPgStar && id + 1 == setItems.size()) {
                res.emplace_back(CreatePgStarResultItem());
                i++;
            }
            bool maybeSelectWithJustSetConfig = !selectSettings.Inner && !sort && windowItems.empty() && !having && !groupBy && !whereFilter && !x->distinctClause  && ListLength(x->targetList) == 1;
            if (maybeSelectWithJustSetConfig) {
                auto node = ListNodeNth(x->targetList, 0);
                if (NodeTag(node) != T_ResTarget) {
                    NodeNotImplemented(x, node);
                    return nullptr;
                }
                auto r = CAST_NODE(ResTarget, node);
                if (!r->val) {
                    AddError("SelectStmt: expected val");
                    return nullptr;
                }
                auto call = r->val;
                if (NodeTag(call) == T_FuncCall) {
                    auto fn = CAST_NODE(FuncCall, call);
                    if (ListLength(fn->funcname) == 1) {
                        auto nameNode = ListNodeNth(fn->funcname, 0);
                        if (NodeTag(nameNode) != T_String) {
                            AddError("Function name must be string");
                            return nullptr;
                        }
                        auto name = to_lower(TString(StrVal(ListNodeNth(fn->funcname, 0))));
                        if (name == "set_config") {
                            return ParseSetConfig(fn);
                        }
                    }
                }
            }
            for (int targetIndex = 0; targetIndex < ListLength(x->targetList); ++targetIndex) {
                auto node = ListNodeNth(x->targetList, targetIndex);
                if (NodeTag(node) != T_ResTarget) {
                    NodeNotImplemented(x, node);
                    return nullptr;
                }

                auto r = CAST_NODE(ResTarget, node);
                if (!r->val) {
                    AddError("SelectStmt: expected val");
                    return nullptr;
                }

                TExprSettings settings;
                settings.AllowColumns = true;
                settings.AllowAggregates = true;
                settings.AllowOver = true;
                settings.AllowSubLinks = true;
                settings.WindowItems = &windowItems;
                settings.Scope = "SELECT";
                auto x = ParseExpr(r->val, settings);
                if (!x) {
                    return nullptr;
                }
                res.push_back(CreatePgResultItem(r, x, i));
            }

            TVector<TAstNode*> setItemOptions;
            if (selectSettings.EmitPgStar) {
                setItemOptions.push_back(QL(QA("emit_pg_star")));
            }
            if (!selectSettings.TargetColumns.empty()) {
                setItemOptions.push_back(QL(QA("target_columns"), QVL(selectSettings.TargetColumns.data(), selectSettings.TargetColumns.size())));
            }
            if (selectSettings.FillTargetColumns) {
                setItemOptions.push_back(QL(QA("fill_target_columns")));
            }
            if (ListLength(x->targetList) > 0) {
                setItemOptions.push_back(QL(QA("result"), QVL(res.data(), res.size())));
            } else {
                auto valuesList = ParseValuesList(x->valuesLists, /*buildCommonType=*/!isValuesClauseOfInsertStmt);
                if (!valuesList) {
                    return nullptr;
                }
                setItemOptions.push_back(valuesList);
            }

            if (!fromList.empty()) {
                setItemOptions.push_back(QL(QA("from"), QVL(fromList.data(), fromList.size())));
                setItemOptions.push_back(QL(QA("join_ops"), QVL(joinOps.data(), joinOps.size())));
            }

            if (whereFilter) {
                auto lambda = L(A("lambda"), QL(), whereFilter);
                setItemOptions.push_back(QL(QA("where"), L(A("PgWhere"), L(A("Void")), lambda)));
            }

            if (groupBy) {
                setItemOptions.push_back(QL(QA("group_by"), groupBy));
            }

            if (windowItems.size()) {
                auto window = QVL(windowItems.data(), windowItems.size());
                setItemOptions.push_back(QL(QA("window"), window));
            }

            if (having) {
                auto lambda = L(A("lambda"), QL(), having);
                setItemOptions.push_back(QL(QA("having"), L(A("PgWhere"), L(A("Void")), lambda)));
            }

            if (hasDistinctAll) {
                setItemOptions.push_back(QL(QA("distinct_all")));
            } else if (!distinctOnItems.empty()) {
                auto distinctOn = QVL(distinctOnItems.data(), distinctOnItems.size());
                setItemOptions.push_back(QL(QA("distinct_on"), distinctOn));
            }

            if (!hasCombiningQueries && sort) {
                setItemOptions.push_back(QL(QA("sort"), sort));
            }

            if (selectSettings.UnknownsAllowed || hasCombiningQueries) {
                setItemOptions.push_back(QL(QA("unknowns_allowed")));
            }

            auto setItem = L(A("PgSetItem"), QVL(setItemOptions.data(), setItemOptions.size()));
            setItemNodes.push_back(setItem);
        }

        if (value->intoClause) {
            AddError("SelectStmt: not supported intoClause");
            return nullptr;
        }

        if (ListLength(value->lockingClause) > 0) {
            AddWarning(TIssuesIds::PG_NO_LOCKING_SUPPORT, "SelectStmt: lockingClause is ignored");
        }

        TAstNode* limit = nullptr;
        TAstNode* offset = nullptr;
        if (value->limitOption == LIMIT_OPTION_COUNT || value->limitOption == LIMIT_OPTION_DEFAULT) {
            if (value->limitCount) {
                TExprSettings settings;
                settings.AllowColumns = false;
                settings.AllowSubLinks = true;
                settings.Scope = "LIMIT";
                limit = ParseExpr(value->limitCount, settings);
                if (!limit) {
                    return nullptr;
                }
            }

            if (value->limitOffset) {
                TExprSettings settings;
                settings.AllowColumns = false;
                settings.AllowSubLinks = true;
                settings.Scope = "OFFSET";
                offset = ParseExpr(value->limitOffset, settings);
                if (!offset) {
                    return nullptr;
                }
            }
        } else {
            AddError(TStringBuilder() << "LimitOption unsupported value: " << (int)value->limitOption);
            return nullptr;
        }

        TVector<TAstNode*> selectOptions;

        selectOptions.push_back(QL(QA("set_items"), QVL(setItemNodes.data(), setItemNodes.size())));
        selectOptions.push_back(QL(QA("set_ops"), QVL(setOpsNodes.data(), setOpsNodes.size())));

        if (hasCombiningQueries && sort) {
            selectOptions.push_back(QL(QA("sort"), sort));
        }

        if (limit) {
            selectOptions.push_back(QL(QA("limit"), limit));
        }

        if (offset) {
            selectOptions.push_back(QL(QA("offset"), offset));
        }

        auto output = L(A("PgSelect"), QVL(selectOptions.data(), selectOptions.size()));

        if (selectSettings.Inner) {
            return output;
        }

        if (Settings_.Mode == NSQLTranslation::ESqlMode::LIMITED_VIEW) {
            State_.Statements.push_back(L(A("return"), L(A("Right!"), L(A("Cons!"), A("world"), output))));
            return State_.Statements.back();
        }

        auto resOptions = BuildResultOptions(!sort);
        State_.Statements.push_back(L(A("let"), A("output"), output));
        State_.Statements.push_back(L(A("let"), A("result_sink"), L(A("DataSink"), QA(TString(NYql::ResultProviderName)))));
        State_.Statements.push_back(L(A("let"), A("world"), L(A("Write!"),
            A("world"), A("result_sink"), L(A("Key")), A("output"), resOptions)));
        State_.Statements.push_back(L(A("let"), A("world"), L(A("Commit!"),
            A("world"), A("result_sink"))));
        return State_.Statements.back();
    }

    TAstNode* BuildResultOptions(bool unordered) {
        TVector<TAstNode*> options;
        options.push_back(QL(QA("type")));
        options.push_back(QL(QA("autoref")));
        if (unordered && UnorderedResult_) {
            options.push_back(QL(QA("unordered")));
        }

        return QVL(options.data(), options.size());
    }

    [[nodiscard]]
    bool ParseWithClause(const WithClause* value) {
        AT_LOCATION(value);
        for (int i = 0; i < ListLength(value->ctes); ++i) {
            auto object = ListNodeNth(value->ctes, i);
            if (NodeTag(object) != T_CommonTableExpr) {
                NodeNotImplemented(value, object);
                return false;
            }

            if (!ParseCTE(CAST_NODE(CommonTableExpr, object), value->recursive)) {
                return false;
            }
        }

        return true;
    }

    [[nodiscard]]
    bool ParseCTE(const CommonTableExpr* value, bool recursive) {
        AT_LOCATION(value);
        TView view;
        view.Name = value->ctename;

        for (int i = 0; i < ListLength(value->aliascolnames); ++i) {
            auto node = ListNodeNth(value->aliascolnames, i);
            if (NodeTag(node) != T_String) {
                NodeNotImplemented(value, node);
                return false;
            }

            view.ColNames.push_back(StrVal(node));
        }

        if (NodeTag(value->ctequery) != T_SelectStmt) {
            AddError("Expected Select statement as CTE query");
            return false;
        }

        view.Source = ParseSelectStmt(CAST_NODE(SelectStmt, value->ctequery), {
            .Inner = true,
            .Recursive = recursive ? &view : nullptr
        });

        if (!view.Source) {
            return false;
        }

        auto& currentCTEs = State_.CTE.back();
        if (currentCTEs.find(view.Name) != currentCTEs.end()) {
            AddError(TStringBuilder() << "CTE already exists: '" << view.Name << "'");
            return false;
        }

        currentCTEs[view.Name] = view;
        return true;
    }

    [[nodiscard]]
    TAstNode* AsScalarContext(TAstNode* subquery) {
        return L(A("SingleMember"), L(A("Head"), L(A("Take"), subquery, L(A("Uint64"), QA("1")))));
    }

    [[nodiscard]]
    TAstNode* MakeLambda(TVector<TAstNode*> args, TAstNode* body) {
        return L(A("lambda"), QVL(args), body);
    }

    [[nodiscard]]
    TAstNode* CreatePgStarResultItem() {
        TAstNode* starLambda = L(A("lambda"), QL(), L(A("PgStar")));
        return L(A("PgResultItem"), QAX(""), L(A("Void")), starLambda);
    }

    [[nodiscard]]
    TAstNode* CreatePgResultItem(const ResTarget* r, TAstNode* x, ui32& columnIndex) {
        bool isStar = false;
        if (NodeTag(r->val) == T_ColumnRef) {
            auto ref = CAST_NODE(ColumnRef, r->val);
            for (int fieldNo = 0; fieldNo < ListLength(ref->fields); ++fieldNo) {
                if (NodeTag(ListNodeNth(ref->fields, fieldNo)) == T_A_Star) {
                    isStar = true;
                    break;
                }
            }
        }

        TString name;
        if (!isStar) {
            name = r->name;
            if (name.empty()) {
                if (NodeTag(r->val) == T_ColumnRef) {
                    auto ref = CAST_NODE(ColumnRef, r->val);
                    auto field = ListNodeNth(ref->fields, ListLength(ref->fields) - 1);
                    if (NodeTag(field) == T_String) {
                        name = StrVal(field);
                    }
                } else if (NodeTag(r->val) == T_FuncCall) {
                    auto func = CAST_NODE(FuncCall, r->val);
                    if (!ExtractFuncName(func, name, nullptr)) {
                        return nullptr;
                    }
                }
            }

            if (name.empty()) {
                name = "column" + ToString(columnIndex++);
            }
        }

        const auto lambda = L(A("lambda"), QL(), x);
        const auto columnName = QAX(name);
        return L(A("PgResultItem"), columnName, L(A("Void")), lambda);
    }

    [[nodiscard]]
    std::optional<TVector<TAstNode*>> ParseReturningList(const List* returningList) {
        TVector <TAstNode*> list;
        if (ListLength(returningList) == 0) {
            return {};
        }
        ui32 index = 0;
        for (int i = 0; i < ListLength(returningList); i++) {
            auto node = ListNodeNth(returningList, i);
            if (NodeTag(node) != T_ResTarget) {
                NodeNotImplemented(returningList, node);
                return std::nullopt;
            }
            auto r = CAST_NODE(ResTarget, node);
            if (!r->val) {
                AddError("SelectStmt: expected value");
                return std::nullopt;
            }
            if (NodeTag(r->val) != T_ColumnRef) {
                NodeNotImplemented(r, r->val);
                return std::nullopt;
            }
            TExprSettings settings;
            settings.AllowColumns = true;
            auto columnRef = ParseColumnRef(CAST_NODE(ColumnRef, r->val), settings);
            if (!columnRef) {
                return std::nullopt;
            }
            list.emplace_back(CreatePgResultItem(r, columnRef, index));
        }
        return list;
    }

    [[nodiscard]]
    TAstNode* ParseInsertStmt(const InsertStmt* value) {
        if (value->onConflictClause) {
            AddError("InsertStmt: not supported onConflictClause");
            return nullptr;
        }

        TVector <TAstNode*> returningList;
        if (value->returningList) {
            auto list = ParseReturningList(value->returningList);
            if (list.has_value()) {
                returningList = list.value();
            } else {
                return nullptr;
            }
        }

        if (value->withClause) {
            AddError("InsertStmt: not supported withClause");
            return nullptr;
        }

        const auto [sink, key] = ParseWriteRangeVar(value->relation);
        if (!sink || !key) {
            return nullptr;
        }

        TVector <TAstNode*> targetColumns;
        if (value->cols) {
            for (int i = 0; i < ListLength(value->cols); i++) {
                auto node = ListNodeNth(value->cols, i);
                if (NodeTag(node) != T_ResTarget) {
                    NodeNotImplemented(value, node);
                    return nullptr;
                }
                auto r = CAST_NODE(ResTarget, node);
                if (!r->name) {
                    AddError("SelectStmt: expected name");
                    return nullptr;
                }
                targetColumns.push_back(QA(r->name));
            }
        }

        const auto select = (value->selectStmt)
            ? ParseSelectStmt(
                CAST_NODE(SelectStmt, value->selectStmt),
                {
                    .Inner = true,
                    .TargetColumns = targetColumns,
                    .AllowEmptyResSet = false,
                    .EmitPgStar = false,
                    .FillTargetColumns = true,
                    .UnknownsAllowed = true
                })
            : L(A("Void"));
        if (!select) {
            return nullptr;
        }

        const auto writeOptions = BuildWriteOptions(value, std::move(returningList));

        State_.Statements.push_back(L(
            A("let"),
            A("world"),
            L(
                A("Write!"),
                A("world"),
                sink,
                key,
                select,
                writeOptions
            )
        ));

        return State_.Statements.back();
    }

    [[nodiscard]]
    TAstNode* ParseUpdateStmt(const UpdateStmt* value) {
        const auto fromClause = value->fromClause ? value->fromClause : ListMake1(value->relation).get();
        SelectStmt selectStmt {
            .type = T_SelectStmt,
            .targetList = value->targetList,
            .fromClause = fromClause,
            .whereClause = value->whereClause,
            .withClause = value->withClause,
        };
        const auto select = ParseSelectStmt(
            &selectStmt,
            {
                .Inner = true,
                .AllowEmptyResSet = true,
                .EmitPgStar = true,
                .FillTargetColumns = false,
                .UnknownsAllowed = true
            }
        );
        if (!select) {
            return nullptr;
        }

        const auto [sink, key] = ParseWriteRangeVar(value->relation);
        if (!sink || !key) {
            return nullptr;
        }

        TVector<TAstNode*> returningList;
        if (value->returningList) {
            auto list = ParseReturningList(value->returningList);
            if (list.has_value()) {
                returningList = list.value();
            } else {
                return nullptr;
            }
        }

        TVector<TAstNode*> options;
        options.push_back(QL(QA("pg_update"), A("update_select")));
        options.push_back(QL(QA("mode"), QA("update")));
        if (!returningList.empty()) {
            options.push_back(QL(QA("returning"), QVL(returningList.data(), returningList.size())));
        }
        const auto writeUpdate = L(A("block"), QL(
            L(A("let"), A("update_select"), select),
            L(A("let"), A("sink"), sink),
            L(A("let"), A("key"), key),
            L(A("return"), L(
                A("Write!"),
                A("world"),
                A("sink"),
                A("key"),
                L(A("Void")),
                QVL(options.data(), options.size())))
            ));
        State_.Statements.push_back(L(
            A("let"),
            A("world"),
            writeUpdate
        ));

        return State_.Statements.back();
    }

    [[nodiscard]]
    TAstNode* ParseViewStmt(const ViewStmt* value) {
        if (ListLength(value->options) > 0) {
            AddError("Create view: not supported options");
            return nullptr;
        }

        TView view;
        if (StrLength(value->view->catalogname) > 0) {
            AddError("catalogname is not supported");
            return nullptr;
        }

        if (StrLength(value->view->schemaname) > 0) {
            AddError("schemaname is not supported");
            return nullptr;
        }

        if (StrLength(value->view->relname) == 0) {
            AddError("relname should be specified");
            return nullptr;
        }

        view.Name = value->view->relname;
        if (value->view->alias) {
            AddError("alias is not supported");
            return nullptr;
        }

        if (ListLength(value->aliases) == 0) {
            AddError("expected at least one target column");
            return nullptr;
        }

        for (int i = 0; i < ListLength(value->aliases); ++i) {
            auto node = ListNodeNth(value->aliases, i);
            if (NodeTag(node) != T_String) {
                NodeNotImplemented(value, node);
                return nullptr;
            }

            view.ColNames.push_back(StrVal(node));
        }

        if (value->withCheckOption != NO_CHECK_OPTION) {
            AddError("Create view: not supported options");
            return nullptr;
        }


        view.Source = ParseSelectStmt(CAST_NODE(SelectStmt, value->query), { .Inner = true });
        if (!view.Source) {
            return nullptr;
        }

        auto it = State_.Views.find(view.Name);
        if (it != State_.Views.end() && !value->replace) {
            AddError(TStringBuilder() << "View already exists: '" << view.Name << "'");
            return nullptr;
        }

        State_.Views[view.Name] = view;
        return State_.Statements.back();
    }

#pragma region CreateTable
private:

    struct TColumnInfo {
        TString Name;
        TString Type;
        bool Serial = false;
        bool NotNull = false;
        TAstNode* Default = nullptr;
    };

    struct TCreateTableCtx {
        std::unordered_map<TString, TColumnInfo> ColumnsSet;
        std::vector<TString> ColumnOrder;
        std::vector<TAstNode*> PrimaryKey;
        std::vector<std::vector<TAstNode*>> UniqConstr;
        bool IsTemporary;
        bool IfNotExists;
    };

    bool CheckConstraintSupported(const Constraint* pk) {
        bool isSupported = true;

        if (pk->deferrable) {
            AddError("DEFERRABLE constraints not supported");
            isSupported = false;
        }

        if (pk->initdeferred) {
            AddError("INITIALLY DEFERRED constraints not supported");
            isSupported = false;
        }

        if (0 < ListLength(pk->including)) {
            AddError("INCLUDING columns not supported");
            isSupported = false;
        }

        if (0 < ListLength(pk->options)) {
            AddError("WITH options not supported");
            isSupported = false;
        }

        if (pk->indexname) {
            AddError("INDEX name not supported");
            isSupported = false;
        }

        if (pk->indexspace) {
            AddError("USING INDEX TABLESPACE not supported");
            isSupported = false;
        }

        return isSupported;
    }

    bool FillPrimaryKeyColumns(TCreateTableCtx& ctx, const Constraint* pk) {
        if (!CheckConstraintSupported(pk))
            return false;

        for (int i = 0; i < ListLength(pk->keys); ++i) {
            auto node = ListNodeNth(pk->keys, i);
            auto nodeName = StrVal(node);

            auto it = ctx.ColumnsSet.find(nodeName);
            if (it == ctx.ColumnsSet.end()) {
                AddError("PK column does not belong to table");
                return false;
            }
            it->second.NotNull = true;
            ctx.PrimaryKey.push_back(QA(StrVal(node)));
        }

        Y_ENSURE(0 < ctx.PrimaryKey.size());

        return true;
    }

    bool FillUniqueConstraint(TCreateTableCtx& ctx, const Constraint* constr) {
        if (!CheckConstraintSupported(constr))
            return false;

        const auto length = ListLength(constr->keys);
        std::vector<TAstNode*> uniq;
        uniq.reserve(length);

        for (auto i = 0; i < length; ++i) {
            auto node = ListNodeNth(constr->keys, i);
            auto nodeName = StrVal(node);

            if (!ctx.ColumnsSet.contains(nodeName)) {
                AddError("UNIQUE column does not belong to table");
                return false;
            }
            uniq.push_back(QA(nodeName));
        }

        Y_ENSURE(0 < uniq.size());
        ctx.UniqConstr.emplace_back(std::move(uniq));

        return true;
    }

    const TString& FindColumnTypeAlias(const TString& colType, bool& isTypeSerial) {
        const static std::unordered_map<TString, TString> aliasMap {
            {"smallserial", "int2"},
            {"serial2", "int2"},
            {"serial", "int4"},
            {"serial4", "int4"},
            {"bigserial", "int8"},
            {"serial8", "int8"},
        };
        const auto aliasIt = aliasMap.find(to_lower(colType));
        if (aliasIt == aliasMap.end()) {
            isTypeSerial = false;
            return colType;
        }
        isTypeSerial = true;
        return aliasIt->second;
    }

    bool AddColumn(TCreateTableCtx& ctx, const ColumnDef* node) {
        TColumnInfo cinfo{.Name = node->colname};
        if (SystemColumns.contains(to_lower(cinfo.Name))) {
            AddError(TStringBuilder() << "system column can't be used: " << node->colname);
            return false;
        }

        if (node->constraints) {
            for (int i = 0; i < ListLength(node->constraints); ++i) {
                auto constraintNode =
                        CAST_NODE(Constraint, ListNodeNth(node->constraints, i));

                switch (constraintNode->contype) {
                    case CONSTR_NOTNULL:
                        cinfo.NotNull = true;
                        break;

                    case CONSTR_PRIMARY: {
                        if (!ctx.PrimaryKey.empty()) {
                            AddError("Only a single PK is allowed per table");
                            return false;
                        }
                        cinfo.NotNull = true;
                        ctx.PrimaryKey.push_back(QA(node->colname));
                    } break;

                    case CONSTR_UNIQUE: {
                        ctx.UniqConstr.push_back({QA(node->colname)});
                    } break;

                    case CONSTR_DEFAULT: {
                        TExprSettings settings;
                        settings.AllowColumns = false;
                        settings.Scope = "DEFAULT";
                        settings.AutoParametrizeEnabled = false;
                        cinfo.Default = ParseExpr(constraintNode->raw_expr, settings);
                        if (!cinfo.Default) {
                            return false;
                        }
                    } break;

                    default:
                        AddError("column constraint not supported");
                        return false;
                }
            }
        }

        // for now we pass just the last part of the type name
        auto colTypeVal = StrVal( ListNodeNth(node->typeName->names,
                                           ListLength(node->typeName->names) - 1));

        cinfo.Type = FindColumnTypeAlias(colTypeVal, cinfo.Serial);
        auto [it, inserted] = ctx.ColumnsSet.emplace(node->colname, cinfo);
        if (!inserted) {
            AddError("duplicated column names found");
            return false;
        }

        ctx.ColumnOrder.push_back(node->colname);
        return true;
    }

    bool AddConstraint(TCreateTableCtx& ctx, const Constraint* node) {
        switch (node->contype) {
            case CONSTR_PRIMARY: {
                if (!ctx.PrimaryKey.empty()) {
                    AddError("Only a single PK is allowed per table");
                    return false;
                }
                if (!FillPrimaryKeyColumns(ctx, node)) {
                    return false;
                }
            } break;

            case CONSTR_UNIQUE: {
                if (!FillUniqueConstraint(ctx, node)) {
                    return false;
                }
            } break;

            // TODO: support table-level not null constraints like:
            // CHECK (col1 is not null [OR col2 is not null])

            default:
                AddError("table constraint not supported");
                return false;
        }
        return true;
    }

    TAstNode* BuildColumnsOptions(TCreateTableCtx& ctx) {
        std::vector<TAstNode*> columns;

        for(const auto& name: ctx.ColumnOrder) {
            auto it = ctx.ColumnsSet.find(name);
            Y_ENSURE(it != ctx.ColumnsSet.end());

            const auto& cinfo = it->second;

            std::vector<TAstNode*> constraints;
            if (cinfo.Serial) {
                constraints.push_back(QL(QA("serial")));
            }

            if (cinfo.NotNull) {
                constraints.push_back(QL(QA("not_null")));
            }

            if (cinfo.Default) {
                constraints.push_back(QL(QA("default"), cinfo.Default));
            }

            columns.push_back(QL(QA(cinfo.Name), L(A("PgType"), QA(cinfo.Type)), QL(QA("columnConstraints"), QVL(constraints.data(), constraints.size()))));
        }

        return QVL(columns.data(), columns.size());
    }

    TAstNode* BuildCreateTableOptions(TCreateTableCtx& ctx) {
        std::vector<TAstNode*> options;

        TString mode = (ctx.IfNotExists) ? "create_if_not_exists" : "create";
        options.push_back(QL(QA("mode"), QA(mode)));
        options.push_back(QL(QA("columns"), BuildColumnsOptions(ctx)));
        if (!ctx.PrimaryKey.empty()) {
            options.push_back(QL(QA("primarykey"), QVL(ctx.PrimaryKey.data(), ctx.PrimaryKey.size())));
        }
        for (auto& uniq : ctx.UniqConstr) {
            auto columns = QVL(uniq.data(), uniq.size());
            options.push_back(QL(QA("index"), QL(
                                  QL(QA("indexName")),
                                  QL(QA("indexType"), QA("syncGlobalUnique")),
                                  QL(QA("dataColumns"), QL()),
                                  QL(QA("indexColumns"), columns))));
        }
        if (ctx.IsTemporary) {
            options.push_back(QL(QA("temporary")));
        }
        return QVL(options.data(), options.size());
    }

    TAstNode* BuildWriteOptions(const InsertStmt* value, TVector<TAstNode*> returningList = {}) {
        std::vector<TAstNode*> options;

        const auto insertMode = (ProviderToInsertModeMap.contains(Provider_))
            ? ProviderToInsertModeMap.at(Provider_)
            : "append";
        options.push_back(QL(QA("mode"), QA(insertMode)));

        if (!returningList.empty()) {
            options.push_back(QL(QA("returning"), QVL(returningList.data(), returningList.size())));
        }

        if (!value->selectStmt) {
            options.push_back(QL(QA("default_values")));
        }

        return QVL(options.data(), options.size());
    }

public:
    [[nodiscard]]
    TAstNode* ParseCreateStmt(const CreateStmt* value) {
        // See also transformCreateStmt() in parse_utilcmd.c
        if (0 < ListLength(value->inhRelations)) {
            AddError("table inheritance not supported");
            return nullptr;
        }

        if (value->partspec) {
            AddError("PARTITION BY clause not supported");
            return nullptr;
        }

        if (value->partbound) {
            AddError("FOR VALUES clause not supported");
            return nullptr;
        }

        // if we ever support typed tables, check transformOfType() in parse_utilcmd.c
        if (value->ofTypename) {
            AddError("typed tables not supported");
            return nullptr;
        }

        if (0 < ListLength(value->options)) {
            AddError("table options not supported");
            return nullptr;
        }

        if (value->oncommit != ONCOMMIT_NOOP && value->oncommit != ONCOMMIT_PRESERVE_ROWS) {
            AddError("ON COMMIT actions not supported");
            return nullptr;
        }

        if (value->tablespacename) {
            AddError("TABLESPACE not supported");
            return nullptr;
        }

        if (value->accessMethod) {
            AddError("USING not supported");
            return nullptr;
        }

        TCreateTableCtx ctx {};

        if (value->if_not_exists) {
            ctx.IfNotExists = true;
        }

        const auto relPersistence = static_cast<NPg::ERelPersistence>(value->relation->relpersistence);
        switch (relPersistence) {
            case NPg::ERelPersistence::Temp:
                ctx.IsTemporary = true;
                break;
            case NPg::ERelPersistence::Unlogged:
                AddError("UNLOGGED tables not supported");
                return nullptr;
                break;
            case NPg::ERelPersistence::Permanent:
                break;
        }

        auto [sink, key] = ParseWriteRangeVar(value->relation, true);

        if (!sink || !key) {
            return nullptr;
        }

        for (int i = 0; i < ListLength(value->tableElts); ++i) {
            auto rawNode = ListNodeNth(value->tableElts, i);

            switch (NodeTag(rawNode)) {
                case T_ColumnDef:
                    if (!AddColumn(ctx, CAST_NODE(ColumnDef, rawNode))) {
                        return nullptr;
                    }
                    break;

                case T_Constraint:
                    if (!AddConstraint(ctx, CAST_NODE(Constraint, rawNode))) {
                        return nullptr;
                    }
                    break;

                default:
                    NodeNotImplemented(value, rawNode);
                    return nullptr;
            }
        }

        State_.Statements.push_back(
                L(A("let"), A("world"),
                  L(A("Write!"), A("world"), sink, key, L(A("Void")),
                    BuildCreateTableOptions(ctx))));

        return State_.Statements.back();
    }
#pragma endregion CreateTable

    [[nodiscard]]
    TAstNode* ParseDropStmt(const DropStmt* value) {
        TVector<const List*> nameListNodes;
        for (int i = 0; i < ListLength(value->objects); ++i) {
            auto object = ListNodeNth(value->objects, i);
            if (NodeTag(object) != T_List) {
                NodeNotImplemented(value, object);
                return nullptr;
            }
            auto nameListNode = CAST_NODE(List, object);
            nameListNodes.push_back(nameListNode);
        }

        switch (value->removeType) {
            case OBJECT_VIEW: {
                return ParseDropViewStmt(value, nameListNodes);
            }
            case OBJECT_TABLE: {
                return ParseDropTableStmt(value, nameListNodes);
            }
            case OBJECT_INDEX: {
                return ParseDropIndexStmt(value, nameListNodes);
            }
            case OBJECT_SEQUENCE: {
                return ParseDropSequenceStmt(value, nameListNodes);
            }
            default: {
                AddError("Not supported object type for DROP");
                return nullptr;
            }
        }
    }

    TAstNode* ParseDropViewStmt(const DropStmt* value, const TVector<const List*>& names) {
        // behavior and concurrent don't matter here

        for (const auto& nameList : names) {
            if (ListLength(nameList) != 1) {
                AddError("Expected view name");
            }
            const auto nameNode = ListNodeNth(nameList, 0);

            if (NodeTag(nameNode) != T_String) {
                NodeNotImplemented(value, nameNode);
                return nullptr;
            }

            const auto name = StrVal(nameNode);
            auto it = State_.Views.find(name);
            if (!value->missing_ok && it == State_.Views.end()) {
                AddError(TStringBuilder() << "View not found: '" << name << "'");
                return nullptr;
            }

            if (it != State_.Views.end()) {
                State_.Views.erase(it);
            }
        }

        return State_.Statements.back();
    }

    TAstNode* ParseDropTableStmt(const DropStmt* value, const TVector<const List*>& names) {
        if (value->behavior == DROP_CASCADE) {
            AddError("CASCADE is not implemented");
            return nullptr;
        }

        for (const auto& nameList : names) {
            const auto [clusterName, tableName] = GetSchemaAndObjectName(nameList);
            const auto [sink, key] = ParseQualifiedRelationName(
                /* catalogName */ "",
                clusterName,
                tableName,
                /* isSink */ true,
                /* isScheme */ true
            );
            if (sink == nullptr) {
                return nullptr;
            }

            TString mode = (value->missing_ok) ? "drop_if_exists" : "drop";
            State_.Statements.push_back(L(
                A("let"),
                A("world"),
                L(
                    A("Write!"),
                    A("world"),
                    sink,
                    key,
                    L(A("Void")),
                    QL(
                        QL(QA("mode"), QA(mode))
                    )
                )
            ));
        }

        return State_.Statements.back();
    }

    TAstNode* ParseDropIndexStmt(const DropStmt* value, const TVector<const List*>& names) {
        if (value->behavior == DROP_CASCADE) {
            AddError("CASCADE is not implemented");
            return nullptr;
        }

        if (names.size() != 1) {
            AddError("DROP INDEX requires exactly one index");
            return nullptr;
        }

        for (const auto& nameList : names) {
            const auto [clusterName, indexName] = GetSchemaAndObjectName(nameList);
            const auto [sink, key] = ParseQualifiedPgObjectName(
                /* catalogName */ "",
                clusterName,
                indexName,
                "pgIndex"
            );

            TString missingOk = (value->missing_ok) ? "true" : "false";
            State_.Statements.push_back(L(
                A("let"),
                A("world"),
                L(
                    A("Write!"),
                    A("world"),
                    sink,
                    key,
                    L(A("Void")),
                    QL(
                        QL(QA("mode"), QA("dropIndex")),
                        QL(QA("ifExists"), QA(missingOk))
                    )
                )
            ));
        }

        return State_.Statements.back();
    }

    TAstNode* ParseDropSequenceStmt(const DropStmt* value, const TVector<const List*>& names) {
        if (value->behavior == DROP_CASCADE) {
            AddError("CASCADE is not implemented");
            return nullptr;
        }

        if (names.size() != 1) {
            AddError("DROP SEQUENCE requires exactly one sequence");
            return nullptr;
        }

        for (const auto& nameList : names) {
            const auto [clusterName, indexName] = GetSchemaAndObjectName(nameList);
            const auto [sink, key] = ParseQualifiedPgObjectName(
                /* catalogName */ "",
                clusterName,
                indexName,
                "pgSequence"
            );

            TString mode = (value->missing_ok) ? "drop_if_exists" : "drop";
            State_.Statements.push_back(L(
                A("let"),
                A("world"),
                L(
                    A("Write!"),
                    A("world"),
                    sink,
                    key,
                    L(A("Void")),
                    QL(
                        QL(QA("mode"), QA(mode))
                    )
                )
            ));
        }

        return State_.Statements.back();
    }

    [[nodiscard]]
    TAstNode* ParseVariableSetStmt(const VariableSetStmt* value, bool isSetConfig = false) {
        if (value->kind != VAR_SET_VALUE) {
            AddError(TStringBuilder() << "VariableSetStmt, not supported kind: " << (int)value->kind);
            return nullptr;
        }

        auto name = to_lower(TString(value->name));
        if (name == "search_path") {
            THashSet<TString> visitedValues;
            TVector<TString> values;
            for (int i = 0; i < ListLength(value->args); ++i) {
                auto val = ListNodeNth(value->args, i);
                if (NodeTag(val) != T_A_Const || CAST_NODE(A_Const, val)->isnull || NodeTag(CAST_NODE(A_Const, val)->val) != T_String) {
                    AddError(TStringBuilder() << "VariableSetStmt, expected string literal for " << value->name << " option");
                    return nullptr;
                }
                TString rawStr = to_lower(TString(StrVal(CAST_NODE(A_Const, val)->val)));
                if (visitedValues.emplace(rawStr).second) {
                    values.emplace_back(rawStr);
                }
            }

            if (values.size() != 1) {
                AddError(TStringBuilder() << "VariableSetStmt, expected 1 unique scheme, but got: " << values.size());
                return nullptr;
            }
            auto rawStr = values[0];
            if (rawStr != "pg_catalog" && rawStr != "public" && rawStr != "" && rawStr != "information_schema") {
                AddError(TStringBuilder() << "VariableSetStmt, search path supports only 'information_schema', 'public', 'pg_catalog', '' but got: '" << rawStr << "'");
                return nullptr;
            }
            if (Settings_.GUCSettings) {
                Settings_.GUCSettings->Set(name, rawStr, value->is_local);
                if (StmtParseInfo_) {
                    (*StmtParseInfo_)[StatementId_].KeepInCache = false;
                }
            }
            return State_.Statements.back();
        }

        if (isSetConfig) {
            if (name != "search_path") {
                AddError(TStringBuilder() << "VariableSetStmt, set_config doesn't support that option:" << name);
                return nullptr;
            }
        }

        if (name == "useblocks" || name == "emitaggapply" || name == "unorderedresult") {
            if (ListLength(value->args) != 1) {
                AddError(TStringBuilder() << "VariableSetStmt, expected 1 arg, but got: " << ListLength(value->args));
                return nullptr;
            }

            auto arg = ListNodeNth(value->args, 0);
            if (NodeTag(arg) == T_A_Const && (NodeTag(CAST_NODE(A_Const, arg)->val) == T_String)) {
                TString rawStr = StrVal(CAST_NODE(A_Const, arg)->val);
                if (name == "unorderedresult") {
                    UnorderedResult_ = (rawStr == "true");
                } else {
                    auto configSource = L(A("DataSource"), QA(TString(NYql::ConfigProviderName)));
                    State_.Statements.push_back(L(A("let"), A("world"), L(A(TString(NYql::ConfigureName)), A("world"), configSource,
                        QA(TString(rawStr == "true" ? "" : "Disable") + TString((name == "useblocks") ? "UseBlocks" : "PgEmitAggApply")))));
                }
            } else {
                AddError(TStringBuilder() << "VariableSetStmt, expected string literal for " << value->name << " option");
                return nullptr;
            }
        } else if (name == "dqengine" || name == "blockengine") {
            if (ListLength(value->args) != 1) {
                AddError(TStringBuilder() << "VariableSetStmt, expected 1 arg, but got: " << ListLength(value->args));
                return nullptr;
            }

            auto arg = ListNodeNth(value->args, 0);
            if (NodeTag(arg) == T_A_Const && (NodeTag(CAST_NODE(A_Const, arg)->val) == T_String)) {
                auto rawStr = StrVal(CAST_NODE(A_Const, arg)->val);
                auto str = to_lower(TString(rawStr));
                const bool isDqEngine = name == "dqengine";
                auto& enable = isDqEngine ? DqEngineEnabled_ : BlockEngineEnabled_;
                auto& force =  isDqEngine ? DqEngineForce_   : BlockEngineForce_;
                if (str == "auto") {
                    enable = true;
                    force = false;
                } else if (str == "force") {
                    enable = true;
                    force = true;
                } else if (str == "disable") {
                    enable = false;
                    force = false;
                } else {
                    AddError(TStringBuilder() << "VariableSetStmt, not supported " << value->name << " option value: " << rawStr);
                    return nullptr;
                }
            } else {
                AddError(TStringBuilder() << "VariableSetStmt, expected string literal for " << value->name << " option");
                return nullptr;
            }
        } else if (name.StartsWith("dq.") || name.StartsWith("yt.") || name.StartsWith("s3.") || name.StartsWith("ydb.")) {
            if (ListLength(value->args) != 1) {
                AddError(TStringBuilder() << "VariableSetStmt, expected 1 arg, but got: " << ListLength(value->args));
                return nullptr;
            }

            auto arg = ListNodeNth(value->args, 0);
            if (NodeTag(arg) == T_A_Const && (NodeTag(CAST_NODE(A_Const, arg)->val) == T_String)) {
                auto dotPos = name.find('.');
                auto provider = name.substr(0, dotPos);
                TString providerName;
                if (name.StartsWith("dq.")) {
                    providerName = NYql::DqProviderName;
                } else if (name.StartsWith("yt.")) {
                    providerName = NYql::YtProviderName;
                } else if (name.StartsWith("s3.")) {
                    providerName = NYql::S3ProviderName;
                } else if (name.StartsWith("ydb.")) {
                    providerName = NYql::YdbProviderName;
                } else {
                    Y_ASSERT(0);
                }

                auto providerSource = L(A("DataSource"), QA(providerName), QA("$all"));

                auto rawStr = StrVal(CAST_NODE(A_Const, arg)->val);

                State_.Statements.push_back(L(A("let"), A("world"), L(A(TString(NYql::ConfigureName)), A("world"), providerSource,
                    QA("Attr"), QAX(name.substr(dotPos + 1)), QAX(rawStr))));
            } else {
                AddError(TStringBuilder() << "VariableSetStmt, expected string literal for " << value->name << " option");
                return nullptr;
            }
        } else if (name == "tablepathprefix") {
            if (ListLength(value->args) != 1) {
                AddError(TStringBuilder() << "VariableSetStmt, expected 1 arg, but got: " << ListLength(value->args));
                return nullptr;
            }

            auto arg = ListNodeNth(value->args, 0);
            if (NodeTag(arg) == T_A_Const && (NodeTag(CAST_NODE(A_Const, arg)->val) == T_String)) {
                auto rawStr = StrVal(CAST_NODE(A_Const, arg)->val);
                TablePathPrefix_ = rawStr;
            } else {
                AddError(TStringBuilder() << "VariableSetStmt, expected string literal for " << value->name << " option");
                return nullptr;
            }
        } else if (name == "costbasedoptimizer") {
            if (ListLength(value->args) != 1) {
                AddError(TStringBuilder() << "VariableSetStmt, expected 1 arg, but got: " << ListLength(value->args));
                return nullptr;
            }

            auto arg = ListNodeNth(value->args, 0);
            if (NodeTag(arg) == T_A_Const && (NodeTag(CAST_NODE(A_Const, arg)->val) == T_String)) {
                auto rawStr = StrVal(CAST_NODE(A_Const, arg)->val);
                auto str = to_lower(TString(rawStr));
                if (!(str == "disable" || str == "pg" || str == "native")) {
                    AddError(TStringBuilder() << "VariableSetStmt, not supported CostBasedOptimizer option value: " << rawStr);
                    return nullptr;
                }

                State_.CostBasedOptimizer = str;
            } else {
                AddError(TStringBuilder() << "VariableSetStmt, expected string literal for " << value->name << " option");
                return nullptr;
            }
        } else if (name == "applicationname") {
            if (ListLength(value->args) != 1) {
                AddError(TStringBuilder() << "VariableSetStmt, expected 1 arg, but got: " << ListLength(value->args));
                return nullptr;
            }

            auto arg = ListNodeNth(value->args, 0);
            if (NodeTag(arg) == T_A_Const && (NodeTag(CAST_NODE(A_Const, arg)->val) == T_String)) {
                auto rawStr = StrVal(CAST_NODE(A_Const, arg)->val);
                State_.ApplicationName = rawStr;
            } else {
                AddError(TStringBuilder() << "VariableSetStmt, expected string literal for " << value->name << " option");
                return nullptr;
            }
        } else {
            AddError(TStringBuilder() << "VariableSetStmt, not supported name: " << value->name);
            return nullptr;
        }

        return State_.Statements.back();
    }

    [[nodiscard]]
    TAstNode* ParseDeleteStmt(const DeleteStmt* value) {
        if (value->usingClause) {
            AddError("using is not supported");
            return nullptr;
        }
        TVector <TAstNode*> returningList;
        if (value->returningList) {
            auto list = ParseReturningList(value->returningList);
            if (list.has_value()) {
                returningList = list.value();
            } else {
                return nullptr;
            }
        }
        if (value->withClause) {
            AddError("with is not supported");
            return nullptr;
        }

        if (!value->relation) {
            AddError("DeleteStmt: expected relation");
            return nullptr;
        }

        TVector<TAstNode*> fromList;
        auto p = ParseRangeVar(value->relation);
        if (!p) {
            return nullptr;
        }
        AddFrom(*p, fromList);

        TAstNode* whereFilter = nullptr;
        if (value->whereClause) {
            TExprSettings settings;
            settings.AllowColumns = true;
            settings.AllowSubLinks = true;
            settings.Scope = "WHERE";
            whereFilter = ParseExpr(value->whereClause, settings);
            if (!whereFilter) {
                return nullptr;
            }
        }

        TVector<TAstNode*> setItemOptions;

        setItemOptions.push_back(QL(QA("result"), QVL(CreatePgStarResultItem())));
        setItemOptions.push_back(QL(QA("from"), QVL(fromList.data(), fromList.size())));
        setItemOptions.push_back(QL(QA("join_ops"), QVL(QL(QL(QA("push"))))));

        NYql::TAstNode* lambda = nullptr;
        if (whereFilter) {
            lambda = L(A("lambda"), QL(), whereFilter);
            setItemOptions.push_back(QL(QA("where"), L(A("PgWhere"), L(A("Void")), lambda)));
        }

        auto setItemNode = L(A("PgSetItem"), QVL(setItemOptions.data(), setItemOptions.size()));

        TVector<TAstNode*> selectOptions;
        selectOptions.push_back(QL(QA("set_items"), QVL(setItemNode)));
        selectOptions.push_back(QL(QA("set_ops"), QVL(QA("push"))));

        auto select = L(A("PgSelect"), QVL(selectOptions.data(), selectOptions.size()));

        auto [sink, key] = ParseWriteRangeVar(value->relation);

        if (!sink || !key) {
            return nullptr;
        }

        std::vector<TAstNode*> options;
        options.push_back(QL(QA("pg_delete"), select));
        options.push_back(QL(QA("mode"), QA("delete")));
        if (!returningList.empty()) {
            options.push_back(QL(QA("returning"), QVL(returningList.data(), returningList.size())));
        }
        State_.Statements.push_back(L(
            A("let"),
            A("world"),
            L(
                A("Write!"),
                A("world"),
                sink,
                key,
                L(A("Void")),
                QVL(options.data(), options.size())
            )
        ));
        return State_.Statements.back();
    }

    TMaybe<TString> GetConfigVariable(const TString& varName) {
        if (varName == "server_version") {
            return GetPostgresServerVersionStr();
        }
        if (varName == "server_version_num") {
            return GetPostgresServerVersionNum();
        }
        if (varName == "standard_conforming_strings"){
            return "on";
        }
        if (varName == "search_path"){
            auto searchPath = Settings_.GUCSettings->Get("search_path");
            return searchPath ? *searchPath : "public";
        }
        if (varName == "default_transaction_read_only"){
            return "off"; // mediawiki
        }
        if (varName == "transaction_isolation"){
            return "serializable";
        }
        return {};
    }

    TMaybe<std::vector<TAstNode*>> ParseIndexElements(List* list) {
        const auto length = ListLength(list);
        std::vector<TAstNode*> columns;
        columns.reserve(length);

        for (auto i = 0; i < length; ++i) {
            auto node = ListNodeNth(list, i);
            auto indexElem = IndexElement(node);
            if (indexElem->expr || indexElem->indexcolname) {
                AddError("index expression is not supported yet");
                return {};
            }

            columns.push_back(QA(indexElem->name));
        }

        return columns;
    }


    [[nodiscard]]
    TAstNode* ParseVariableShowStmt(const VariableShowStmt* value) {
        const auto varName = to_lower(TString(value->name));

        const auto varValue = GetConfigVariable(varName);
        if (!varValue) {
            AddError("unrecognized configuration parameter \"" + varName + "\"");
            return nullptr;
        }

        const auto columnName = QAX(varName);
        const auto varValueNode =
            L(A("PgConst"), QAX(*varValue), L(A("PgType"), QA("text")));

        const auto lambda = L(A("lambda"), QL(), varValueNode);
        const auto res = QL(L(A("PgResultItem"), columnName, L(A("Void")), lambda));

        const auto setItem = L(A("PgSetItem"), QL(QL(QA("result"), res)));
        const auto setItems = QL(QA("set_items"), QL(setItem));
        const auto setOps = QL(QA("set_ops"), QVL(QA("push")));
        const auto selectOptions = QL(setItems, setOps);

        const auto output = L(A("PgSelect"), selectOptions);
        State_.Statements.push_back(L(A("let"), A("output"), output));
        State_.Statements.push_back(L(A("let"), A("result_sink"), L(A("DataSink"), QA(TString(NYql::ResultProviderName)))));

        const auto resOptions = BuildResultOptions(true);
        State_.Statements.push_back(L(A("let"), A("world"), L(A("Write!"),
            A("world"), A("result_sink"), L(A("Key")), A("output"), resOptions)));
        State_.Statements.push_back(L(A("let"), A("world"), L(A("Commit!"),
            A("world"), A("result_sink"))));
        return State_.Statements.back();
    }

    [[nodiscard]]
    bool ParseTransactionStmt(const TransactionStmt* value) {
        switch (value->kind) {
        case TRANS_STMT_BEGIN:
        case TRANS_STMT_START:
        case TRANS_STMT_SAVEPOINT:
        case TRANS_STMT_RELEASE:
        case TRANS_STMT_ROLLBACK_TO:
            return true;
        case TRANS_STMT_COMMIT:
            State_.Statements.push_back(L(A("let"), A("world"), L(A("CommitAll!"),
                A("world"))));
            if (Settings_.GUCSettings) {
                Settings_.GUCSettings->Commit();
            }
            return true;
        case TRANS_STMT_ROLLBACK:
            State_.Statements.push_back(L(A("let"), A("world"), L(A("CommitAll!"),
                A("world"), QL(QL(QA("mode"), QA("rollback"))))));
            if (Settings_.GUCSettings) {
                Settings_.GUCSettings->RollBack();
            }
            return true;
        default:
            AddError(TStringBuilder() << "TransactionStmt: kind is not supported: " << (int)value->kind);
            return false;
        }
    }

    [[nodiscard]]
    TAstNode* ParseIndexStmt(const IndexStmt* value) {
        if (value->unique) {
            AddError("unique index creation is not supported yet");
            return nullptr;
        }

        if (value->primary) {
            AddError("primary key creation is not supported yet");
            return nullptr;
        }

        if (value->isconstraint || value->deferrable || value->initdeferred) {
            AddError("constraint modification is not supported yet");
            return nullptr;
        }

        if (value->whereClause) {
            AddError("partial index is not supported yet");
            return nullptr;
        }

        if (value->options) {
            AddError("storage parameters for index is not supported yet");
            return nullptr;
        }

        auto columns = ParseIndexElements(value->indexParams);
        if (!columns)
            return nullptr;

        auto coverColumns = ParseIndexElements(value->indexIncludingParams);
        if (!coverColumns)
            return nullptr;

        const auto [sink, key] = ParseWriteRangeVar(value->relation, true);
        if (!sink || !key) {
            return nullptr;
        }

        std::vector<TAstNode*> flags;
        flags.emplace_back(QA("pg"));
        if (value->if_not_exists) {
            flags.emplace_back(QA("ifNotExists"));
        }

        std::vector<TAstNode*> desc;
        auto indexNameAtom = QA("indexName");
        if (value->idxname) {
            desc.emplace_back(QL(indexNameAtom, QA(value->idxname)));
        } else {
            desc.emplace_back(QL(indexNameAtom));
        }
        desc.emplace_back(QL(QA("indexType"), QA(value->unique ? "syncGlobalUnique" : "syncGlobal")));
        desc.emplace_back(QL(QA("indexColumns"), QVL(columns->data(), columns->size())));
        desc.emplace_back(QL(QA("dataColumns"), QVL(coverColumns->data(), coverColumns->size())));
        desc.emplace_back(QL(QA("flags"), QVL(flags.data(), flags.size())));

        State_.Statements.push_back(L(
            A("let"),
            A("world"),
            L(
                A("Write!"),
                A("world"),
                sink,
                key,
                L(A("Void")),
                QL(
                    QL(QA("mode"), QA("alter")),
                    QL(QA("actions"), QL(QL(QA("addIndex"), QVL(desc.data(), desc.size()))))
                )
            )
        ));

        return State_.Statements.back();
    }

    [[nodiscard]]
    TAstNode* ParseCreateSeqStmt(const CreateSeqStmt* value) {

        std::vector<TAstNode*> options;

        TString mode = (value->if_not_exists) ? "create_if_not_exists" : "create";
        options.push_back(QL(QA("mode"), QA(mode)));

        auto [sink, key] = ParseQualifiedPgObjectName(
            value->sequence->catalogname,
            value->sequence->schemaname,
            value->sequence->relname,
            "pgSequence"
        );

        if (!sink || !key) {
            return nullptr;
        }

        const auto relPersistence = static_cast<NPg::ERelPersistence>(value->sequence->relpersistence);
        switch (relPersistence) {
            case NPg::ERelPersistence::Temp:
                options.push_back(QL(QA("temporary")));
                break;
            case NPg::ERelPersistence::Unlogged:
                AddError("UNLOGGED sequence not supported");
                return nullptr;
                break;
            case NPg::ERelPersistence::Permanent:
                break;
        }

        for (int i = 0; i < ListLength(value->options); ++i) {
            auto rawNode = ListNodeNth(value->options, i);

            switch (NodeTag(rawNode)) {
                case T_DefElem: {
                    const auto* defElem = CAST_NODE(DefElem, rawNode);
                    TString nameElem = defElem->defname;
                    if (defElem->arg) {
                        switch (NodeTag(defElem->arg))
                        {
                            case T_Boolean:
                                options.emplace_back(QL(QAX(nameElem), QA(ToString(boolVal(defElem->arg)))));
                                break;
                            case T_Integer:
                                options.emplace_back(QL(QAX(nameElem), QA(ToString(intVal(defElem->arg)))));
                                break;
                            case T_Float:
                                options.emplace_back(QL(QAX(nameElem), QA(strVal(defElem->arg))));
                                break;
                            case T_TypeName: {
                                const auto* typeName = CAST_NODE_EXT(PG_TypeName, T_TypeName, defElem->arg);
                                if (ListLength(typeName->names) > 0) {
                                    options.emplace_back(QL(QAX(nameElem),
                                        QAX(StrVal(ListNodeNth(typeName->names, ListLength(typeName->names) - 1)))));
                                }
                                break;
                            }
                            default:
                                NodeNotImplemented(defElem->arg);
                                return nullptr;
                        }
                    }
                    break;
                }
                default:
                    NodeNotImplemented(rawNode);
                    return nullptr;
            }
        }

        if (value->for_identity) {
            options.push_back(QL(QA("for_identity")));
        }

        if (value->ownerId != InvalidOid) {
            options.push_back(QL(QA("owner_id"), QA(ToString(value->ownerId))));
        }

        State_.Statements.push_back(
                L(A("let"), A("world"),
                  L(A("Write!"), A("world"), sink, key, L(A("Void")),
                    QVL(options.data(), options.size()))));

        return State_.Statements.back();
    }

    [[nodiscard]]
    TAstNode* ParseAlterSeqStmt(const AlterSeqStmt* value) {

        std::vector<TAstNode*> options;
        TString mode = (value->missing_ok) ? "alter_if_exists" : "alter";

        options.push_back(QL(QA("mode"), QA(mode)));

        auto [sink, key] = ParseQualifiedPgObjectName(
            value->sequence->catalogname,
            value->sequence->schemaname,
            value->sequence->relname,
            "pgSequence"
        );

        if (!sink || !key) {
            return nullptr;
        }

        for (int i = 0; i < ListLength(value->options); ++i) {
            auto rawNode = ListNodeNth(value->options, i);
            switch (NodeTag(rawNode)) {
                case T_DefElem: {
                    const auto* defElem = CAST_NODE(DefElem, rawNode);
                    TString nameElem = defElem->defname;
                    if (defElem->arg) {
                        switch (NodeTag(defElem->arg))
                        {
                            case T_Boolean:
                                options.emplace_back(QL(QAX(nameElem), QA(ToString(boolVal(defElem->arg)))));
                                break;
                            case T_Integer:
                                options.emplace_back(QL(QAX(nameElem), QA(ToString(intVal(defElem->arg)))));
                                break;
                            case T_Float:
                                options.emplace_back(QL(QAX(nameElem), QA(strVal(defElem->arg))));
                                break;
                            case T_TypeName: {
                                const auto* typeName = CAST_NODE_EXT(PG_TypeName, T_TypeName, defElem->arg);
                                if (ListLength(typeName->names) > 0) {
                                    options.emplace_back(QL(QAX(nameElem),
                                        QAX(StrVal(ListNodeNth(typeName->names, ListLength(typeName->names) - 1)))));
                                }
                                break;
                            }
                            default:
                                NodeNotImplemented(defElem->arg);
                                return nullptr;
                        }
                    } else {
                        if (nameElem == "restart") {
                            options.emplace_back(QL(QAX(nameElem), QA(TString())));
                        }
                    }
                    break;
                }
                default:
                    NodeNotImplemented(rawNode);
                    return nullptr;
            }
        }

        if (value->for_identity) {
            options.push_back(QL(QA("for_identity")));
        }

        State_.Statements.push_back(
                L(A("let"), A("world"),
                  L(A("Write!"), A("world"), sink, key, L(A("Void")),
                    QVL(options.data(), options.size()))));

        return State_.Statements.back();
    }

    [[nodiscard]]
    TAstNode* ParseAlterTableStmt(const AlterTableStmt* value) {
        std::vector<TAstNode*> options;
        TString mode = (value->missing_ok) ? "alter_if_exists" : "alter";

        options.push_back(QL(QA("mode"), QA(mode)));

        const auto [sink, key] = ParseWriteRangeVar(value->relation, true);
        if (!sink || !key) {
            return nullptr;
        }

        std::vector<TAstNode*> alterColumns;
        for (int i = 0; i < ListLength(value->cmds); ++i) {
            auto rawNode = ListNodeNth(value->cmds, i);

            const auto* cmd = CAST_NODE(AlterTableCmd, rawNode);
            switch (cmd->subtype) {
                case AT_ColumnDefault: { /* ALTER COLUMN DEFAULT */
                    const auto* def = cmd->def;
                    const auto* colName = cmd->name;
                    if (def == nullptr) {
                        alterColumns.push_back(QL(QAX(colName), QL(QA("setDefault"), QL(QA("Null")))));
                        break;
                    }
                    switch (NodeTag(def)) {
                        case T_FuncCall: {
                            const auto* newDefault = CAST_NODE(FuncCall, def);
                            const auto* funcName = ListNodeNth(newDefault->funcname, 0);
                            if (NodeTag(funcName) != T_String) {
                                NodeNotImplemented(newDefault, funcName);
                                return nullptr;
                            }
                            auto strFuncName = StrVal(funcName);
                            if (strcmp(strFuncName, "nextval") != 0) {
                                NodeNotImplemented(newDefault, funcName);
                                return nullptr;
                            }
                            const auto* rawArg = ListNodeNth(newDefault->args, 0);
                            if (NodeTag(rawArg) != T_TypeCast && NodeTag(rawArg) != T_A_Const) {
                                AddError(TStringBuilder() << "Expected type cast node or a_const, but got something wrong: " << NodeTag(rawArg));
                                return nullptr;
                            }
                            const A_Const* localConst = nullptr;
                            if (NodeTag(rawArg) == T_TypeCast) {
                                auto localCast = CAST_NODE(TypeCast, rawArg)->arg;
                                if (NodeTag(localCast) != T_A_Const) {
                                    AddError(TStringBuilder() << "Expected a_const in cast, but got something wrong: " << NodeTag(localCast));
                                    return nullptr;
                                }
                                localConst = CAST_NODE(A_Const, localCast);
                            } else {
                                localConst = CAST_NODE(A_Const, rawArg);
                            }
                            if (NodeTag(localConst->val) != T_String) {
                                AddError(TStringBuilder() << "Expected string in const, but got something wrong: " << NodeTag(localConst->val));
                                return nullptr;
                            }
                            auto seqName = StrVal(localConst->val);
                            TVector<TString> seqNameList;
                            Split(seqName, ".", seqNameList);
                            if (seqNameList.size() != 2 && seqNameList.size() != 1) {
                                AddError(TStringBuilder() << "Expected list size is 1 or 2, but there are " << seqNameList.size());
                                return nullptr;
                            }
                            alterColumns.push_back(QL(QAX(colName), QL(QA("setDefault"), QL(QA("nextval"), QA(seqNameList.back())))));
                            break;
                        }
                        default:
                            NodeNotImplemented(def);
                            return nullptr;
                    }
                    break;
                }
                default:
                    NodeNotImplemented(rawNode);
                    return nullptr;
            }
        }

        std::vector<TAstNode*> actions { QL(QA("alterColumns"), QVL(alterColumns.data(), alterColumns.size())) };

        options.push_back(
            QL(QA("actions"),
               QVL(actions.data(), actions.size())
            )
        );

        State_.Statements.push_back(
                L(A("let"), A("world"),
                  L(A("Write!"), A("world"), sink, key, L(A("Void")),
                    QVL(options.data(), options.size()))));

        return State_.Statements.back();
    }

    TMaybe<TFromDesc> ParseFromClause(const Node* node) {
        switch (NodeTag(node)) {
        case T_RangeVar:
            return ParseRangeVar(CAST_NODE(RangeVar, node));
        case T_RangeSubselect:
            return ParseRangeSubselect(CAST_NODE(RangeSubselect, node));
        case T_RangeFunction:
            return ParseRangeFunction(CAST_NODE(RangeFunction, node));
        default:
            NodeNotImplementedImpl<SelectStmt>(node);
            return {};
        }
    }

    void AddFrom(const TFromDesc& p, TVector<TAstNode*>& fromList) {
        auto aliasNode = QAX(p.Alias);
        TVector<TAstNode*> colNamesNodes;
        for (const auto& c : p.ColNames) {
            colNamesNodes.push_back(QAX(c));
        }

        auto colNamesTuple = QVL(colNamesNodes.data(), colNamesNodes.size());
        if (p.InjectRead) {
            auto label = "read" + ToString(State_.ReadIndex);
            State_.Statements.push_back(L(A("let"), A(label), p.Source));
            State_.Statements.push_back(L(A("let"), A("world"), L(A("Left!"), A(label))));
            fromList.push_back(QL(L(A("Right!"), A(label)), aliasNode, colNamesTuple));
            ++State_.ReadIndex;
        } else {
            auto source = p.Source;
            if (!source) {
                source = L(A("PgSelf"));
            }

            fromList.push_back(QL(source, aliasNode, colNamesTuple));
        }
    }

    bool ParseAlias(const Alias* alias, TString& res, TVector<TString>& colnames) {
        for (int i = 0; i < ListLength(alias->colnames); ++i) {
            auto node = ListNodeNth(alias->colnames, i);
            if (NodeTag(node) != T_String) {
                NodeNotImplemented(alias, node);
                return false;
            }

            colnames.push_back(StrVal(node));
        }

        res = alias->aliasname;
        return true;
    }

    TString ResolveCluster(const TStringBuf schemaname, TString name) {
        if (NYql::NPg::GetStaticColumns().contains(NPg::TTableInfoKey{"pg_catalog", name})) {
            return "pg_catalog";
        }

        if (schemaname == "public") {
            return Settings_.DefaultCluster;;
        }
        if (schemaname == "" && Settings_.GUCSettings) {
            auto search_path = Settings_.GUCSettings->Get("search_path");
            if (!search_path || *search_path == "public" || search_path->empty()) {
                return Settings_.DefaultCluster;
            }
            return TString(*search_path);
        }
        return TString(schemaname);
    }

    TAstNode* BuildClusterSinkOrSourceExpression(
        bool isSink, const TStringBuf schemaname) {
      TString usedCluster(schemaname);
      auto p = Settings_.ClusterMapping.FindPtr(usedCluster);
      if (!p) {
        usedCluster = to_lower(usedCluster);
        p = Settings_.ClusterMapping.FindPtr(usedCluster);
      }

      if (!p) {
        AddError(TStringBuilder() << "Unknown cluster: " << schemaname);
        return nullptr;
      }

      return L(isSink ? A("DataSink") : A("DataSource"), QAX(*p), QAX(usedCluster));
    }

    TAstNode* BuildTableKeyExpression(const TStringBuf relname,
        const TStringBuf cluster, bool isScheme = false
    ) {
        auto lowerCluster = to_lower(TString(cluster));
        bool noPrefix = (lowerCluster == "pg_catalog" || lowerCluster == "information_schema");
        TString tableName = noPrefix ? to_lower(TString(relname)) : TablePathPrefix_ + relname;
        return L(A("Key"), QL(QA(isScheme ? "tablescheme" : "table"),
                            L(A("String"), QAX(std::move(tableName)))));
    }

    TReadWriteKeyExprs ParseQualifiedRelationName(const TStringBuf catalogname,
                                                  const TStringBuf schemaname,
                                                  const TStringBuf relname,
                                                  bool isSink, bool isScheme) {
      if (!catalogname.empty()) {
        AddError("catalogname is not supported");
        return {};
      }
      if (relname.empty()) {
        AddError("relname should be specified");
        return {};
      }

      const auto cluster = ResolveCluster(schemaname, TString(relname));
      const auto sinkOrSource = BuildClusterSinkOrSourceExpression(isSink, cluster);
      const auto key = BuildTableKeyExpression(relname, cluster, isScheme);
      return {sinkOrSource, key};
    }


    TAstNode* BuildPgObjectExpression(const TStringBuf objectName, const TStringBuf objectType) {
        bool noPrefix = (objectType == "pgIndex");
        TString name = noPrefix ? TString(objectName) : TablePathPrefix_ + TString(objectName);
        return L(A("Key"), QL(QA("pgObject"),
                              L(A("String"), QAX(std::move(name))),
                              L(A("String"), QA(objectType))
                              ));
    }

    TReadWriteKeyExprs ParseQualifiedPgObjectName(const TStringBuf catalogname,
                                               const TStringBuf schemaname,
                                               const TStringBuf objectName,
                                               const TStringBuf pgObjectType) {
        if (!catalogname.empty()) {
            AddError("catalogname is not supported");
            return {};
        }
        if (objectName.empty()) {
            AddError("objectName should be specified");
            return {};
        }

        const auto cluster = ResolveCluster(schemaname, TString(objectName));
        const auto sinkOrSource = BuildClusterSinkOrSourceExpression(true, cluster);
        const auto key = BuildPgObjectExpression(objectName, pgObjectType);
        return {sinkOrSource, key};
    }

    TReadWriteKeyExprs ParseWriteRangeVar(const RangeVar *value,
                                          bool isScheme = false) {
      if (value->alias) {
        AddError("alias is not supported");
        return {};
      }

      return ParseQualifiedRelationName(value->catalogname, value->schemaname,
                                        value->relname,
                                        /* isSink */ true, isScheme);
    }

    TMaybe<TFromDesc> ParseRangeVar(const RangeVar* value) {
        AT_LOCATION(value);

        const TView* view = nullptr;
        if (StrLength(value->schemaname) == 0) {
            for (auto rit = State_.CTE.rbegin(); rit != State_.CTE.rend(); ++rit) {
                auto cteIt = rit->find(value->relname);
                if (cteIt != rit->end()) {
                    view = &cteIt->second;
                    break;
                }
            }
            if (!view && State_.CurrentRecursiveView && State_.CurrentRecursiveView->Name == value->relname) {
                view = State_.CurrentRecursiveView;
            }

            if (!view) {
                auto viewIt = State_.Views.find(value->relname);
                if (viewIt != State_.Views.end()) {
                    view = &viewIt->second;
                }
            }
        }

        TString alias;
        TVector<TString> colnames;
        if (value->alias) {
            if (!ParseAlias(value->alias, alias, colnames)) {
                return {};
            }
        } else {
            alias = value->relname;
        }

        if (view) {
            return TFromDesc{view->Source, alias, colnames.empty() ? view->ColNames : colnames, false };
        }

        TString schemaname = value->schemaname;
        if (!StrCompare(value->schemaname, "bindings")) {
            bool isBinding = false;
            switch (Settings_.BindingsMode) {
            case NSQLTranslation::EBindingsMode::DISABLED:
                AddError("Please remove 'bindings.' from your query, the support for this syntax has ended");
                return {};
            case NSQLTranslation::EBindingsMode::ENABLED:
                isBinding = true;
                break;
            case NSQLTranslation::EBindingsMode::DROP_WITH_WARNING:
                AddWarning(TIssuesIds::YQL_DEPRECATED_BINDINGS, "Please remove 'bindings.' from your query, the support for this syntax will be dropped soon");
                [[fallthrough]];
            case NSQLTranslation::EBindingsMode::DROP:
                schemaname = Settings_.DefaultCluster;
                break;
            }

            if (isBinding) {
                auto s = BuildBindingSource(value);
                if (!s) {
                    return {};
                }
                return TFromDesc{ s, alias, colnames, true };
            }
        }


        const auto [source, key] = ParseQualifiedRelationName(
            value->catalogname, schemaname, value->relname,
            /* isSink */ false,
            /* isScheme */ false);
        if (source == nullptr || key == nullptr) {
            return {};
        }
        const auto readExpr = this->SqlProcArgsCount_ ?
        L(A("Cons!"),
            A("world"),
            L(
                A("PgTableContent"),
                QA("pg_catalog"),
                QAX(value->relname),
                L(A("Void")),
                QL()
            )
        ) :
        L(
            A("Read!"),
            A("world"),
            source,
            key,
            L(A("Void")),
            QL()
        );
        return TFromDesc {
            readExpr,
            alias,
            colnames,
            /* injectRead */ true,
        };
    }

    TAstNode* BuildBindingSource(const RangeVar* value) {
        if (StrLength(value->relname) == 0) {
            AddError("relname should be specified");
        }

        const TString binding = value->relname;
        NSQLTranslation::TBindingInfo bindingInfo;
        if (const auto& error = ExtractBindingInfo(Settings_, binding, bindingInfo)) {
            AddError(error);
            return nullptr;
        }
        TVector<TAstNode*> hints;
        if (bindingInfo.Schema) {
            auto schema = QA(bindingInfo.Schema);

            auto type = L(A("SqlTypeFromYson"), schema);
            auto columns = L(A("SqlColumnOrderFromYson"), schema);
            hints.emplace_back(QL(QA("userschema"), type, columns));
        }

        for (auto& [key, value] : bindingInfo.Attributes) {
            TVector<TAstNode*> hintValues;
            hintValues.push_back(QA(NormalizeName(key)));
            for (auto& v : value) {
                hintValues.push_back(QA(v));
            }
            hints.emplace_back(QVL(hintValues.data(), hintValues.size()));
        }

        auto source = L(A("DataSource"), QAX(bindingInfo.ClusterType), QAX(bindingInfo.Cluster));
        return L(
                  A("Read!"),
                  A("world"),
                  source,
                  L(
                    A("MrTableConcat"),
                    L(
                      A("Key"),
                      QL(
                        QA("table"),
                        L(
                          A("String"),
                          QAX(bindingInfo.Path)
                        )
                      )
                    )
                  ),
                  L(A("Void")),
                  QVL(hints.data(), hints.size())
                );
    }

    TMaybe<TFromDesc> ParseRangeFunction(const RangeFunction* value) {
        if (value->lateral) {
            AddError("RangeFunction: unsupported lateral");
            return {};
        }

        if (value->ordinality) {
            AddError("RangeFunction: unsupported ordinality");
            return {};
        }

        if (value->is_rowsfrom) {
            AddError("RangeFunction: unsupported is_rowsfrom");
            return {};
        }

        if (ListLength(value->coldeflist) > 0) {
            AddError("RangeFunction: unsupported coldeflist");
            return {};
        }

        if (ListLength(value->functions) != 1) {
            AddError("RangeFunction: only one function is supported");
            return {};
        }

        TString alias;
        TVector<TString> colnames;
        if (value->alias) {
            if (!ParseAlias(value->alias, alias, colnames)) {
                return {};
            }
        }

        auto funcNode = ListNodeNth(value->functions, 0);
        if (NodeTag(funcNode) != T_List) {
            AddError("RangeFunction: expected pair");
            return {};
        }

        auto lst = CAST_NODE(List, funcNode);
        if (ListLength(lst) != 2) {
            AddError("RangeFunction: expected pair");
            return {};
        }

        TExprSettings settings;
        settings.AllowColumns = false;
        settings.AllowReturnSet = true;
        settings.Scope = "RANGE FUNCTION";
        auto node = ListNodeNth(lst, 0);
        if (NodeTag(node) != T_FuncCall) {
            AddError("RangeFunction: extected FuncCall");
            return {};
        }

        bool injectRead = false;
        auto func = ParseFuncCall(CAST_NODE(FuncCall, node), settings, true, injectRead);
        if (!func) {
            return {};
        }

        return TFromDesc{ func, alias, colnames, injectRead };
    }

    TMaybe<TFromDesc> ParseRangeSubselect(const RangeSubselect* value) {
        if (value->lateral) {
            AddError("RangeSubselect: unsupported lateral");
            return {};
        }

        if (!value->alias) {
            AddError("RangeSubselect: expected alias");
            return {};
        }

        TString alias;
        TVector<TString> colnames;
        if (!ParseAlias(value->alias, alias, colnames)) {
            return {};
        }

        if (!value->subquery) {
            AddError("RangeSubselect: expected subquery");
            return {};
        }

        if (NodeTag(value->subquery) != T_SelectStmt) {
            NodeNotImplemented(value, value->subquery);
            return {};
        }

        return TFromDesc{ ParseSelectStmt(CAST_NODE(SelectStmt, value->subquery), { .Inner = true }), alias, colnames, false };
    }

    TAstNode* ParseNullTestExpr(const NullTest* value, const TExprSettings& settings) {
        AT_LOCATION(value);
        if (value->argisrow) {
            AddError("NullTest: unsupported argisrow");
            return nullptr;
        }
        auto arg = ParseExpr(Expr2Node(value->arg), settings);
        if (!arg) {
            return nullptr;
        }
        auto result = L(A("Exists"), arg);
        if (value->nulltesttype == IS_NULL) {
            result = L(A("Not"), result);
        }
        return L(A("ToPg"), result);
    }

    struct TCaseBranch {
        TAstNode* Pred;
        TAstNode* Value;
    };

    TCaseBranch ReduceCaseBranches(std::vector<TCaseBranch>::const_iterator begin, std::vector<TCaseBranch>::const_iterator end) {
        Y_ENSURE(begin < end);
        const size_t branchCount = end - begin;
        if (branchCount == 1) {
            return *begin;
        }

        auto mid = begin + branchCount / 2;
        auto left = ReduceCaseBranches(begin, mid);
        auto right = ReduceCaseBranches(mid, end);

        TVector<TAstNode*> preds;
        preds.reserve(branchCount + 1);
        preds.push_back(A("Or"));
        for (auto it = begin; it != end; ++it) {
            preds.push_back(it->Pred);
        }

        TCaseBranch result;
        result.Pred = VL(&preds[0], preds.size());
        result.Value = L(A("If"), left.Pred, left.Value, right.Value);
        return result;

    }

    TAstNode* ParseCaseExpr(const CaseExpr* value, const TExprSettings& settings) {
        AT_LOCATION(value);
        TAstNode* testExpr = nullptr;
        if (value->arg) {
            testExpr = ParseExpr(Expr2Node(value->arg), settings);
            if (!testExpr) {
                return nullptr;
            }
        }
        std::vector<TCaseBranch> branches;
        for (int i = 0; i < ListLength(value->args); ++i) {
            auto node = ListNodeNth(value->args, i);
            auto whenNode = CAST_NODE(CaseWhen, node);
            auto whenExpr = ParseExpr(Expr2Node(whenNode->expr), settings);
            if (!whenExpr) {
                return nullptr;
            }
            if (testExpr) {
                whenExpr = L(A("PgOp"), QA("="), testExpr, whenExpr);
            }

            whenExpr = L(A("Coalesce"),
                L(A("FromPg"), whenExpr),
                L(A("Bool"), QA("false"))
            );

            auto whenResult = ParseExpr(Expr2Node(whenNode->result), settings);
            if (!whenResult) {
                return nullptr;
            }
            branches.emplace_back(TCaseBranch{ .Pred = whenExpr,.Value = whenResult });
        }
        TAstNode* defaultResult = nullptr;
        if (value->defresult) {
            defaultResult = ParseExpr(Expr2Node(value->defresult), settings);
            if (!defaultResult) {
                return nullptr;
            }
        } else {
            defaultResult = L(A("Null"));
        }
        auto final = ReduceCaseBranches(branches.begin(), branches.end());
        return L(A("If"), final.Pred, final.Value, defaultResult);
    }

    TAstNode* ParseParamRefExpr(const ParamRef* value) {
        if (SqlProcArgsCount_ && (value->number < 1 || (ui32)value->number > *SqlProcArgsCount_)) {
            AddError(TStringBuilder() << "Unexpected parameter number: " << value->number);
            return nullptr;
        }

        const auto varName = PREPARED_PARAM_PREFIX + ToString(value->number);
        if (!State_.ParamNameToPgTypeName.contains(varName)) {
            State_.ParamNameToPgTypeName[varName] = DEFAULT_PARAM_TYPE;
        }
        return A(varName);
    }

    TAstNode* ParseReturnStmt(const ReturnStmt* value) {
        TExprSettings settings;
        settings.AllowColumns = false;
        settings.Scope = "RETURN";
        auto expr = ParseExpr(value->returnval, settings);
        if (!expr) {
            return nullptr;
        }

        State_.Statements.push_back(L(A("return"), expr));
        return State_.Statements.back();
    }

    TAstNode* ParseSQLValueFunction(const SQLValueFunction* value) {
        AT_LOCATION(value);
        switch (value->op) {
        case SVFOP_CURRENT_DATE:
            return L(A("PgCast"),
                L(A("PgCall"), QA("now"), QL()),
                L(A("PgType"), QA("date"))
            );
        case SVFOP_CURRENT_TIME:
            return L(A("PgCast"),
                L(A("PgCall"), QA("now"), QL()),
                L(A("PgType"), QA("timetz"))
            );
        case SVFOP_CURRENT_TIME_N:
            return L(A("PgCast"),
                L(A("PgCall"), QA("now"), QL()),
                L(A("PgType"), QA("timetz")),
                L(A("PgConst"), QA(ToString(value->typmod)), L(A("PgType"), QA("int4")))
            );
        case SVFOP_CURRENT_TIMESTAMP:
            return L(A("PgCall"), QA("now"), QL());
        case SVFOP_CURRENT_TIMESTAMP_N:
            return L(A("PgCast"),
                L(A("PgCall"), QA("now"), QL()),
                L(A("PgType"), QA("timestamptz")),
                L(A("PgConst"), QA(ToString(value->typmod)), L(A("PgType"), QA("int4")))
            );
        case SVFOP_CURRENT_USER:
        case SVFOP_CURRENT_ROLE:
        case SVFOP_USER: {
            auto user = Settings_.GUCSettings->Get("ydb_user");
            return L(A("PgConst"), user ? QAX(TString(*user))  : QA("postgres"), L(A("PgType"), QA("name")));
        }
        case SVFOP_CURRENT_CATALOG: {
            std::optional<TString> database;
            if (Settings_.GUCSettings) {
                database = Settings_.GUCSettings->Get("ydb_database");
            }

            return L(A("PgConst"), QA(database ? *database : "postgres"), L(A("PgType"), QA("name")));
        }
        case SVFOP_CURRENT_SCHEMA:
            return GetCurrentSchema();
        default:
            AddError(TStringBuilder() << "Usupported SQLValueFunction: " << (int)value->op);
            return nullptr;
        }
    }

    TAstNode* GetCurrentSchema() {
        std::optional<TString> searchPath;
        if (Settings_.GUCSettings) {
            searchPath = Settings_.GUCSettings->Get("search_path");
        }

        return L(A("PgConst"), QA(searchPath ? *searchPath : "public"), L(A("PgType"), QA("name")));
    }

    TAstNode* ParseBooleanTest(const BooleanTest* value, const TExprSettings& settings) {
        AT_LOCATION(value);

        auto arg = ParseExpr(Expr2Node(value->arg), settings);
        if (!arg) {
            return nullptr;
        }

        TString op;
        bool isNot = false;

        switch (value->booltesttype) {
            case IS_TRUE: {
                op = "PgIsTrue";
                break;
            }
            case IS_NOT_TRUE: {
                op = "PgIsTrue";
                isNot = true;
                break;
            }

            case IS_FALSE: {
                op = "PgIsFalse";
                break;
            }

            case IS_NOT_FALSE: {
                op = "PgIsFalse";
                isNot = true;
                break;
            }

            case IS_UNKNOWN: {
                op = "PgIsUnknown";
                break;
            }

            case IS_NOT_UNKNOWN: {
                op = "PgIsUnknown";
                isNot = true;
                break;
            }

            default: {
                TStringBuilder b;
                b << "Unsupported booltesttype " << static_cast<int>(value->booltesttype);
                AddError(b);
                return nullptr;
            }
        }
        auto result = L(A(op), arg);
        if (isNot) {
            result = L(A("PgNot"), result);
        }
        return result;
    }

    TAstNode* ParseExpr(const Node* node, const TExprSettings& settings) {
        switch (NodeTag(node)) {
        case T_A_Const: {
            return ParseAConst(CAST_NODE(A_Const, node), settings);
        }
        case T_A_Expr: {
            return ParseAExpr(CAST_NODE(A_Expr, node), settings);
        }
        case T_CaseExpr: {
            return ParseCaseExpr(CAST_NODE(CaseExpr, node), settings);
        }
        case T_ColumnRef: {
            return ParseColumnRef(CAST_NODE(ColumnRef, node), settings);
        }
        case T_TypeCast: {
            return ParseTypeCast(CAST_NODE(TypeCast, node), settings);
        }
        case T_BoolExpr: {
            return ParseBoolExpr(CAST_NODE(BoolExpr, node), settings);
        }
        case T_NullTest: {
            return ParseNullTestExpr(CAST_NODE(NullTest, node), settings);
        }
        case T_FuncCall: {
            bool injectRead;
            return ParseFuncCall(CAST_NODE(FuncCall, node), settings, false, injectRead);
        }
        case T_A_ArrayExpr: {
            return ParseAArrayExpr(CAST_NODE(A_ArrayExpr, node), settings);
        }
        case T_SubLink: {
            return ParseSubLinkExpr(CAST_NODE(SubLink, node), settings);
        }
        case T_CoalesceExpr: {
            return ParseCoalesceExpr(CAST_NODE(CoalesceExpr, node), settings);
        }
        case T_GroupingFunc: {
            return ParseGroupingFunc(CAST_NODE(GroupingFunc, node));
        }
        case T_ParamRef: {
            return ParseParamRefExpr(CAST_NODE(ParamRef, node));
        }
        case T_SQLValueFunction: {
            return ParseSQLValueFunction(CAST_NODE(SQLValueFunction, node));
        }
        case T_BooleanTest: {
            return ParseBooleanTest(CAST_NODE(BooleanTest, node), settings);
        }
        default:
            NodeNotImplemented(node);
            return nullptr;
        }
    }

    TAstNode* AutoParametrizeConst(TPgConst&& valueNType, TAstNode* pgType) {
        const auto& paramName = AddSimpleAutoParam(std::move(valueNType));
        State_.Statements.push_back(L(A("declare"), A(paramName), pgType));

        YQL_CLOG(INFO, Default) << "Autoparametrized " << paramName << " at " << State_.Positions.back();

        return A(paramName);
    }

    TAstNode* ParseAConst(const A_Const* value, const TExprSettings& settings) {
        AT_LOCATION(value);
        const auto& val = value->val;
        auto valueNType = GetValueNType(value);
        if (!valueNType) {
            return nullptr;
        }

        TAstNode* pgTypeNode = !value->isnull
            ? L(A("PgType"), QA(TPgConst::ToString(valueNType->Type)))
            : L(A("PgType"), QA("unknown"));

        if (Settings_.AutoParametrizeEnabled && settings.AutoParametrizeEnabled) {
            return AutoParametrizeConst(std::move(valueNType.GetRef()), pgTypeNode);
        }

        if (value->isnull) {
            return L(A("PgCast"), L(A("Null")), pgTypeNode);
        }

        switch (NodeTag(val)) {
            case T_Integer:
            case T_Float: {
                return L(A("PgConst"), QA(valueNType->Value.GetRef()), pgTypeNode);
            }
            case T_Boolean:
            case T_String:
            case T_BitString: {
                return L(A("PgConst"), QAX(valueNType->Value.GetRef()), pgTypeNode);
            }
            default: {
                NodeNotImplemented((const Node*)value);
                return nullptr;
            }
        }
    }

    TAstNode* ParseAArrayExpr(const A_ArrayExpr* value, const TExprSettings& settings) {
        AT_LOCATION(value);
        TVector<TAstNode*> args;
        args.push_back(A("PgArray"));
        for (int i = 0; i < ListLength(value->elements); ++i) {
            auto elem = ParseExpr(ListNodeNth(value->elements, i), settings);
            if (!elem) {
                return nullptr;
            }

            args.push_back(elem);
        }

        return VL(args.data(), args.size());
    }

    TAstNode* ParseCoalesceExpr(const CoalesceExpr* value, const TExprSettings& settings) {
        AT_LOCATION(value);
        TVector<TAstNode*> args;
        args.push_back(A("Coalesce"));
        for (int i = 0; i < ListLength(value->args); ++i) {
            auto elem = ParseExpr(ListNodeNth(value->args, i), settings);
            if (!elem) {
                return nullptr;
            }

            args.push_back(elem);
        }

        return VL(args.data(), args.size());
    }

    TAstNode* ParseGroupingFunc(const GroupingFunc* value) {
        AT_LOCATION(value);
        TVector<TAstNode*> args;
        args.push_back(A("PgGrouping"));
        TExprSettings settings;
        settings.Scope = "GROUPING";
        settings.AllowColumns = true;
        for (int i = 0; i < ListLength(value->args); ++i) {
            auto elem = ParseExpr(ListNodeNth(value->args, i), settings);
            if (!elem) {
                return nullptr;
            }

            args.push_back(elem);
        }

        return VL(args.data(), args.size());
    }

    TAstNode* ParseGroupingSet(const GroupingSet* value, const TExprSettings& settings) {
        AT_LOCATION(value);
        TString mode;
        switch (value->kind) {
        case GROUPING_SET_ROLLUP:
            mode = "rollup";
            break;
        case GROUPING_SET_CUBE:
            mode = "cube";
            break;
        case GROUPING_SET_SETS:
            mode = "sets";
            break;
        default:
            AddError(TStringBuilder() << "Unexpected grouping set kind: " << (int)value->kind);
            return nullptr;
        }

        auto innerSettings = settings;
        innerSettings.Scope = to_title(mode);

        TVector<TAstNode*> args;
        args.push_back(A("PgGroupingSet"));
        args.push_back(QA(mode));
        if (value->kind == GROUPING_SET_SETS) {
            // tuple for each set
            for (int i = 0; i < ListLength(value->content); ++i) {
                auto child = ListNodeNth(value->content, i);
                if (NodeTag(child) == T_GroupingSet) {
                    auto kind = CAST_NODE(GroupingSet, child)->kind;
                    if (kind != GROUPING_SET_EMPTY) {
                        AddError(TStringBuilder() << "Unexpected inner grouping set kind: " << (int)kind);
                        return nullptr;
                    }

                    args.push_back(QL());
                    continue;
                }

                if (NodeTag(child) == T_RowExpr) {
                    auto row = CAST_NODE(RowExpr, child);
                    TVector<TAstNode*> tupleItems;
                    for (int j = 0; j < ListLength(row->args); ++j) {
                        auto elem = ParseExpr(ListNodeNth(row->args, j), innerSettings);
                        if (!elem) {
                            return nullptr;
                        }

                        tupleItems.push_back(elem);
                    }

                    args.push_back(QVL(tupleItems.data(), tupleItems.size()));
                    continue;
                }

                auto elem = ParseExpr(ListNodeNth(value->content, i), innerSettings);
                if (!elem) {
                    return nullptr;
                }

                args.push_back(QL(elem));
            }
        } else {
            // one tuple
            TVector<TAstNode*> tupleItems;
            for (int i = 0; i < ListLength(value->content); ++i) {
                auto elem = ParseExpr(ListNodeNth(value->content, i), innerSettings);
                if (!elem) {
                    return nullptr;
                }

                tupleItems.push_back(elem);
            }

            args.push_back(QVL(tupleItems.data(), tupleItems.size()));
        }

        return VL(args.data(), args.size());
    }


    TAstNode* ParseSubLinkExpr(const SubLink* value, const TExprSettings& settings) {
        AT_LOCATION(value);
        if (!settings.AllowSubLinks) {
            AddError(TStringBuilder() << "SubLinks are not allowed in: " << settings.Scope);
            return nullptr;
        }

        TString linkType;
        TString operName;
        switch (value->subLinkType) {
        case EXISTS_SUBLINK:
            linkType = "exists";
            break;
        case ALL_SUBLINK:
            linkType = "all";
            operName = "=";
            break;
        case ANY_SUBLINK:
            linkType = "any";
            operName = "=";
            break;
        case EXPR_SUBLINK:
            linkType = "expr";
            break;
        case ARRAY_SUBLINK:
            linkType = "array";
            break;
        default:
            AddError(TStringBuilder() << "SublinkExpr: unsupported link type: " << (int)value->subLinkType);
            return nullptr;
        }

        if (ListLength(value->operName) > 1) {
            AddError("SubLink: unsuppoted opername");
            return nullptr;
        } else if (ListLength(value->operName) == 1) {
            auto nameNode = ListNodeNth(value->operName, 0);
            if (NodeTag(nameNode) != T_String) {
                NodeNotImplemented(value, nameNode);
                return nullptr;
            }

            operName = StrVal(nameNode);
        }

        TAstNode* rowTest;
        if (value->testexpr) {
            TExprSettings localSettings = settings;
            localSettings.Scope = "SUBLINK TEST";
            auto test = ParseExpr(value->testexpr, localSettings);
            if (!test) {
                return nullptr;
            }

            rowTest = L(A("lambda"), QL(A("value")), L(A("PgOp"), QAX(operName), test, A("value")));
        } else {
            rowTest = L(A("Void"));
        }

        auto select = ParseSelectStmt(CAST_NODE(SelectStmt, value->subselect), {.Inner = true});
        if (!select) {
            return nullptr;
        }

        return L(A("PgSubLink"), QA(linkType), L(A("Void")), L(A("Void")), rowTest, L(A("lambda"), QL(), select));
    }

    TAstNode* ParseTableRangeFunction(const TString& name, const TString& schema, List* args) {
        auto source = BuildClusterSinkOrSourceExpression(false, schema);
        if (!source) {
            return nullptr;
        }

        TVector<TString> argStrs;
        for (int i = 0; i < ListLength(args); ++i) {
            auto arg = ListNodeNth(args, i);
            if (NodeTag(arg) == T_A_Const && (NodeTag(CAST_NODE(A_Const, arg)->val) == T_String)) {
                TString rawStr = StrVal(CAST_NODE(A_Const, arg)->val);
                argStrs.push_back(rawStr);
            } else {
                AddError("Expected String argument for table function");
                return nullptr;
            }
        }

        if (argStrs.empty()) {
            AddError("Expected at least one argument for table function");
            return nullptr;
        }

        TAstNode* key;
        auto lowerName = to_lower(name);
        auto options = QL();
        if (lowerName == "concat") {
            TVector<TAstNode*> concatArgs;
            concatArgs.push_back(A("MrTableConcat"));
            for (const auto& s : argStrs) {
                concatArgs.push_back(L(A("Key"), QL(QA("table"),L(A("String"), QAX(s)))));
            }

            key = VL(concatArgs);
        } else if (lowerName == "concat_view") {
            if (argStrs.size() % 2 != 0) {
                AddError("Expected sequence of pairs of table and view for concat_view");
                return nullptr;
            }

            TVector<TAstNode*> concatArgs;
            concatArgs.push_back(A("MrTableConcat"));
            for (ui32 i = 0; i < argStrs.size(); i += 2) {
                concatArgs.push_back(L(A("Key"),
                    QL(QA("table"),L(A("String"), QAX(argStrs[i]))),
                    QL(QA("view"),L(A("String"), QAX(argStrs[i + 1])))));
            }

            key = VL(concatArgs);
        } else if (lowerName == "range") {
            if (argStrs.size() > 5) {
                AddError("Too many arguments");
                return nullptr;
            }

            options = QL(QL(QA("ignorenonexisting")));
            TAstNode* expr;
            if (argStrs.size() == 1) {
                expr = L(A("Bool"),QA("true"));
            } else if (argStrs.size() == 2) {
                expr = L(A(">="),A("item"),L(A("String"),QAX(argStrs[1])));
            } else {
                expr = L(A("And"),
                    L(A(">="),A("item"),L(A("String"),QAX(argStrs[1]))),
                    L(A("<="),A("item"),L(A("String"),QAX(argStrs[2])))
                );
            }

            auto lambda = L(A("lambda"), QL(A("item")), expr);
            auto range = L(A("MrTableRange"), QAX(argStrs[0]), lambda, QAX(argStrs.size() < 4 ? "" : argStrs[3]));
            if (argStrs.size() < 5) {
                key = L(A("Key"), QL(QA("table"),range));
            } else {
                key = L(A("Key"), QL(QA("table"),range), QL(QA("view"),L(A("String"), QAX(argStrs[4]))));
            }
        } else if (lowerName == "regexp" || lowerName == "like") {
            if (argStrs.size() < 2 || argStrs.size() > 4) {
                AddError("Expected from 2 to 4 arguments");
                return nullptr;
            }

            options = QL(QL(QA("ignorenonexisting")));
            TAstNode* expr;
            if (lowerName == "regexp") {
                expr = L(A("Apply"),L(A("Udf"),QA("Re2.Grep"),
                    QL(L(A("String"),QAX(argStrs[1])),L(A("Null")))),
                    A("item"));
            } else {
                expr = L(A("Apply"),L(A("Udf"),QA("Re2.Match"),
                    QL(L(A("Apply"),
                        L(A("Udf"), QA("Re2.PatternFromLike")),
                        L(A("String"),QAX(argStrs[1]))),L(A("Null")))),
                    A("item"));
            }

            auto lambda = L(A("lambda"), QL(A("item")), expr);
            auto range = L(A("MrTableRange"), QAX(argStrs[0]), lambda, QAX(argStrs.size() < 3 ? "" : argStrs[2]));
            if (argStrs.size() < 4) {
                key = L(A("Key"), QL(QA("table"),range));
            } else {
                key = L(A("Key"), QL(QA("table"),range), QL(QA("view"),L(A("String"), QAX(argStrs[3]))));
            }
        } else {
            AddError(TStringBuilder() << "Unknown table function: " << name);
            return nullptr;
        }

        return L(
            A("Read!"),
            A("world"),
            source,
            key,
            L(A("Void")),
            options
        );
    }

    TAstNode* ParseFuncCall(const FuncCall* value, const TExprSettings& settings, bool rangeFunction, bool& injectRead) {
        AT_LOCATION(value);
        if (ListLength(value->agg_order) > 0) {
            AddError("FuncCall: unsupported agg_order");
            return nullptr;
        }

        if (value->agg_filter) {
            AddError("FuncCall: unsupported agg_filter");
            return nullptr;
        }

        if (value->agg_within_group) {
            AddError("FuncCall: unsupported agg_within_group");
            return nullptr;
        }

        if (value->func_variadic) {
            AddError("FuncCall: unsupported func_variadic");
            return nullptr;
        }

        TAstNode* window = nullptr;
        if (value->over) {
            if (!settings.AllowOver) {
                AddError(TStringBuilder() << "Over is not allowed in: " << settings.Scope);
                return nullptr;
            }

            if (StrLength(value->over->name)) {
                window = QAX(value->over->name);
            } else {
                auto index = settings.WindowItems->size();
                auto def = ParseWindowDef(value->over);
                if (!def) {
                    return nullptr;
                }

                window = L(A("PgAnonWindow"), QA(ToString(index)));
                settings.WindowItems->push_back(def);
            }
        }

        TString name;
        TString schema;
        if (!ExtractFuncName(value, name, rangeFunction ? &schema : nullptr)) {
            return nullptr;
        }

        if (rangeFunction && !schema.empty() && schema != "pg_catalog") {
            injectRead = true;
            return ParseTableRangeFunction(name, schema, value->args);
        }

        if (name == "shobj_description" || name == "obj_description") {
            AddWarning(TIssuesIds::PG_COMPAT, name + " function forced to NULL");
            return L(A("Null"));
        }

        if (name == "current_schema") {
            return GetCurrentSchema();
        }

        // for zabbix https://github.com/ydb-platform/ydb/issues/2904
        if (name == "pg_try_advisory_lock" || name == "pg_try_advisory_lock_shared" || name == "pg_advisory_unlock" || name == "pg_try_advisory_xact_lock" || name == "pg_try_advisory_xact_lock_shared"){
            AddWarning(TIssuesIds::PG_COMPAT, name + " function forced to return OK without waiting and without really lock/unlock");
                return L(A("PgConst"), QA("true"), L(A("PgType"), QA("bool")));
        }

        if (name == "pg_advisory_lock" || name == "pg_advisory_lock_shared" || name == "pg_advisory_unlock_all" || name == "pg_advisory_xact_lock" || name == "pg_advisory_xact_lock_shared"){
            AddWarning(TIssuesIds::PG_COMPAT, name + " function forced to return OK without waiting and without really lock/unlock");
            return L(A("Null"));
        }

        const bool isAggregateFunc = NYql::NPg::HasAggregation(name, NYql::NPg::EAggKind::Normal);
        const bool hasReturnSet = NYql::NPg::HasReturnSetProc(name);

        if (isAggregateFunc && !settings.AllowAggregates) {
            AddError(TStringBuilder() << "Aggregate functions are not allowed in: " << settings.Scope);
            return nullptr;
        }

        if (hasReturnSet && !settings.AllowReturnSet) {
            AddError(TStringBuilder() << "Generator functions are not allowed in: " << settings.Scope);
            return nullptr;
        }

        TVector<TAstNode*> args;
        TString callable;
        if (window) {
            if (isAggregateFunc) {
                callable = "PgAggWindowCall";
            } else {
                callable = "PgWindowCall";
            }
        } else {
            if (isAggregateFunc) {
                callable = "PgAgg";
            } else {
                callable = "PgCall";
            }
        }

        args.push_back(A(callable));
        args.push_back(QAX(name));
        if (window) {
            args.push_back(window);
        }

        TVector<TAstNode*> callSettings;
        if (value->agg_distinct) {
            if (!isAggregateFunc) {
                AddError("FuncCall: agg_distinct must be set only for aggregate functions");
                return nullptr;
            }

            callSettings.push_back(QL(QA("distinct")));
        }

        if (rangeFunction) {
            callSettings.push_back(QL(QA("range")));
        }

        args.push_back(QVL(callSettings.data(), callSettings.size()));
        if (value->agg_star) {
            if (name != "count") {
                AddError("FuncCall: * is expected only in count function");
                return nullptr;
            }
        } else {
            if (name == "count" && ListLength(value->args) == 0) {
                AddError("FuncCall: count(*) must be used to call a parameterless aggregate function");
                return nullptr;
            }

            bool hasError = false;
            for (int i = 0; i < ListLength(value->args); ++i) {
                auto x = ListNodeNth(value->args, i);
                auto arg = ParseExpr(x, settings);
                if (!arg) {
                    hasError = true;
                    continue;
                }

                args.push_back(arg);
            }

            if (hasError) {
                return nullptr;
            }
        }

        return VL(args.data(), args.size());
    }

    bool ExtractFuncName(const FuncCall* value, TString& name, TString* schemaName) {
        TVector<TString> names;
        for (int i = 0; i < ListLength(value->funcname); ++i) {
            auto x = ListNodeNth(value->funcname, i);
            if (NodeTag(x) != T_String) {
                NodeNotImplemented(value, x);
                return false;
            }

            names.push_back(to_lower(TString(StrVal(x))));
        }

        if (names.empty()) {
            AddError("FuncCall: missing function name");
            return false;
        }

        if (names.size() > 2) {
            AddError(TStringBuilder() << "FuncCall: too many name components:: " << names.size());
            return false;
        }

        if (names.size() == 2) {
            if (!schemaName && names[0] != "pg_catalog") {
                AddError(TStringBuilder() << "FuncCall: expected pg_catalog, but got: " << names[0]);
                return false;
            }

            if (schemaName) {
                *schemaName = names[0];
            }
        }

        name = names.back();
        return true;
    }

    TAstNode* ParseTypeCast(const TypeCast* value, const TExprSettings& settings) {
        AT_LOCATION(value);
        if (!value->arg) {
            AddError("Expected arg");
            return nullptr;
        }

        if (!value->typeName) {
            AddError("Expected type_name");
            return nullptr;
        }

        auto arg = value->arg;
        auto typeName = value->typeName;
        auto supportedTypeName = typeName->typeOid == 0 &&
            !typeName->setof &&
            !typeName->pct_type &&
            (ListLength(typeName->names) == 2 &&
                NodeTag(ListNodeNth(typeName->names, 0)) == T_String &&
                !StrICompare(StrVal(ListNodeNth(typeName->names, 0)), "pg_catalog") || ListLength(typeName->names) == 1) &&
            NodeTag(ListNodeNth(typeName->names, ListLength(typeName->names) - 1)) == T_String;

        if (NodeTag(arg) == T_A_Const &&
            (NodeTag(CAST_NODE(A_Const, arg)->val) == T_String ||
            CAST_NODE(A_Const, arg)->isnull) &&
            supportedTypeName &&
            typeName->typemod == -1 &&
            ListLength(typeName->typmods) == 0 &&
            ListLength(typeName->arrayBounds) == 0) {
            TStringBuf targetType = StrVal(ListNodeNth(typeName->names, ListLength(typeName->names) - 1));
            if (NodeTag(CAST_NODE(A_Const, arg)->val) == T_String && targetType == "bool") {
                auto str = StrVal(CAST_NODE(A_Const, arg)->val);
                return L(A("PgConst"), QAX(str), L(A("PgType"), QA("bool")));
            }
        }

        if (supportedTypeName) {
            AT_LOCATION(typeName);
            TStringBuf targetType = StrVal(ListNodeNth(typeName->names, ListLength(typeName->names) - 1));
            auto input = ParseExpr(arg, settings);
            if (!input) {
                return nullptr;
            }

            auto finalType = TString(targetType);
            if (ListLength(typeName->arrayBounds) && !finalType.StartsWith('_')) {
                finalType = "_" + finalType;
            }

            if (!NPg::HasType(finalType)) {
                AddError(TStringBuilder() << "Unknown type: " << finalType);
                return nullptr;
            }

            if (ListLength(typeName->typmods) == 0 && typeName->typemod == -1) {
                return L(A("PgCast"), input, L(A("PgType"), QAX(finalType)));
            } else {
                const auto& typeDesc = NPg::LookupType(finalType);
                ui32 typeModInFuncId;
                if (typeDesc.ArrayTypeId == typeDesc.TypeId) {
                    const auto& typeDescElem = NPg::LookupType(typeDesc.ElementTypeId);
                    typeModInFuncId = typeDescElem.TypeModInFuncId;
                } else {
                    typeModInFuncId = typeDesc.TypeModInFuncId;
                }

                if (!typeModInFuncId) {
                    AddError(TStringBuilder() << "Type " << finalType << " doesn't support modifiers");
                    return nullptr;
                }

                const auto& procDesc = NPg::LookupProc(typeModInFuncId);

                TAstNode* typeMod;
                if (typeName->typemod != -1) {
                    typeMod = L(A("PgConst"), QA(ToString(typeName->typemod)), L(A("PgType"), QA("int4")));
                } else {
                    TVector<TAstNode*> args;
                    args.push_back(A("PgArray"));
                    for (int i = 0; i < ListLength(typeName->typmods); ++i) {
                        auto typeMod = ListNodeNth(typeName->typmods, i);
                        if (NodeTag(typeMod) != T_A_Const) {
                            AddError("Expected T_A_Const as typmod");
                            return nullptr;
                        }

                        auto aConst = CAST_NODE(A_Const, typeMod);
                        TString s;
                        if (!ValueAsString(aConst->val, aConst->isnull, s)) {
                            AddError("Unsupported format of typmod");
                            return nullptr;
                        }

                        args.push_back(L(A("PgConst"), QAX(s), L(A("PgType"), QA("cstring"))));
                    }

                    typeMod = L(A("PgCall"), QA(procDesc.Name), QL(), VL(args.data(), args.size()));
                }

                return L(A("PgCast"), input, L(A("PgType"), QAX(finalType)), typeMod);
            }
        }

        AddError("Unsupported form of type cast");
        return nullptr;
    }

    TAstNode* ParseAndOrExpr(const BoolExpr* value, const TExprSettings& settings, const TString& pgOpName) {
        auto length = ListLength(value->args);
        if (length < 2) {
            AddError(TStringBuilder() << "Expected >1 args for " << pgOpName << " but have " << length << " args");
            return nullptr;
        }

        auto lhs = ParseExpr(ListNodeNth(value->args, 0), settings);
        if (!lhs) {
            return nullptr;
        }

        for (auto i = 1; i < length; ++i) {
            auto rhs = ParseExpr(ListNodeNth(value->args, i), settings);
            if (!rhs) {
                return nullptr;
            }
            lhs = L(A(pgOpName), lhs, rhs);
        }

        return lhs;
    }

    TAstNode* ParseBoolExpr(const BoolExpr* value, const TExprSettings& settings) {
        AT_LOCATION(value);
        switch (value->boolop) {
        case AND_EXPR: {
            return ParseAndOrExpr(value, settings, "PgAnd");
        }
        case OR_EXPR: {
            return ParseAndOrExpr(value, settings, "PgOr");
        }
        case NOT_EXPR: {
            if (ListLength(value->args) != 1) {
                AddError("Expected 1 arg for NOT");
                return nullptr;
            }

            auto arg = ParseExpr(ListNodeNth(value->args, 0), settings);
            if (!arg) {
                return nullptr;
            }

            return L(A("PgNot"), arg);
        }
        default:
            AddError(TStringBuilder() << "BoolExprType unsupported value: " << (int)value->boolop);
            return nullptr;
        }
    }

    TAstNode* ParseWindowDef(const WindowDef* value) {
        AT_LOCATION(value);
        auto name = QAX(value->name);
        auto refName = QAX(value->refname);
        TVector<TAstNode*> sortItems;
        for (int i = 0; i < ListLength(value->orderClause); ++i) {
            auto node = ListNodeNth(value->orderClause, i);
            if (NodeTag(node) != T_SortBy) {
                NodeNotImplemented(value, node);
                return nullptr;
            }

            auto sort = ParseSortBy(CAST_NODE_EXT(PG_SortBy, T_SortBy, node), true, false);
            if (!sort) {
                return nullptr;
            }

            sortItems.push_back(sort);
        }

        auto sort = QVL(sortItems.data(), sortItems.size());
        TVector<TAstNode*> groupByItems;
        for (int i = 0; i < ListLength(value->partitionClause); ++i) {
            auto node = ListNodeNth(value->partitionClause, i);
            TExprSettings settings;
            settings.AllowColumns = true;
            settings.AllowAggregates = true;
            settings.Scope = "PARTITITON BY";
            auto expr = ParseExpr(node, settings);
            if (!expr) {
                return nullptr;
            }

            auto lambda = L(A("lambda"), QL(), expr);
            groupByItems.push_back(L(A("PgGroup"), L(A("Void")), lambda));
        }

        auto group = QVL(groupByItems.data(), groupByItems.size());
        TVector<TAstNode*> optionItems;
        if (value->frameOptions & FRAMEOPTION_NONDEFAULT) {
            TString exclude;
            if (value->frameOptions & FRAMEOPTION_EXCLUDE_CURRENT_ROW) {
                if (exclude) {
                    AddError("Wrong frame options");
                    return nullptr;
                }

                exclude = "c";
            }

            if (value->frameOptions & FRAMEOPTION_EXCLUDE_GROUP) {
                if (exclude) {
                    AddError("Wrong frame options");
                    return nullptr;
                }

                exclude = "cp";
            }

            if (value->frameOptions & FRAMEOPTION_EXCLUDE_TIES) {
                if (exclude) {
                    AddError("Wrong frame options");
                    return nullptr;
                }

                exclude = "p";
            }

            if (exclude) {
                optionItems.push_back(QL(QA("exclude"), QA(exclude)));
            }

            TString type;
            if (value->frameOptions & FRAMEOPTION_RANGE) {
                if (type) {
                    AddError("Wrong frame options");
                    return nullptr;
                }

                type = "range";
            }

            if (value->frameOptions & FRAMEOPTION_ROWS) {
                if (type) {
                    AddError("Wrong frame options");
                    return nullptr;
                }

                type = "rows";
            }

            if (value->frameOptions & FRAMEOPTION_GROUPS) {
                if (type) {
                    AddError("Wrong frame options");
                    return nullptr;
                }

                type = "groups";
            }

            if (!type) {
                AddError("Wrong frame options");
                return nullptr;
            }

            TString from;
            if (value->frameOptions & FRAMEOPTION_START_UNBOUNDED_PRECEDING) {
                if (from) {
                    AddError("Wrong frame options");
                    return nullptr;
                }

                from = "up";
            }

            if (value->frameOptions & FRAMEOPTION_START_OFFSET_PRECEDING) {
                if (from) {
                    AddError("Wrong frame options");
                    return nullptr;
                }

                from = "p";
                auto offset = ConvertFrameOffset(value->startOffset);
                if (!offset) {
                    return nullptr;
                }

                optionItems.push_back(QL(QA("from_value"), offset));
            }

            if (value->frameOptions & FRAMEOPTION_START_CURRENT_ROW) {
                if (from) {
                    AddError("Wrong frame options");
                    return nullptr;
                }

                from = "c";
            }

            if (value->frameOptions & FRAMEOPTION_START_OFFSET_FOLLOWING) {
                if (from) {
                    AddError("Wrong frame options");
                    return nullptr;
                }

                from = "f";
                auto offset = ConvertFrameOffset(value->startOffset);
                if (!offset) {
                    return nullptr;
                }

                optionItems.push_back(QL(QA("from_value"), offset));
            }

            if (value->frameOptions & FRAMEOPTION_START_UNBOUNDED_FOLLOWING) {
                AddError("Wrong frame options");
                return nullptr;
            }

            if (!from) {
                AddError("Wrong frame options");
                return nullptr;
            }

            TString to;
            if (value->frameOptions & FRAMEOPTION_END_UNBOUNDED_PRECEDING) {
                AddError("Wrong frame options");
                return nullptr;
            }

            if (value->frameOptions & FRAMEOPTION_END_OFFSET_PRECEDING) {
                if (to) {
                    AddError("Wrong frame options");
                    return nullptr;
                }

                to = "p";
                auto offset = ConvertFrameOffset(value->endOffset);
                if (!offset) {
                    return nullptr;
                }

                optionItems.push_back(QL(QA("to_value"), offset));
            }

            if (value->frameOptions & FRAMEOPTION_END_CURRENT_ROW) {
                if (to) {
                    AddError("Wrong frame options");
                    return nullptr;
                }

                to = "c";
            }

            if (value->frameOptions & FRAMEOPTION_END_OFFSET_FOLLOWING) {
                if (to) {
                    AddError("Wrong frame options");
                    return nullptr;
                }

                to = "f";
                auto offset = ConvertFrameOffset(value->endOffset);
                if (!offset) {
                    return nullptr;
                }

                optionItems.push_back(QL(QA("to_value"), offset));
            }

            if (value->frameOptions & FRAMEOPTION_END_UNBOUNDED_FOLLOWING) {
                if (to) {
                    AddError("Wrong frame options");
                    return nullptr;
                }

                to = "uf";
            }

            if (!to) {
                AddError("Wrong frame options");
                return nullptr;
            }

            optionItems.push_back(QL(QA("type"), QAX(type)));
            optionItems.push_back(QL(QA("from"), QAX(from)));
            optionItems.push_back(QL(QA("to"), QAX(to)));
        }

        auto options = QVL(optionItems.data(), optionItems.size());
        return L(A("PgWindow"), name, refName, group, sort, options);
    }

    TAstNode* ConvertFrameOffset(const Node* off) {
        if (NodeTag(off) == T_A_Const
            && NodeTag(CAST_NODE(A_Const, off)->val) == T_Integer) {
            return L(A("Int32"), QA(ToString(IntVal(CAST_NODE(A_Const, off)->val))));
        } else {
            TExprSettings settings;
            settings.AllowColumns = false;
            settings.Scope = "FRAME";
            auto offset = ParseExpr(off, settings);
            if (!offset) {
                return nullptr;
            }

            return L(A("EvaluateExpr"), L(A("Unwrap"), offset, L(A("String"), QA("Frame offset must be non-null"))));
        }
    }

    TAstNode* ParseSortBy(const PG_SortBy* value, bool allowAggregates, bool useProjectionRefs) {
        AT_LOCATION(value);
        bool asc = true;
        bool nullsFirst = true;
        switch (value->sortby_dir) {
        case SORTBY_DEFAULT:
        case SORTBY_ASC:
            if (Settings_.PgSortNulls) {
                nullsFirst = false;
            }
            break;
        case SORTBY_DESC:
            asc = false;
            break;
        default:
            AddError(TStringBuilder() << "sortby_dir unsupported value: " << (int)value->sortby_dir);
            return nullptr;
        }

        switch (value->sortby_nulls) {
        case SORTBY_NULLS_DEFAULT:
            break;
        case SORTBY_NULLS_FIRST:
            nullsFirst = true;
            break;
        case SORTBY_NULLS_LAST:
            nullsFirst = false;
            break;
        default:
            AddError(TStringBuilder() << "sortby_dir unsupported value: " << (int)value->sortby_dir);
            return nullptr;
        }

        if (ListLength(value->useOp) > 0) {
            AddError("Unsupported operators in sort_by");
            return nullptr;
        }

        TAstNode* expr;
        if (useProjectionRefs && NodeTag(value->node) == T_A_Const && (NodeTag(CAST_NODE(A_Const, value->node)->val) == T_Integer)) {
            expr = MakeProjectionRef("ORDER BY", CAST_NODE(A_Const, value->node));
        } else {
            TExprSettings settings;
            settings.AllowColumns = true;
            settings.AllowSubLinks = true;
            settings.Scope = "ORDER BY";
            settings.AllowAggregates = allowAggregates;
            expr = ParseExpr(value->node, settings);
        }

        if (!expr) {
            return nullptr;
        }

        auto lambda = L(A("lambda"), QL(), expr);
        return L(A("PgSort"), L(A("Void")), lambda, QA(asc ? "asc" : "desc"), QA(nullsFirst ? "first" : "last"));
    }

    TAstNode* ParseColumnRef(const ColumnRef* value, const TExprSettings& settings) {
        AT_LOCATION(value);
        if (!settings.AllowColumns) {
            AddError(TStringBuilder() << "Columns are not allowed in: " << settings.Scope);
            return nullptr;
        }

        if (ListLength(value->fields) == 0) {
            AddError("No fields");
            return nullptr;
        }

        if (ListLength(value->fields) > 2) {
            AddError("Too many fields");
            return nullptr;
        }

        bool isStar = false;
        TVector<TString> fields;
        for (int i = 0; i < ListLength(value->fields); ++i) {
            auto x = ListNodeNth(value->fields, i);
            if (isStar) {
                AddError("Star is already defined");
                return nullptr;
            }

            if (NodeTag(x) == T_String) {
                fields.push_back(StrVal(x));
            } else if (NodeTag(x) == T_A_Star) {
                isStar = true;
            } else {
                NodeNotImplemented(value, x);
                return nullptr;
            }
        }

        if (isStar) {
            if (fields.size() == 0) {
                return L(A("PgStar"));
            } else {
                return L(A("PgQualifiedStar"), QAX(fields[0]));
            }
        } else if (fields.size() == 1) {
            return L(A("PgColumnRef"), QAX(fields[0]));
        } else {
            return L(A("PgColumnRef"), QAX(fields[0]), QAX(fields[1]));
        }
    }

    TAstNode* ParseAExprOp(const A_Expr* value, const TExprSettings& settings) {
        AT_LOCATION(value);
        if (ListLength(value->name) != 1) {
            AddError(TStringBuilder() << "Unsupported count of names: " << ListLength(value->name));
            return nullptr;
        }

        auto nameNode = ListNodeNth(value->name, 0);
        if (NodeTag(nameNode) != T_String) {
            NodeNotImplemented(value, nameNode);
            return nullptr;
        }

        auto op = StrVal(nameNode);
        if (!value->rexpr) {
            AddError("Missing operands");
            return nullptr;
        }

        if (!value->lexpr) {
            auto rhs = ParseExpr(value->rexpr, settings);
            if (!rhs) {
                return nullptr;
            }

            return L(A("PgOp"), QAX(op), rhs);
        }

        auto lhs = ParseExpr(value->lexpr, settings);
        auto rhs = ParseExpr(value->rexpr, settings);
        if (!lhs || !rhs) {
            return nullptr;
        }

        return L(A("PgOp"), QAX(op), lhs, rhs);
    }

    TAstNode* ParseAExprOpAnyAll(const A_Expr* value, const TExprSettings& settings, bool all) {
        if (ListLength(value->name) != 1) {
            AddError(TStringBuilder() << "Unsupported count of names: " << ListLength(value->name));
            return nullptr;
        }

        auto nameNode = ListNodeNth(value->name, 0);
        if (NodeTag(nameNode) != T_String) {
            NodeNotImplemented(value, nameNode);
            return nullptr;
        }

        auto op = StrVal(nameNode);
        if (!value->lexpr || !value->rexpr) {
            AddError("Missing operands");
            return nullptr;
        }

        auto lhs = ParseExpr(value->lexpr, settings);
        if (NodeTag(value->rexpr) == T_SubLink) {
            auto sublink = CAST_NODE(SubLink, value->rexpr);
            auto subselect = CAST_NODE(SelectStmt, sublink->subselect);
            if (subselect->withClause && subselect->withClause->recursive) {
                if (State_.ApplicationName && State_.ApplicationName->StartsWith("pgAdmin")) {
                    AddWarning(TIssuesIds::PG_COMPAT, "AEXPR_OP_ANY forced to false");
                    return L(A("PgConst"), QA("false"), L(A("PgType"), QA("bool")));
                }
            }
        }

        auto rhs = ParseExpr(value->rexpr, settings);
        if (!lhs || !rhs) {
            return nullptr;
        }

        return L(A(all ? "PgAllOp" : "PgAnyOp"), QAX(op), lhs, rhs);
    }

    TAstNode* ParseAExprLike(const A_Expr* value, const TExprSettings& settings, bool insensitive) {
        if (ListLength(value->name) != 1) {
            AddError(TStringBuilder() << "Unsupported count of names: " << ListLength(value->name));
            return nullptr;
        }

        auto nameNode = ListNodeNth(value->name, 0);
        if (NodeTag(nameNode) != T_String) {
            NodeNotImplemented(value, nameNode);
            return nullptr;
        }

        auto op = TString(StrVal(nameNode));
        if (insensitive) {
            if (op != "~~*" && op != "!~~*") {
                AddError(TStringBuilder() << "Unsupported operation: " << op);
                return nullptr;
            }
        } else {
            if (op != "~~" && op != "!~~") {
                AddError(TStringBuilder() << "Unsupported operation: " << op);
                return nullptr;
            }
        }

        if (!value->lexpr || !value->rexpr) {
            AddError("Missing operands");
            return nullptr;
        }

        auto lhs = ParseExpr(value->lexpr, settings);
        auto rhs = ParseExpr(value->rexpr, settings);
        if (!lhs || !rhs) {
            return nullptr;
        }

        auto ret = L(A(insensitive ? "PgILike" : "PgLike"), lhs, rhs);
        if (op[0] == '!') {
            ret = L(A("PgNot"), ret);
        }

        return ret;
    }

    TAstNode* ParseAExprNullIf(const A_Expr* value, const TExprSettings& settings) {
        if (ListLength(value->name) != 1) {
            AddError(TStringBuilder() << "Unsupported count of names: " << ListLength(value->name));
            return nullptr;
        }
        if (!value->lexpr || !value->rexpr) {
            AddError("Missing operands");
            return nullptr;
        }

        auto lhs = ParseExpr(value->lexpr, settings);
        auto rhs = ParseExpr(value->rexpr, settings);
        if (!lhs || !rhs) {
            return nullptr;
        }
        return L(A("PgNullIf"), lhs, rhs);
    }

    TAstNode* ParseAExprIn(const A_Expr* value, const TExprSettings& settings) {
        if (ListLength(value->name) != 1) {
            AddError(TStringBuilder() << "Unsupported count of names: " << ListLength(value->name));
            return nullptr;
        }

        auto nameNode = ListNodeNth(value->name, 0);
        if (NodeTag(nameNode) != T_String) {
            NodeNotImplemented(value, nameNode);
            return nullptr;
        }

        auto op = TString(StrVal(nameNode));
        if (op != "=" && op != "<>") {
            AddError(TStringBuilder() << "Unsupported operation: " << op);
            return nullptr;
        }

        if (!value->lexpr || !value->rexpr) {
            AddError("Missing operands");
            return nullptr;
        }

        auto lhs = ParseExpr(value->lexpr, settings);
        if (!lhs) {
            return nullptr;
        }

        if (NodeTag(value->rexpr) != T_List) {
            NodeNotImplemented(value, value->rexpr);
            return nullptr;
        }

        auto lst = CAST_NODE(List, value->rexpr);

        TVector<TAstNode*> children;
        children.reserve(2 + ListLength(lst));

        children.push_back(A("PgIn"));
        children.push_back(lhs);
        for (int item = 0; item < ListLength(lst); ++item) {
            auto cell = ParseExpr(ListNodeNth(lst, item), settings);
            if (!cell) {
                return nullptr;
            }
            children.push_back(cell);
        }

        auto ret = VL(children.data(), children.size());
        if (op[0] == '<') {
            ret = L(A("PgNot"), ret);
        }

        return ret;
    }

    TAstNode* ParseAExprBetween(const A_Expr* value, const TExprSettings& settings) {
        if (!value->lexpr || !value->rexpr) {
            AddError("Missing operands");
            return nullptr;
        }

        if (NodeTag(value->rexpr) != T_List) {
            AddError(TStringBuilder() << "Expected T_List tag, but have " << NodeTag(value->rexpr));
            return nullptr;
        }

        const List* rexprList = CAST_NODE(List, value->rexpr);
        if (ListLength(rexprList) != 2) {
            AddError(TStringBuilder() << "Expected 2 args in BETWEEN range, but have " << ListLength(rexprList));
            return nullptr;
        }

        auto b = ListNodeNth(rexprList, 0);
        auto e = ListNodeNth(rexprList, 1);

        auto lhs = ParseExpr(value->lexpr, settings);
        auto rbhs = ParseExpr(b, settings);
        auto rehs = ParseExpr(e, settings);
        if (!lhs || !rbhs || !rehs) {
            return nullptr;
        }

        A_Expr_Kind kind = value->kind;
        bool inverse = false;
        if (kind == AEXPR_NOT_BETWEEN) {
            inverse = true;
            kind = AEXPR_BETWEEN;
        } else if (kind == AEXPR_NOT_BETWEEN_SYM) {
            inverse = true;
            kind = AEXPR_BETWEEN_SYM;
        }

        TAstNode* ret;
        switch (kind) {
        case AEXPR_BETWEEN:
        case AEXPR_BETWEEN_SYM:
            ret = L(A(kind == AEXPR_BETWEEN ? "PgBetween" : "PgBetweenSym"), lhs, rbhs, rehs);
            break;
        default:
            AddError(TStringBuilder() << "BETWEEN kind unsupported value: " << (int)value->kind);
            return nullptr;
        }

        if (inverse) {
            ret = L(A("PgNot"), ret);
        }

        return ret;
    }

    TAstNode* ParseAExpr(const A_Expr* value, const TExprSettings& settings) {
        AT_LOCATION(value);
        switch (value->kind) {
        case AEXPR_OP:
            return ParseAExprOp(value, settings);
        case AEXPR_LIKE:
        case AEXPR_ILIKE:
            return ParseAExprLike(value, settings, value->kind == AEXPR_ILIKE);
        case AEXPR_IN:
            return ParseAExprIn(value, settings);
        case AEXPR_BETWEEN:
        case AEXPR_NOT_BETWEEN:
        case AEXPR_BETWEEN_SYM:
        case AEXPR_NOT_BETWEEN_SYM:
            return ParseAExprBetween(value, settings);
        case AEXPR_OP_ANY:
        case AEXPR_OP_ALL:
            return ParseAExprOpAnyAll(value, settings, value->kind == AEXPR_OP_ALL);
        case AEXPR_NULLIF:
            return ParseAExprNullIf(value, settings);
        default:
            AddError(TStringBuilder() << "A_Expr_Kind unsupported value: " << (int)value->kind);
            return nullptr;
        }

    }

    void AddVariableDeclarations() {
      for (const auto& [varName, typeName] : State_.ParamNameToPgTypeName) {
        const auto pgType = L(A("PgType"), QA(typeName));
        State_.Statements.push_back(L(A("declare"), A(varName), pgType));
      }
    }

    template <typename T>
    void NodeNotImplementedImpl(const Node* nodeptr) {
        TStringBuilder b;
        b << TypeName<T>() << ": ";
        b << "alternative is not implemented yet : " << NodeTag(nodeptr);
        AddError(b);
    }

    template <typename T>
    void NodeNotImplemented(const T* outer, const Node* nodeptr) {
        Y_UNUSED(outer);
        NodeNotImplementedImpl<T>(nodeptr);
    }

    void NodeNotImplemented(const Node* nodeptr) {
        TStringBuilder b;
        b << "alternative is not implemented yet : " << NodeTag(nodeptr);
        AddError(b);
    }

    TAstNode* VL(TAstNode** nodes, ui32 size, TPosition pos = {}) {
        return TAstNode::NewList(pos.Row ? pos : State_.Positions.back(), nodes, size, *AstParseResults_[StatementId_].Pool);
    }

    TAstNode* VL(TArrayRef<TAstNode*> nodes, TPosition pos = {}) {
        return TAstNode::NewList(pos.Row ? pos : State_.Positions.back(), nodes.data(), nodes.size(), *AstParseResults_[StatementId_].Pool);
    }

    TAstNode* QVL(TAstNode** nodes, ui32 size, TPosition pos = {}) {
        return Q(VL(nodes, size, pos), pos);
    }

    TAstNode* QVL(TAstNode* node, TPosition pos = {}) {
        return QVL(&node, 1, pos);
    }

    TAstNode* QVL(TArrayRef<TAstNode*> nodes, TPosition pos = {}) {
        return Q(VL(nodes, pos), pos);
    }

    TAstNode* A(const TStringBuf str, TPosition pos = {}, ui32 flags = 0) {
        return TAstNode::NewAtom(pos.Row ? pos : State_.Positions.back(), str, *AstParseResults_[StatementId_].Pool, flags);
    }

    TAstNode* AX(const TString& str, TPosition pos = {}) {
        return A(str, pos.Row ? pos : State_.Positions.back(), TNodeFlags::ArbitraryContent);
    }

    TAstNode* Q(TAstNode* node, TPosition pos = {}) {
        return L(A("quote", pos), node, pos);
    }

    TAstNode* QA(const TStringBuf str, TPosition pos = {}, ui32 flags = 0) {
        return Q(A(str, pos, flags), pos);
    }

    TAstNode* QAX(const TString& str, TPosition pos = {}) {
        return QA(str, pos, TNodeFlags::ArbitraryContent);
    }

    template <typename... TNodes>
    TAstNode* L(TNodes... nodes) {
        TLState state;
        LImpl(state, nodes...);
        return TAstNode::NewList(state.Position.Row ? state.Position : State_.Positions.back(), state.Nodes.data(), state.Nodes.size(), *AstParseResults_[StatementId_].Pool);
    }

    template <typename... TNodes>
    TAstNode* QL(TNodes... nodes) {
        return Q(L(nodes...));
    }

    template <typename... TNodes>
    TAstNode* E(TAstNode* list, TNodes... nodes)  {
        Y_ABORT_UNLESS(list->IsList());
        TVector<TAstNode*> nodes_vec;
        nodes_vec.reserve(list->GetChildrenCount() + sizeof...(nodes));

        auto children = list->GetChildren();
        if (children) {
            nodes_vec.assign(children.begin(), children.end());
        }
        nodes_vec.assign({nodes...});
        return VL(nodes_vec.data(), nodes_vec.size());
    }

private:
    void AddError(const TString& value) {
        AstParseResults_[StatementId_].Issues.AddIssue(TIssue(State_.Positions.back(), value));
    }

    void AddWarning(int code, const TString& value) {
        AstParseResults_[StatementId_].Issues.AddIssue(TIssue(State_.Positions.back(), value).SetCode(code, ESeverity::TSeverityIds_ESeverityId_S_WARNING));
    }

    struct TLState {
        TPosition Position;
        TVector<TAstNode*> Nodes;
    };

    template <typename... TNodes>
    void LImpl(TLState& state, TNodes... nodes);

    void LImpl(TLState& state) {
        Y_UNUSED(state);
    }

    void LImpl(TLState& state, TPosition pos) {
        state.Position = pos;
    }

    void LImpl(TLState& state, TAstNode* node) {
        state.Nodes.push_back(node);
    }

    template <typename T, typename... TNodes>
    void LImpl(TLState& state, T node, TNodes... nodes) {
        state.Nodes.push_back(node);
        LImpl(state, nodes...);
    }

    void PushPosition(int location) {
        if (location == -1) {
            State_.Positions.push_back(State_.Positions.back());
            return;
        }

        State_.Positions.push_back(Location2Position(location));
    };

    void PopPosition() {
        State_.Positions.pop_back();
    }

    NYql::TPosition Location2Position(int location) const {
        if (!QuerySize_) {
            return NYql::TPosition(0, 0);
        }

        if (location < 0) {
            return NYql::TPosition(0, 0);
        }

        auto it = LowerBound(RowStarts_.begin(), RowStarts_.end(), Min((ui32)location, QuerySize_));
        Y_ENSURE(it != RowStarts_.end());

        if (*it == (ui32)location) {
            auto row = 1 + it - RowStarts_.begin();
            auto column = 1;
            return NYql::TPosition(column, row);
        } else {
            Y_ENSURE(it != RowStarts_.begin());
            auto row = it - RowStarts_.begin();
            auto column = 1 + location - *(it - 1);
            return NYql::TPosition(column, row);
        }
    }

    void ScanRows(const TString& query) {
        QuerySize_ = query.size();
        RowStarts_.push_back(0);
        TPosition position(0, 1);
        TTextWalker walker(position, true);
        auto prevRow = position.Row;
        for (ui32 i = 0; i < query.size(); ++i) {
            walker.Advance(query[i]);
            while (position.Row != prevRow) {
                RowStarts_.push_back(i);
                ++prevRow;
            }
        }

        RowStarts_.push_back(QuerySize_);
    }

    TAstNode* MakeProjectionRef(const TStringBuf& scope, const A_Const* aConst) {
        AT_LOCATION(aConst);
        auto num = IntVal(aConst->val);
        if (num <= 0) {
            AddError(TStringBuilder() << scope << ": position " << num << " is not in select list");
            return nullptr;
        }

        return L(A("PgProjectionRef"), QA(ToString(num - 1)));
    }

private:
    TVector<TAstParseResult>& AstParseResults_;
    NSQLTranslation::TTranslationSettings Settings_;
    bool DqEngineEnabled_ = false;
    bool DqEngineForce_ = false;
    bool BlockEngineEnabled_ = false;
    bool BlockEngineForce_ = false;
    bool UnorderedResult_ = false;
    TString TablePathPrefix_;
    TVector<ui32> RowStarts_;
    ui32 QuerySize_;
    TString Provider_;
    static const THashMap<TStringBuf, TString> ProviderToInsertModeMap;

    TState State_;
    ui32 StatementId_ = 0;
    TVector<TStmtParseInfo>* StmtParseInfo_;
    bool PerStatementResult_;
    TMaybe<ui32> SqlProcArgsCount_;
    bool HasSelectInLimitedView_ = false;
};

const THashMap<TStringBuf, TString> TConverter::ProviderToInsertModeMap = {
    {NYql::KikimrProviderName, "insert_abort"},
    {NYql::YtProviderName, "append"}
};

NYql::TAstParseResult PGToYql(const TString& query, const NSQLTranslation::TTranslationSettings& settings, TStmtParseInfo* stmtParseInfo) {
    TVector<NYql::TAstParseResult> results;
    TVector<TStmtParseInfo> stmtParseInfos;
    TConverter converter(results, settings, query, &stmtParseInfos, false, Nothing());
    NYql::PGParse(query, converter);
    if (stmtParseInfo) {
        Y_ENSURE(!stmtParseInfos.empty());
        *stmtParseInfo = stmtParseInfos.back();
    }
    Y_ENSURE(!results.empty());
    results.back().ActualSyntaxType = NYql::ESyntaxType::Pg;
    return std::move(results.back());
}

TVector<NYql::TAstParseResult> PGToYqlStatements(const TString& query, const NSQLTranslation::TTranslationSettings& settings, TVector<TStmtParseInfo>* stmtParseInfo) {
    TVector<NYql::TAstParseResult> results;
    TConverter converter(results, settings, query, stmtParseInfo, true, Nothing());
    NYql::PGParse(query, converter);
    for (auto& res : results) {
        res.ActualSyntaxType = NYql::ESyntaxType::Pg;
    }
    return results;
}

bool ParseTypeName(const PG_TypeName* typeName, TString& value, bool* setOf = nullptr) {
    auto len = ListLength(typeName->names);
    if (len < 1 || len > 2) {
        return false;
    }

    if (len == 2) {
        auto schemaStr = to_lower(TString(StrVal(ListNodeNth(typeName->names, 0))));
        if (schemaStr != "pg_catalog") {
            return false;
        }
    }

    value = to_lower(TString(StrVal(ListNodeNth(typeName->names, len - 1))));
    if (ListLength(typeName->arrayBounds) && !value.StartsWith('_')) {
        value = "_" + value;
    }

    if (!setOf && typeName->setof) {
        return false;
    }

    if (setOf) {
        *setOf = typeName->setof;
    }

    return true;
}

bool ParseCreateFunctionStmtImpl(const CreateFunctionStmt* value, ui32 extensionIndex,
    NPg::IExtensionSqlBuilder* builder, NYql::NPg::TProcDesc& desc) {
    if (ListLength(value->funcname) != 1) {
        return false;
    }

    auto nameNode = ListNodeNth(value->funcname, 0);
    auto name = to_lower(TString(StrVal(nameNode)));
    desc.ExtensionIndex = extensionIndex;
    desc.Name = name;
    desc.IsStrict = false;
    if (value->returnType) {
        TString resultTypeStr;
        if (!ParseTypeName(value->returnType, resultTypeStr, &desc.ReturnSet)) {
            return false;
        }

        if (builder) {
            builder->PrepareType(extensionIndex, resultTypeStr);
        }

        desc.ResultType = NPg::LookupType(resultTypeStr).TypeId;
    } else {
        desc.ResultType = NPg::LookupType("record").TypeId;
    }

    for (ui32 pass = 0; pass < 2; ++pass) {
        for (int i = 0; i < ListLength(value->options); ++i) {
            auto node = LIST_CAST_NTH(DefElem, value->options, i);
            TString defnameStr(node->defname);
            if (pass == 1 && defnameStr == "as") {
                auto asList = CAST_NODE(List, node->arg);
                auto asListLen = ListLength(asList);
                if (desc.Lang == NPg::LangC) {
                    if (asListLen < 1 || asListLen > 2) {
                        return false;
                    }

                    auto extStr = TString(StrVal(ListNodeNth(asList, 0)));
                    auto srcStr = asListLen > 1 ?
                        TString(StrVal(ListNodeNth(asList, 1))) :
                        name;

                    Y_ENSURE(extensionIndex == NPg::LookupExtensionByInstallName(extStr));
                    desc.Src = srcStr;
                } else if (desc.Lang == NPg::LangInternal || desc.Lang == NPg::LangSQL) {
                    if (asListLen != 1) {
                        return false;
                    }

                    auto srcStr = TString(StrVal(ListNodeNth(asList, 0)));
                    desc.Src = srcStr;
                }
            } else if (pass == 0 && defnameStr == "strict") {
                desc.IsStrict  = BoolVal(node->arg);
            } else if (pass == 0 && defnameStr == "language") {
                auto langStr = to_lower(TString(StrVal(node->arg)));
                if (langStr == "c") {
                    desc.Lang = NPg::LangC;
                } else if (extensionIndex == 0 && langStr == "internal") {
                    desc.Lang = NPg::LangInternal;
                } else if (langStr == "sql") {
                    desc.Lang = NPg::LangSQL;
                } else {
                    return false;
                }
            }
        }
    }

    bool hasArgNames = false;
    for (int i = 0; i < ListLength(value->parameters); ++i) {
        auto node = LIST_CAST_NTH(FunctionParameter, value->parameters, i);
        hasArgNames = hasArgNames || (node->name != nullptr);
        if (node->mode == FUNC_PARAM_IN || node->mode == FUNC_PARAM_DEFAULT) {
            if (node->defexpr) {
                desc.DefaultArgs.emplace_back();
                auto& value = desc.DefaultArgs.back();
                auto expr = node->defexpr;
                if (NodeTag(expr) == T_TypeCast) {
                    expr = CAST_NODE(TypeCast, expr)->arg;
                }

                if (NodeTag(expr) != T_A_Const) {
                    return false;
                }

                auto pgConst = GetValueNType(CAST_NODE(A_Const, expr));
                if (!pgConst) {
                    return false;
                }

                value = pgConst->Value;
            } else {
                Y_ENSURE(desc.DefaultArgs.empty());
            }

            desc.InputArgNames.push_back(node->name ? node->name : "");
        } else if (node->mode == FUNC_PARAM_OUT) {
            desc.OutputArgNames.push_back(node->name ? node->name : "");
        } else if (node->mode == FUNC_PARAM_VARIADIC) {
            desc.VariadicArgName = node->name ? node->name : "";
        } else {
            return false;
        }

        TString argTypeStr;
        if (!ParseTypeName(node->argType, argTypeStr)) {
            return false;
        }

        if (builder) {
            builder->PrepareType(extensionIndex, argTypeStr);
        }

        const auto& argTypeDesc = NPg::LookupType(argTypeStr);
        if (node->mode == FUNC_PARAM_IN || node->mode == FUNC_PARAM_DEFAULT) {
            desc.ArgTypes.push_back(argTypeDesc.TypeId);
        } else if (node->mode == FUNC_PARAM_VARIADIC) {
            desc.VariadicType = (argTypeDesc.ArrayTypeId == argTypeDesc.TypeId) ? argTypeDesc.ElementTypeId : argTypeDesc.TypeId;
            desc.VariadicArgType = argTypeDesc.TypeId;
        } else if (node->mode == FUNC_PARAM_OUT) {
            desc.OutputArgTypes.push_back(argTypeDesc.TypeId);
        }
    }

    if (!hasArgNames) {
        desc.InputArgNames.clear();
        desc.VariadicArgName.clear();
        desc.OutputArgNames.clear();
    }

    if (desc.Lang == NPg::LangSQL) {
        auto parser = NPg::GetSqlLanguageParser();
        if (!value->sql_body) {
            if (parser) {
                parser->Parse(desc.Src, desc);
            } else {
                return false;
            }
        } else {
            if (parser) {
                parser->ParseNode(value->sql_body, desc);
            } else {
                return false;
            }
        }

        if (!desc.ExprNode) {
            return false;
        }
    }

    return true;
}

class TExtensionHandler : public IPGParseEvents {
public:
    TExtensionHandler(ui32 extensionIndex, NYql::NPg::IExtensionSqlBuilder& builder)
        : ExtensionIndex_(extensionIndex)
        , Builder_(builder)
    {}

    void OnResult(const List* raw) final {
        for (int i = 0; i < ListLength(raw); ++i) {
            if (!ParseRawStmt(LIST_CAST_NTH(RawStmt, raw, i))) {
                continue;
            }
        }
    }

    void OnError(const TIssue& issue) final {
        throw yexception() << "Can't parse extension DDL: " << issue.ToString();
    }

    [[nodiscard]]
    bool ParseRawStmt(const RawStmt* value) {
        auto node = value->stmt;
        switch (NodeTag(node)) {
        case T_CreateFunctionStmt:
            return ParseCreateFunctionStmt(CAST_NODE(CreateFunctionStmt, node));
        case T_DefineStmt:
            return ParseDefineStmt(CAST_NODE(DefineStmt, node));
        case T_CreateStmt:
            return ParseCreateStmt(CAST_NODE(CreateStmt, node));
        case T_InsertStmt:
            return ParseInsertStmt(CAST_NODE(InsertStmt, node));
        case T_CreateCastStmt:
            return ParseCreateCastStmt(CAST_NODE(CreateCastStmt, node));
        case T_CreateOpClassStmt:
            return ParseCreateOpClassStmt(CAST_NODE(CreateOpClassStmt, node));
        default:
            return false;
        }
    }

    [[nodiscard]]
    bool ParseDefineStmt(const DefineStmt* value) {
        switch (value->kind) {
        case OBJECT_TYPE:
            return ParseDefineType(value);
        case OBJECT_OPERATOR:
            return ParseDefineOperator(value);
        case OBJECT_AGGREGATE:
            return ParseDefineAggregate(value);
        default:
            return false;
        }
    }

    [[nodiscard]]
    bool ParseDefineType(const DefineStmt* value) {
        if (ListLength(value->defnames) != 1) {
            return false;
        }

        auto nameNode = ListNodeNth(value->defnames, 0);
        auto name = to_lower(TString(StrVal(nameNode)));
        Builder_.PrepareType(ExtensionIndex_, name);

        NPg::TTypeDesc desc = NPg::LookupType(name);

        for (int i = 0; i < ListLength(value->definition); ++i) {
            auto node = LIST_CAST_NTH(DefElem, value->definition, i);
            auto defnameStr = to_lower(TString(node->defname));
            if (defnameStr == "internallength") {
                if (NodeTag(node->arg) == T_Integer) {
                    desc.TypeLen = IntVal(node->arg);
                } else if (NodeTag(node->arg) == T_TypeName) {
                    TString value;
                    if (!ParseTypeName(CAST_NODE_EXT(PG_TypeName, T_TypeName, node->arg), value)) {
                        return false;
                    }

                    if (value == "variable") {
                        desc.TypeLen = -1;
                    } else {
                        return false;
                    }
                } else {
                    return false;
                }
            } else if (defnameStr == "alignment") {
                if (NodeTag(node->arg) != T_TypeName) {
                    return false;
                }

                TString value;
                if (!ParseTypeName(CAST_NODE_EXT(PG_TypeName, T_TypeName, node->arg), value)) {
                    return false;
                }

                if (value == "double") {
                    desc.TypeAlign = 'd';
                } else if (value == "int") {
                    desc.TypeAlign = 'i';
                } else if (value == "short") {
                    desc.TypeAlign = 's';
                } else if (value == "char") {
                    desc.TypeAlign = 'c';
                } else {
                    throw yexception() << "Unsupported alignment: " << value;
                }
            } else if (defnameStr == "input") {
                if (NodeTag(node->arg) != T_TypeName) {
                    return false;
                }

                TString value;
                if (!ParseTypeName(CAST_NODE_EXT(PG_TypeName, T_TypeName, node->arg), value)) {
                    return false;
                }

                try {
                    desc.InFuncId = NPg::LookupProc(value, {NPg::LookupType("cstring").TypeId}).ProcId;
                } catch (const yexception&) {
                    desc.InFuncId = NPg::LookupProc(value, {
                        NPg::LookupType("cstring").TypeId,
                        NPg::LookupType("oid").TypeId,
                        NPg::LookupType("integer").TypeId
                    }).ProcId;
                }
            } else if (defnameStr == "output") {
                if (NodeTag(node->arg) != T_TypeName) {
                    return false;
                }

                TString value;
                if (!ParseTypeName(CAST_NODE_EXT(PG_TypeName, T_TypeName, node->arg), value)) {
                    return false;
                }

                desc.OutFuncId = NPg::LookupProc(value, {desc.TypeId}).ProcId;
            } else if (defnameStr == "send") {
                if (NodeTag(node->arg) != T_TypeName) {
                    return false;
                }

                TString value;
                if (!ParseTypeName(CAST_NODE_EXT(PG_TypeName, T_TypeName, node->arg), value)) {
                    return false;
                }

                desc.SendFuncId = NPg::LookupProc(value, {desc.TypeId}).ProcId;
            } else if (defnameStr == "receive") {
                if (NodeTag(node->arg) != T_TypeName) {
                    return false;
                }

                TString value;
                if (!ParseTypeName(CAST_NODE_EXT(PG_TypeName, T_TypeName, node->arg), value)) {
                    return false;
                }

                try {
                    desc.ReceiveFuncId = NPg::LookupProc(value, {NPg::LookupType("internal").TypeId}).ProcId;
                } catch (const yexception&) {
                    desc.ReceiveFuncId = NPg::LookupProc(value, {
                        NPg::LookupType("internal").TypeId,
                        NPg::LookupType("oid").TypeId,
                        NPg::LookupType("integer").TypeId
                    }).ProcId;
                }
            } else if (defnameStr == "delimiter") {
                if (NodeTag(node->arg) != T_String) {
                    return false;
                }

                TString value(StrVal(node->arg));
                Y_ENSURE(value.size() == 1);
                desc.TypeDelim = value[0];
            } else if (defnameStr == "typmod_in") {
                if (NodeTag(node->arg) != T_TypeName) {
                    return false;
                }

                TString value;
                if (!ParseTypeName(CAST_NODE_EXT(PG_TypeName, T_TypeName, node->arg), value)) {
                    return false;
                }

                desc.TypeModInFuncId = NPg::LookupProc(value, {NPg::LookupType("_cstring").TypeId}).ProcId;
            } else if (defnameStr == "typmod_out") {
                if (NodeTag(node->arg) != T_TypeName) {
                    return false;
                }

                TString value;
                if (!ParseTypeName(CAST_NODE_EXT(PG_TypeName, T_TypeName, node->arg), value)) {
                    return false;
                }

                desc.TypeModInFuncId = NPg::LookupProc(value, {NPg::LookupType("int4").TypeId}).ProcId;
            }
        }

        if (desc.TypeLen >= 0 && desc.TypeLen <= 8) {
            desc.PassByValue = true;
        }

        Builder_.UpdateType(desc);
        return true;
    }

    [[nodiscard]]
    bool ParseDefineOperator(const DefineStmt* value) {
        if (ListLength(value->defnames) != 1) {
            return false;
        }

        auto nameNode = ListNodeNth(value->defnames, 0);
        auto name = to_lower(TString(StrVal(nameNode)));
        TString procedureName;
        TString commutator;
        TString negator;
        ui32 leftType = 0;
        ui32 rightType = 0;
        for (int i = 0; i < ListLength(value->definition); ++i) {
            auto node = LIST_CAST_NTH(DefElem, value->definition, i);
            auto defnameStr = to_lower(TString(node->defname));
            if (defnameStr == "leftarg") {
                if (NodeTag(node->arg) != T_TypeName) {
                    return false;
                }

                TString value;
                if (!ParseTypeName(CAST_NODE_EXT(PG_TypeName, T_TypeName, node->arg), value)) {
                    return false;
                }

                leftType = NPg::LookupType(value).TypeId;
            } else if (defnameStr == "rightarg") {
                if (NodeTag(node->arg) != T_TypeName) {
                    return false;
                }

                TString value;
                if (!ParseTypeName(CAST_NODE_EXT(PG_TypeName, T_TypeName, node->arg), value)) {
                    return false;
                }

                rightType = NPg::LookupType(value).TypeId;
            } else if (defnameStr == "procedure") {
                if (NodeTag(node->arg) != T_TypeName) {
                    return false;
                }

                TString value;
                if (!ParseTypeName(CAST_NODE_EXT(PG_TypeName, T_TypeName, node->arg), value)) {
                    return false;
                }

                procedureName = value;
            } else if (defnameStr == "commutator") {
                if (NodeTag(node->arg) != T_String) {
                    return false;
                }

                commutator = StrVal(node->arg);
            } else if (defnameStr == "negator") {
                if (NodeTag(node->arg) != T_String) {
                    return false;
                }

                negator = StrVal(node->arg);
            }
        }

        if (!leftType) {
            return false;
        }

        if (procedureName.empty()) {
            return false;
        }

        TVector<ui32> args;
        args.push_back(leftType);
        if (rightType) {
            args.push_back(rightType);
        }

        Builder_.PrepareOper(ExtensionIndex_, name, args);
        auto desc = NPg::LookupOper(name, args);
        if (!commutator.empty()) {
            TVector<ui32> commArgs;
            commArgs.push_back(rightType);
            commArgs.push_back(leftType);
            Builder_.PrepareOper(ExtensionIndex_, commutator, commArgs);
            desc.ComId = NPg::LookupOper(commutator, commArgs).OperId;
        }

        if (!negator.empty()) {
            Builder_.PrepareOper(ExtensionIndex_, negator, args);
            desc.NegateId = NPg::LookupOper(negator, args).OperId;
        }

        const auto& procDesc = NPg::LookupProc(procedureName, args);
        desc.ProcId = procDesc.ProcId;
        desc.ResultType = procDesc.ResultType;
        Builder_.UpdateOper(desc);
        return true;
    }

    [[nodiscard]]
    bool ParseDefineAggregate(const DefineStmt* value) {
        if (ListLength(value->defnames) != 1) {
            return false;
        }

        auto nameNode = ListNodeNth(value->defnames, 0);
        auto name = to_lower(TString(StrVal(nameNode)));
        TString sfunc;
        ui32 stype = 0;
        TString combinefunc;
        TString finalfunc;
        TString serialfunc;
        TString deserialfunc;
        bool hypothetical = false;
        for (int i = 0; i < ListLength(value->definition); ++i) {
            auto node = LIST_CAST_NTH(DefElem, value->definition, i);
            auto defnameStr = to_lower(TString(node->defname));
            if (defnameStr == "sfunc") {
                if (NodeTag(node->arg) != T_TypeName) {
                    return false;
                }

                TString value;
                if (!ParseTypeName(CAST_NODE_EXT(PG_TypeName, T_TypeName, node->arg), value)) {
                    return false;
                }

                sfunc = value;
            } else if (defnameStr == "stype") {
                if (NodeTag(node->arg) != T_TypeName) {
                    return false;
                }

                TString value;
                if (!ParseTypeName(CAST_NODE_EXT(PG_TypeName, T_TypeName, node->arg), value)) {
                    return false;
                }

                stype = NPg::LookupType(value).TypeId;
            } else if (defnameStr == "combinefunc") {
                if (NodeTag(node->arg) != T_TypeName) {
                    return false;
                }

                TString value;
                if (!ParseTypeName(CAST_NODE_EXT(PG_TypeName, T_TypeName, node->arg), value)) {
                    return false;
                }

                combinefunc = value;
            } else if (defnameStr == "finalfunc") {
                if (NodeTag(node->arg) != T_TypeName) {
                    return false;
                }

                TString value;
                if (!ParseTypeName(CAST_NODE_EXT(PG_TypeName, T_TypeName, node->arg), value)) {
                    return false;
                }

                finalfunc = value;
            } else if (defnameStr == "serialfunc") {
                if (NodeTag(node->arg) != T_TypeName) {
                    return false;
                }

                TString value;
                if (!ParseTypeName(CAST_NODE_EXT(PG_TypeName, T_TypeName, node->arg), value)) {
                    return false;
                }

                serialfunc = value;
            } else if (defnameStr == "deserialfunc") {
                if (NodeTag(node->arg) != T_TypeName) {
                    return false;
                }

                TString value;
                if (!ParseTypeName(CAST_NODE_EXT(PG_TypeName, T_TypeName, node->arg), value)) {
                    return false;
                }

                deserialfunc = value;
            } else if (defnameStr == "hypothetical") {
                if (NodeTag(node->arg) != T_Boolean) {
                    return false;
                }

                if (BoolVal(node->arg)) {
                    hypothetical = true;
                }
            }
        }

        if (!sfunc || !stype) {
            return false;
        }

        NPg::TAggregateDesc desc;
        desc.Name = name;
        desc.ExtensionIndex = ExtensionIndex_;
        if (ListLength(value->args) != 2) {
            return false;
        }

        auto numDirectArgs = intVal(lsecond(value->args));
        if (numDirectArgs >= 0) {
            desc.NumDirectArgs = numDirectArgs;
            desc.Kind = NPg::EAggKind::OrderedSet;
            Y_ENSURE(!hypothetical);
        } else if (hypothetical) {
            desc.Kind = NPg::EAggKind::Hypothetical;
        }

        auto args = linitial_node(List, value->args);
        for (int i = 0; i < ListLength(args); ++i) {
            auto node = LIST_CAST_NTH(FunctionParameter, args, i);
            if (node->mode == FUNC_PARAM_IN || node->mode == FUNC_PARAM_DEFAULT) {
                if (node->defexpr) {
                    return false;
                }
            } else {
                return false;
            }

            TString argTypeStr;
            if (!ParseTypeName(node->argType, argTypeStr)) {
                return false;
            }

            Builder_.PrepareType(ExtensionIndex_, argTypeStr);
            auto argTypeId = NPg::LookupType(argTypeStr).TypeId;
            desc.ArgTypes.push_back(argTypeId);
        }

        desc.TransTypeId = stype;
        TVector<ui32> stateWithArgs;
        stateWithArgs.push_back(stype);
        stateWithArgs.insert(stateWithArgs.end(), desc.ArgTypes.begin(), desc.ArgTypes.end());
        desc.TransFuncId = NPg::LookupProc(sfunc, stateWithArgs).ProcId;
        if (!finalfunc.empty()) {
            desc.FinalFuncId = NPg::LookupProc(finalfunc, { stype }).ProcId;
        }

        if (!combinefunc.empty()) {
            desc.CombineFuncId = NPg::LookupProc(combinefunc, { stype, stype }).ProcId;
        }

        if (!serialfunc.empty()) {
            const auto& procDesc = NPg::LookupProc(serialfunc, { stype });
            Y_ENSURE(procDesc.ResultType == NPg::LookupType("bytea").TypeId);
            desc.SerializeFuncId = procDesc.ProcId;
        }

        if (!deserialfunc.empty()) {
            Y_ENSURE(!serialfunc.empty());
            const auto& procDesc = NPg::LookupProc(deserialfunc, { NPg::LookupType("bytea").TypeId, stype });
            Y_ENSURE(procDesc.ResultType == stype);
            desc.DeserializeFuncId = procDesc.ProcId;
        }

        Builder_.CreateAggregate(desc);
        return true;
    }

    [[nodiscard]]
    bool ParseCreateFunctionStmt(const CreateFunctionStmt* value) {
        NYql::NPg::TProcDesc desc;
        if (!ParseCreateFunctionStmtImpl(value, ExtensionIndex_, &Builder_, desc)) {
            return false;
        }

        Builder_.CreateProc(desc);
        return true;
    }

    [[nodiscard]]
    bool ParseCreateStmt(const CreateStmt* value) {
        NPg::TTableInfo table;
        table.Schema = "pg_catalog";
        table.Name = value->relation->relname;
        table.Kind = NPg::ERelKind::Relation;
        table.ExtensionIndex = ExtensionIndex_;
        TVector<NPg::TColumnInfo> columns;
        for (int i = 0; i < ListLength(value->tableElts); ++i) {
            auto node = ListNodeNth(value->tableElts, i);
            if (NodeTag(node) != T_ColumnDef) {
                continue;
            }

            auto columnDef = CAST_NODE(ColumnDef, node);
            NPg::TColumnInfo column;
            column.Schema = table.Schema;
            column.TableName = table.Name;
            column.Name = columnDef->colname;
            column.ExtensionIndex = ExtensionIndex_;
            Y_ENSURE(ParseTypeName(columnDef->typeName, column.UdtType));
            columns.push_back(column);
        }

        Builder_.CreateTable(table, columns);
        return true;
    }

    [[nodiscard]]
    bool ParseInsertStmt(const InsertStmt* value) {
        TString tableName = value->relation->relname;
        TVector<TString> colNames;
        for (int i = 0; i < ListLength(value->cols); ++i) {
            auto node = LIST_CAST_NTH(ResTarget, value->cols, i);
            colNames.push_back(node->name);
        }

        auto select = CAST_NODE(SelectStmt, value->selectStmt);
        int rows = ListLength(select->valuesLists);
        if (!rows) {
            return false;
        }

        int cols = ListLength(CAST_NODE(List, ListNodeNth(select->valuesLists, 0)));
        TVector<TMaybe<TString>> data;
        data.reserve(rows * cols);

        for (int rowIdx = 0; rowIdx < rows; ++rowIdx) {
            const auto rawRow = CAST_NODE(List, ListNodeNth(select->valuesLists, rowIdx));

            for (int colIdx = 0; colIdx < ListLength(rawRow); ++colIdx) {
                const auto rawCell = ListNodeNth(rawRow, colIdx);
                if (NodeTag(rawCell) != T_A_Const) {
                    return false;
                }
                auto pgConst = GetValueNType(CAST_NODE(A_Const, rawCell));
                if (!pgConst) {
                    return false;
                }
                data.push_back(pgConst->Value);
            }
        }

        Builder_.InsertValues(NPg::TTableInfoKey{"pg_catalog", tableName}, colNames, data);
        return true;
    }

    [[nodiscard]]
    bool ParseCreateCastStmt(const CreateCastStmt* value) {
        TString sourceType;
        if (!ParseTypeName(value->sourcetype, sourceType)) {
            return false;
        }

        TString targetType;
        if (!ParseTypeName(value->targettype, targetType)) {
            return false;
        }

        NPg::TCastDesc desc;
        desc.ExtensionIndex = ExtensionIndex_;
        desc.SourceId = NPg::LookupType(sourceType).TypeId;
        desc.TargetId = NPg::LookupType(targetType).TypeId;
        if (value->func) {
            if (ListLength(value->func->objname) != 1) {
                return false;
            }

            TString funcName = StrVal(ListNodeNth(value->func->objname, 0));
            TVector<ui32> argTypes;
            for (int i = 0; i < ListLength(value->func->objargs); ++i) {
                auto node = ListNodeNth(value->func->objargs, i);
                if (NodeTag(node) != T_TypeName) {
                    return false;
                }

                TString value;
                if (!ParseTypeName(CAST_NODE_EXT(PG_TypeName, T_TypeName, node), value)) {
                    return false;
                }

                argTypes.push_back(NPg::LookupType(value).TypeId);
            }

            desc.FunctionId = NPg::LookupProc(funcName, argTypes).ProcId;
        } else if (value->inout) {
            desc.Method = NPg::ECastMethod::InOut;
        } else {
            desc.Method = NPg::ECastMethod::Binary;
        }

        switch (value->context) {
        case COERCION_IMPLICIT:
            desc.CoercionCode = NPg::ECoercionCode::Implicit;
            break;
        case COERCION_ASSIGNMENT:
            desc.CoercionCode = NPg::ECoercionCode::Assignment;
            break;
        case COERCION_EXPLICIT:
            desc.CoercionCode = NPg::ECoercionCode::Explicit;
            break;
        default:
            return false;
        }

        Builder_.CreateCast(desc);
        return true;
    }

    [[nodiscard]]
    bool ParseCreateOpClassStmt(const CreateOpClassStmt* value) {
        if (!value->isDefault) {
            return false;
        }

        if (ListLength(value->opclassname) != 1) {
            return false;
        }

        auto opClassName = to_lower(TString(StrVal(ListNodeNth(value->opclassname, 0))));
        if (ListLength(value->opfamilyname) > 1) {
            return false;
        }

        TString familyName;
        if (ListLength(value->opfamilyname) == 1) {
            familyName = to_lower(TString(StrVal(ListNodeNth(value->opfamilyname, 0))));
        }

        auto amName = to_lower(TString(value->amname));
        NPg::EOpClassMethod method;
        if (amName == "btree") {
            method = NPg::EOpClassMethod::Btree;
        } else if (amName == "hash") {
            method = NPg::EOpClassMethod::Hash;
        } else {
            return false;
        }

        TString dataType;
        if (!ParseTypeName(value->datatype, dataType)) {
            return false;
        }

        auto typeId = NPg::LookupType(dataType).TypeId;
        NPg::TOpClassDesc desc;
        desc.ExtensionIndex = ExtensionIndex_;
        desc.Method = method;
        desc.TypeId = typeId;
        desc.Name = opClassName;
        if (familyName.empty()) {
            familyName = amName + "/" + opClassName;
        }

        desc.Family = familyName;
        TVector<NPg::TAmOpDesc> ops;
        TVector<NPg::TAmProcDesc> procs;

        for (int i = 0; i < ListLength(value->items); ++i) {
            auto node = LIST_CAST_NTH(CreateOpClassItem, value->items, i);
            if (node->itemtype != OPCLASS_ITEM_OPERATOR && node->itemtype != OPCLASS_ITEM_FUNCTION) {
                continue;
            }

            if (ListLength(node->name->objname) != 1) {
                return false;
            }

            TString funcName = StrVal(ListNodeNth(node->name->objname, 0));
            if (node->itemtype == OPCLASS_ITEM_OPERATOR) {
                NPg::TAmOpDesc amOpDesc;
                amOpDesc.ExtensionIndex = ExtensionIndex_;
                amOpDesc.Family = familyName;
                amOpDesc.Strategy = node->number;
                amOpDesc.LeftType = typeId;
                amOpDesc.RightType = typeId;
                amOpDesc.OperId = NPg::LookupOper(funcName, {typeId,typeId}).OperId;
                ops.push_back(amOpDesc);
            } else {
                NPg::TAmProcDesc amProcDesc;
                amProcDesc.ExtensionIndex = ExtensionIndex_;
                amProcDesc.Family = familyName;
                amProcDesc.ProcNum = node->number;
                amProcDesc.LeftType = typeId;
                amProcDesc.RightType = typeId;
                TVector<ui32> argTypes;
                for (int i = 0; i < ListLength(node->name->objargs); ++i) {
                    auto typeName = ListNodeNth(node->name->objargs, i);
                    if (NodeTag(typeName) != T_TypeName) {
                        return false;
                    }

                    TString value;
                    if (!ParseTypeName(CAST_NODE_EXT(PG_TypeName, T_TypeName, typeName), value)) {
                        return false;
                    }

                    argTypes.push_back(NPg::LookupType(value).TypeId);
                }

                amProcDesc.ProcId = NPg::LookupProc(funcName, argTypes).ProcId;
                procs.push_back(amProcDesc);
            }
        }

        Builder_.CreateOpClass(desc, ops, procs);
        return true;
    }

private:
    const ui32 ExtensionIndex_;
    NYql::NPg::IExtensionSqlBuilder& Builder_;
};

class TExtensionSqlParser : public NYql::NPg::IExtensionSqlParser {
public:
    void Parse(ui32 extensionIndex, const TVector<TString>& sqls, NYql::NPg::IExtensionSqlBuilder& builder) final {
        TExtensionHandler handler(extensionIndex, builder);
        for (const auto& sql : sqls) {
            NYql::PGParse(sql, handler);
        }

        NKikimr::NMiniKQL::RebuildTypeIndex();
    }
};

class TSystemFunctionsHandler : public IPGParseEvents {
public:
    TSystemFunctionsHandler(TVector<NPg::TProcDesc>& procs)
        : Procs_(procs)
    {}

    void OnResult(const List* raw) final {
        for (int i = 0; i < ListLength(raw); ++i) {
            if (!ParseRawStmt(LIST_CAST_NTH(RawStmt, raw, i))) {
                continue;
            }
        }
    }

    void OnError(const TIssue& issue) final {
        throw yexception() << "Can't parse system functions: " << issue.ToString();
    }

    [[nodiscard]]
    bool ParseRawStmt(const RawStmt* value) {
        auto node = value->stmt;
        switch (NodeTag(node)) {
        case T_CreateFunctionStmt:
            return ParseCreateFunctionStmt(CAST_NODE(CreateFunctionStmt, node));
        default:
            return false;
        }
    }

    [[nodiscard]]
    bool ParseCreateFunctionStmt(const CreateFunctionStmt* value) {
        NYql::NPg::TProcDesc desc;
        if (!ParseCreateFunctionStmtImpl(value, 0, nullptr, desc)) {
            return false;
        }

        Procs_.push_back(desc);
        return true;
    }

private:
    TVector<NPg::TProcDesc>& Procs_;
};

class TSystemFunctionsParser : public NYql::NPg::ISystemFunctionsParser {
public:
    void Parse(const TString& sql, TVector<NPg::TProcDesc>& procs) const final {
        TSystemFunctionsHandler handler(procs);
        NYql::PGParse(sql, handler);
    }
};

class TSqlLanguageParser : public NYql::NPg::ISqlLanguageParser, public IPGParseEvents {
public:
    TSqlLanguageParser() {
        Settings_.ClusterMapping["pg_catalog"] = TString(PgProviderName);
        Settings_.Mode = NSQLTranslation::ESqlMode::LIMITED_VIEW;
    }

    void Parse(const TString& sql, NPg::TProcDesc& proc) final {
        Y_ENSURE(!FreezeGuard_.Defined());
        CurrentProc_ = &proc;
        NYql::PGParse(sql, *this);
        CurrentProc_ = nullptr;
    }

    void ParseNode(const Node* stmt, NPg::TProcDesc& proc) final {
        Y_ENSURE(!FreezeGuard_.Defined());
        proc.ExprNode = nullptr;
        if (proc.VariadicType) {
            // Can't be expressed as usual lambda
            return;
        }

        TVector<NYql::TAstParseResult> results(1);
        results[0].Pool = std::make_unique<TMemoryPool>(4096);
        TVector<TStmtParseInfo> stmtParseInfos(1);
        TConverter converter(results, Settings_, "", &stmtParseInfos, false, proc.ArgTypes.size());
        converter.PrepareStatements();
        TAstNode* root = nullptr;
        switch (NodeTag(stmt)) {
        case T_SelectStmt:
            root = converter.ParseSelectStmt(CAST_NODE(SelectStmt, stmt), {.Inner = false});
            break;
        case T_ReturnStmt:
            root = converter.ParseReturnStmt(CAST_NODE(ReturnStmt, stmt));
            break;
        default:
            return;
        }

        if (!root) {
            //Cerr << "Can't parse SQL for function: " << proc.Name << ", " << results[0].Issues.ToString();
            return;
        }

        root = converter.L(converter.A("block"), converter.Q(converter.FinishStatements()));
        if (NodeTag(stmt) == T_SelectStmt) {
            root = converter.AsScalarContext(root);
        }

        TVector<TAstNode*> args;
        for (ui32 i = 0; i < proc.ArgTypes.size(); ++i) {
            args.push_back(converter.A("$p" + ToString(i + 1)));
        }

        root = converter.MakeLambda(args, root);
        auto program = converter.L(converter.L(converter.A("return"), root));
        TExprNode::TPtr graph;
        Ctx_.IssueManager.Reset();
        if (!CompileExpr(*program, graph, Ctx_, nullptr, nullptr, false, Max<ui32>(), 1)) {
            Cerr << "Can't compile  SQL for function: " << proc.Name << ", " << Ctx_.IssueManager.GetIssues().ToString();
            return;
        }

        SavedNodes_.push_back(graph);
        proc.ExprNode = graph.Get();
    }

    void Freeze() final {
        Y_ENSURE(!FreezeGuard_.Defined());
        FreezeGuard_.ConstructInPlace(Ctx_);
    }

    TExprContext& GetContext() final {
        Y_ENSURE(FreezeGuard_.Defined());
        return Ctx_;
    }

    void OnResult(const List* raw) final {
        if (ListLength(raw) == 1) {
            ParseNode(LIST_CAST_NTH(RawStmt, raw, 0)->stmt, *CurrentProc_);
        }
    }

    void OnError(const TIssue& issue) final {
        throw yexception() << "Can't parse SQL for function: " << CurrentProc_->Name << ", " << issue.ToString();
    }

private:
    NSQLTranslation::TTranslationSettings Settings_;
    TExprContext Ctx_;
    TVector<TExprNode::TPtr> SavedNodes_;
    TMaybe<TExprContext::TFreezeGuard> FreezeGuard_;
    NPg::TProcDesc* CurrentProc_ = nullptr;
};

std::unique_ptr<NPg::IExtensionSqlParser> CreateExtensionSqlParser() {
    return std::make_unique<TExtensionSqlParser>();
}

std::unique_ptr<NYql::NPg::ISystemFunctionsParser> CreateSystemFunctionsParser() {
    return std::make_unique<TSystemFunctionsParser>();
}

std::unique_ptr<NYql::NPg::ISqlLanguageParser> CreateSqlLanguageParser() {
    return std::make_unique<TSqlLanguageParser>();
}

class TTranslator : public NSQLTranslation::ITranslator {
public:
    NSQLTranslation::ILexer::TPtr MakeLexer(const NSQLTranslation::TTranslationSettings& settings) final {
        Y_UNUSED(settings);
        ythrow yexception() << "Unsupported method";
    }

    NYql::TAstParseResult TextToAst(const TString& query, const NSQLTranslation::TTranslationSettings& settings,
        NYql::TWarningRules* warningRules, NYql::TStmtParseInfo* stmtParseInfo) final {
        Y_UNUSED(warningRules);
        return PGToYql(query, settings, stmtParseInfo);
    }

    google::protobuf::Message* TextToMessage(const TString& query, const TString& queryName,
        NYql::TIssues& issues, size_t maxErrors, const NSQLTranslation::TTranslationSettings& settings) final {
        Y_UNUSED(query);
        Y_UNUSED(queryName);
        Y_UNUSED(issues);
        Y_UNUSED(maxErrors);
        Y_UNUSED(settings);
        ythrow yexception() << "Unsupported method";
    }

    NYql::TAstParseResult TextAndMessageToAst(const TString& query, const google::protobuf::Message& protoAst,
        const NSQLTranslation::TSQLHints& hints, const NSQLTranslation::TTranslationSettings& settings) final {
        Y_UNUSED(query);
        Y_UNUSED(protoAst);
        Y_UNUSED(hints);
        Y_UNUSED(settings);
        ythrow yexception() << "Unsupported method";
    }

    TVector<NYql::TAstParseResult> TextToManyAst(const TString& query, const NSQLTranslation::TTranslationSettings& settings,
        NYql::TWarningRules* warningRules, TVector<NYql::TStmtParseInfo>* stmtParseInfo) final {
        Y_UNUSED(warningRules);
        return PGToYqlStatements(query, settings, stmtParseInfo);
    }
};

NSQLTranslation::TTranslatorPtr MakeTranslator() {
    return MakeIntrusive<TTranslator>();
}

} // NSQLTranslationPG
