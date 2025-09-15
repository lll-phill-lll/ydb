#include "utils/dq_setup.h"
#include "utils/dq_factories.h"
#include "utils/utils.h"

#include <yql/essentials/minikql/comp_nodes/ut/mkql_computation_node_ut.h>
#include <yql/essentials/minikql/mkql_node_cast.h>
#include <yql/essentials/minikql/invoke_builtins/mkql_builtins.h>
#include <yql/essentials/minikql/computation/mkql_computation_node_holders.h>
#include <ydb/library/yql/dq/comp_nodes/dq_block_hash_join.h>

namespace NKikimr {
namespace NMiniKQL {

namespace {

NUdf::TUnboxedValue DoTestDqBlockHashJoin(
    TDqSetup<false>& setup,
    TType* leftType, NUdf::TUnboxedValue&& leftListValue, const TVector<ui32>& leftKeyColumns,
    TType* rightType, NUdf::TUnboxedValue&& rightListValue, const TVector<ui32>& rightKeyColumns,
    EJoinKind joinKind
) {
    TDqProgramBuilder& pb = setup.GetDqProgramBuilder();

    // Convert regular lists to block lists
    TRuntimeNode leftList = pb.Arg(leftType);
    TRuntimeNode rightList = pb.Arg(rightType);
    
    // Convert to block format
    const auto leftBlockList = ToBlockList(pb, leftList);
    const auto rightBlockList = ToBlockList(pb, rightList);
    
    const auto leftStream = ToWideStream(pb, leftBlockList);
    const auto rightStream = ToWideStream(pb, rightBlockList);
    const auto joinNode = pb.DqBlockHashJoin(leftStream, rightStream, joinKind, leftKeyColumns, rightKeyColumns, leftStream.GetStaticType());
    
    const auto resultNode = FromWideStream(pb, joinNode);

    const auto graph = setup.BuildGraph(resultNode, {leftList.GetNode(), rightList.GetNode()});
    auto& ctx = graph->GetContext();

    graph->GetEntryPoint(0, true)->SetValue(ctx, std::move(leftListValue));
    graph->GetEntryPoint(1, true)->SetValue(ctx, std::move(rightListValue));
    return graph->GetValue();
}

void RunTestDqBlockHashJoin(
    TDqSetup<false>& setup, EJoinKind joinKind,
    TType* expectedType, const NUdf::TUnboxedValue& expected,
    TType* leftType, NUdf::TUnboxedValue&& leftListValue, const TVector<ui32>& leftKeyColumns,
    TType* rightType, NUdf::TUnboxedValue&& rightListValue, const TVector<ui32>& rightKeyColumns
) {
    const auto got = DoTestDqBlockHashJoin(
        setup,
        leftType, std::move(leftListValue), leftKeyColumns,
        rightType, std::move(rightListValue), rightKeyColumns,
        joinKind
    );
    
    UNIT_ASSERT(got.HasValue());
    CompareListsIgnoringOrder(expectedType, expected, got);
}

} // namespace

Y_UNIT_TEST_SUITE(TDqBlockHashJoinBasicTest) {

    Y_UNIT_TEST(TestBasicPassthrough) {
        TDqSetup<false> setup(GetDqNodeFactory());
        
        TVector<ui64> leftKeys = {1, 2, 3, 4, 5};
        TVector<TString> leftValues = {"a", "b", "c", "d", "e"};
        
        TVector<ui64> rightKeys = {2, 3, 4, 6, 7};
        TVector<TString> rightValues = {"x", "y", "z", "u", "v"};

        // For inner join, we expect intersection of keys: 2, 3, 4
        TVector<ui64> expectedKeys = {2, 3, 4};
        TVector<TString> expectedLeftValues = {"b", "c", "d"};
        TVector<TString> expectedRightValues = {"x", "y", "z"};

        auto [leftType, leftList] = ConvertVectorsToTuples(setup, leftKeys, leftValues);
        auto [rightType, rightList] = ConvertVectorsToTuples(setup, rightKeys, rightValues);
        
        // Create expected result as concatenated tuples (left + right)
        TVector<ui64> expectedResultKeys = {2, 3, 4};
        TVector<TString> expectedResultLeftValues = {"b", "c", "d"};
        TVector<TString> expectedResultRightValues = {"x", "y", "z"};
        auto [expectedType, expected] = ConvertVectorsToTuples(setup, 
            expectedResultKeys, expectedResultLeftValues, expectedResultRightValues);

        RunTestDqBlockHashJoin(
            setup, EJoinKind::Inner,
            expectedType, expected,
            leftType, std::move(leftList), {0},
            rightType, std::move(rightList), {0}
        );
    }

    Y_UNIT_TEST(TestEmptyStreams) {
        TDqSetup<false> setup(GetDqNodeFactory());
        
        TVector<ui64> emptyKeys;
        TVector<TString> emptyValues;

        auto [leftType, leftList] = ConvertVectorsToTuples(setup, emptyKeys, emptyValues);
        auto [rightType, rightList] = ConvertVectorsToTuples(setup, emptyKeys, emptyValues);
        auto [expectedType, expected] = ConvertVectorsToTuples(setup, emptyKeys, emptyValues);

        RunTestDqBlockHashJoin(
            setup, EJoinKind::Inner,
            expectedType, expected,
            leftType, std::move(leftList), {0},
            rightType, std::move(rightList), {0}
        );
    }

} // Y_UNIT_TEST_SUITE

} // namespace NMiniKQL
} // namespace NKikimr 
