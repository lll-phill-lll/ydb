#include "node_whiteboard.h"

#include <ydb/core/blobstorage/groupinfo/blobstorage_groupinfo.h>

namespace NKikimr::NNodeWhiteboard {

TEvWhiteboard::TEvBSGroupStateUpdate::TEvBSGroupStateUpdate(const TIntrusivePtr<TBlobStorageGroupInfo>& groupInfo) {
    Record.SetGroupID(groupInfo->GroupID.GetRawId());
    Record.SetGroupGeneration(groupInfo->GroupGeneration);
    Record.SetErasureSpecies(groupInfo->Type.ErasureSpeciesName(groupInfo->Type.GetErasure()));
    if (ui32 numVDisks = groupInfo->GetTotalVDisksNum()) {
        for (ui32 i = 0; i < numVDisks; ++i) {
            VDiskIDFromVDiskID(groupInfo->GetVDiskId(i), Record.AddVDiskIds());
            const TActorId& actorId = groupInfo->GetActorId(i);
            Record.AddVDiskNodeIds(actorId.NodeId());
        }
    } else {
        Record.SetNoVDisksInGroup(true);
    }
    Record.SetStoragePoolName(groupInfo->GetStoragePoolName());
    if (groupInfo->GetEncryptionMode() != TBlobStorageGroupInfo::EEM_NONE) {
        Record.SetEncryption(true);
    }
    if (groupInfo->BlobDepotId) {
        Record.SetBlobDepotId(*groupInfo->BlobDepotId);
    }
    Record.SetGroupSizeInUnits(groupInfo->GroupSizeInUnits);
    if (const auto& bridgeProxyGroupId = groupInfo->GetBridgeProxyGroupId()) {
        bridgeProxyGroupId->CopyToProto(&Record, &decltype(Record)::SetBridgeProxyGroupId);
    }
    groupInfo->GetBridgePileId().CopyToProto(&Record, &decltype(Record)::SetBridgePileId);
}

} // NKikimr::NNodeWhiteboard
