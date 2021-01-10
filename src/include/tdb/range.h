/*----------------------------------
 *
 * range.h
 *
 * Copyright (c) 2019-Present, TDSQL
 *
 * src/include/tdb/range.h
 *----------------------------------
 */
#include "tdb/range_universal.h"
#include "tdb/rangeid_generator.h"
#include "tdb/range_plan.h"

#define IsRangeIDValid(rangeid) ((rangeid) > 0 && (rangeid) < MAXRANGECOUNT)

extern void freeRangeDescPoints(RangeDesc* range);
extern void freeRangeDesc(RangeDesc range);
extern RangeDesc CreateNewRangeDesc(RangeID rangeID, TupleKeySlice startkey, TupleKeySlice endkey, Replica *replica, int replica_num);
extern Replica CreateNewReplica(ReplicaID replicaID, SegmentID segmentID, ReplicaID leader);
extern void AddReplicaToRange(RangeDesc *range, Replica replica);
extern void RemoveReplicaToRange(RangeDesc *range, SegmentID targetseg);
extern void TransferLeader(RangeDesc *range, ReplicaID targetreplica);
extern RangeDesc copyRangeDesc(RangeDesc range);
extern TupleKeySlice makeRangeDescKey(RangeID rangeID);
extern bool compareReplica(RangeDesc a, RangeDesc b);
extern RangeDesc initNewTableRangeDesc(Relation relation, RangeID rangeid, SegmentID *replica_segid, int replicanum);

extern void storeNewRangeDesc(RangeDesc range);
extern void removeRangeDesc(RangeDesc range);

extern void mergeRange(RangeDesc *a, RangeDesc *b);
extern RangeDesc findUpRangeDescByID(RangeID id);

extern SegmentID GetBestTargetSegment(SegmentID *have_used_seg, int have_used_seg_num, bool *find, RangeDesc range, SegmentSatistics* tempstat);
extern SegmentID* GetBestTargetSegmentList(int replica_num);
extern SegmentID GetBestSourceSegment(SegmentID *have_used_seg, int have_used_seg_num, bool *find, RangeDesc range, SegmentSatistics* tempstat);

extern List* ScanAllRange(void);
extern RangeDesc* range_scan_get_next(KVEngineScanDesc scan);
extern Replica findUpReplicaOnOneSeg(RangeDesc range, SegmentID segid, bool *isleader);
extern Replica findUpReplicaOnThisSeg(RangeDesc range, bool *isleader);
extern Replica findUpReplicaByReplicaID(RangeDesc range, ReplicaID id);

extern TupleKeySlice getRangeMiddleKey(RangeDesc range, Size range_size);
extern Replica replica_state_machine(Replica replica, RangePlanDesc plan);
extern Replica findUpLeader(RangeDesc range);

extern SegmentID* getRangeSegID(RangeDesc range);
