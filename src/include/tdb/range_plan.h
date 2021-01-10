/*----------------------------------
 *
 * range_plan.h
 *
 * Copyright (c) 2019-Present, TDSQL
 *
 * src/include/tdb/range_plan.h
 *----------------------------------
 */

#ifndef RANGE_PLAN_H
#define RANGE_PLAN_H

#include "tdb/range_struct.h"

#define MAX_RANGE_SIZE  ((KeySize)(4096) * (1024))
#define MIN_RANGE_SIZE  ((KeySize)(0))

#define MAX_SEGMENT_THRESHOLD_PERCENT

#define PLAN_NUM 		(10)
#define MAX_PLAN_SIZE   (512)
extern int RangePlanIdGenerator;

typedef enum RangePlanType
{
	MS_INVALID_TYPE = 0,
	MS_HEART_BEAT = 1,
	MS_HEART_BEAT_SUCCESS,
	MS_HEART_BEAT_FAILED,

	/* MS find some range need to be splited, so MS notifies seg to calculate a split key out */
	MS_SPLIT_PREPARE,
	/* Seg successfully calculates the split key, and sends the generated split plan to MS
	 * Next, MS receives the generated split plan, generates a rangeid, for the new range and
	 * sends it to all nodes, asking all nodes to update the status of the range. At this point,
	 * you need to lock the range
	 */
	MS_SPLIT_PLAN,
	/*
	 * After all seg nodes have been updated, prompt MS to complete the update
	 */
	MS_SPLIT_SUCCESS,
	MS_SPLIT_FAILD,
	/* MS discovers that all nodes have been modified and resends the unlock command to all nodes. */
	MS_SPLIT_COMPLETE,

	MS_MERGE_PLAN,
	MS_MERGE_SUCCESS,
	MS_MERGE_FAILED,

	MS_ADDREPLICA_PLAN,
	MS_ADDREPLICA_SUCCESS,
	MS_ADDREPLICA_FAILED,

	MS_REMOVEREPLICA_PLAN,
	MS_REMOVEREPLICA_SUCCESS,
	MS_REMOVEREPLICA_FAILED,

	MS_REBALANCE_PLAN,
	MS_REBALANCE_SUCCESS,
	MS_REBALANCE_FAILED,

	MS_TRANSFERLEADER_PLAN,
	MS_TRANSFERLEADER_SUCCESS,
	MS_TRANSFERLEADER_FAILED,

	MS_COMPLETE,
}RangePlanType;

typedef struct RangePlanDesc
{
	Node node_head;
	RangePlanType  ms_plan_type;       /* type see above */
	int plan_id;
	int complete_count;
	bool send_type[MAXSEGCOUNT];
	int retry_time[MAXSEGCOUNT];
	bool complete_type[MAXSEGCOUNT];
}RangePlanDesc;

typedef RangePlanDesc* RangePlan;

typedef struct HeartBeatPlanDesc
{
	RangePlanDesc header;
	int segnum;
	char ip[MAXSEGCOUNT][20];
}HeartBeatPlanDesc;
typedef HeartBeatPlanDesc* HeartBeatPlan;

typedef struct SplitPreparePlanDesc
{
	RangePlanDesc header;
	SegmentID targetSegID;
	RangeID new_range_id;
	RangeDesc *split_range;
	TupleKeySlice split_key;
}SplitPreparePlanDesc;
typedef SplitPreparePlanDesc* SplitPreparePlan;

typedef struct SplitPlanDesc
{
	RangePlanDesc header;
	SegmentID targetSegID;
	RangeDesc *old_range;
	RangeDesc *new_range;
}SplitPlanDesc;

typedef SplitPlanDesc* SplitPlan;

typedef struct MergePlanDesc
{
	RangePlanDesc header;
	RangeDesc *first_range;
	RangeDesc *second_range;
}MergePlanDesc;

typedef MergePlanDesc* MergePlan;

typedef struct RebalancePlanDesc
{
	RangePlanDesc header;
	SegmentID sourceSegID;
	SegmentID targetSegID;
	RangeDesc *rebalance_range;
}RebalancePlanDesc;

typedef RebalancePlanDesc* RebalancePlan;

typedef struct AddReplicaPlanDesc
{
	RangePlanDesc header;
	SegmentID targetSegID;
	RangeDesc *range;
}AddReplicaPlanDesc;

typedef AddReplicaPlanDesc* AddReplicaPlan;

typedef struct RemoveReplicaPlanDesc
{
	RangePlanDesc header;
	SegmentID targetSegID;
	RangeDesc *range;
}RemoveReplicaPlanDesc;

typedef RemoveReplicaPlanDesc* RemoveReplicaPlan;

typedef struct TransferLeaderPlanDesc
{
	RangePlanDesc header;
	ReplicaID targetID;
	RangeDesc *range;
}TransferLeaderPlanDesc;

typedef TransferLeaderPlanDesc* TransferLeaderPlan;

typedef struct Plan_queue
{
	/* data */
	Size end;
	Size start;
	char buffer[PLAN_NUM][MAX_PLAN_SIZE];	/* every ceil store a Plan */
}Plan_queue;

extern Plan_queue *plan_queue;

extern void RangePlanQueueShmemInit(void);
extern Size RangePlanQueueShmemSize(void);
extern SegmentSatistics* copyTempSegStat(List* context);
extern char* TransferSplitPlanToBuffer(SplitPlan sp, Size* length);
extern SplitPreparePlan TransferBufferToSplitPrepare(char* buffer);
extern char* TransferSplitPrepareToBuffer(SplitPreparePlan spp, Size* length);
extern SplitPlan TransferBufferToSplitPlan(char* buffer);
extern char* TransferAddReplicaPlanToBuffer(AddReplicaPlan sp, Size* length);
extern AddReplicaPlan TransferBufferToAddReplicaPlanPlan(char* buffer);
extern char* TransferRemoveReplicaToBuffer(RemoveReplicaPlan spp, Size* length);
extern RemoveReplicaPlan TransferBufferToRemoveReplica(char* buffer);
extern char* TransferRebalancePlanToBuffer(RebalancePlan sp, Size* length);
extern RebalancePlan TransferBufferToRebalancePlan(char* buffer);
extern char* TransferTransferLeaderPlanToBuffer(TransferLeaderPlan sp, Size* length);
extern TransferLeaderPlan TransferBufferToTransferLeaderPlan(char* buffer);
extern char* TransferMergePlanToBuffer(MergePlan sp, Size* length);
extern MergePlan TransferBufferToMergePlan(char* buffer);

extern SplitPlan TransferSplitPrepareToSplitPlan(SplitPreparePlan lsp);

extern void initRangePlanHead(RangePlan rangeplan, RangePlanType type);
#endif
