/*------------------------------------
 *
 * execKVScan.c
 *
 * by williamcliu
 *
 *------------------------------------
 */

#include "postgres.h"

#include "utils/snapmgr.h"
#include "executor/executor.h"
#include "nodes/execnodes.h"
#include "executor/nodeKVscan.h"
#include "tdb/tdbkvam.h"

#include "utils/guc.h"
#include "access/rwset.h"

#include "nodes/primnodes.h"
#include "parser/parsetree.h"

TupleTableSlot*
KVScanNext(ScanState* scanState)
{
	Assert(IsA(scanState, SeqScanState) ||
		   IsA(scanState, DynamicSeqScanState));
	SeqScanState *node = (SeqScanState*)scanState;
    node->rks_ScanDesc->Scantype = scanState->ps.state->es_plannedstmt->commandType;

    Temporal   *temporal;
	Scan  *tableScanNode;
	tableScanNode = (Scan *)scanState->ps.plan;
	temporal = gettemporal(tableScanNode->scanrelid, node->ss.ps.state->es_range_table);

	if (temporal)
		kvengine_history_getnext(node->rks_ScanDesc, node->ss.ss_ScanTupleSlot);
	else
		kvengine_getnext(node->rks_ScanDesc, ForwardScanDirection, node->ss.ss_ScanTupleSlot);
 

	/* append key to read/write set */
	if (transam_mode == TRANSAM_MODE_NEW_OCC
		&& 0)
	{
		AppendKeyToReadWriteSet(node->ss.ss_ScanTupleSlot->PRIVATE_tts_key.len,
					(char *)node->ss.ss_ScanTupleSlot->PRIVATE_tts_key.data,
					scanState->ps.state->es_plannedstmt->commandType);

		AppendReadXidWithKey(node->ss.ss_ScanTupleSlot->PRIVATE_tts_key.len,
					(char *)node->ss.ss_ScanTupleSlot->PRIVATE_tts_key.data,
					scanState->ps.state->es_plannedstmt->commandType);
	}

	return node->ss.ss_ScanTupleSlot;
}

void
BeginScanKVRelation(ScanState* scanState)
{
	Assert(IsA(scanState, SeqScanState) ||
		   IsA(scanState, DynamicSeqScanState));
	SeqScanState *node = (SeqScanState*)scanState;

	Temporal   *temporal;
	Scan  *tableScanNode;
	tableScanNode = (Scan *)scanState->ps.plan;
	temporal = gettemporal(tableScanNode->scanrelid, node->ss.ps.state->es_range_table);

	struct tm* tmp_time = (struct tm*)palloc(sizeof(struct tm));
	strptime(temporal->lower, "%Y-%m-%d %H:%M:%S", tmp_time);
	time_t xmin_ts = mktime(tmp_time);

	time_t xmax_ts = xmin_ts;
	if (temporal->upper)
	{
		strptime(temporal->upper, "%Y-%m-%d %H:%M:%S", tmp_time);
		time_t xmax_ts = mktime(tmp_time);
	}

	if (temporal)
		node->rks_ScanDesc = kvengine_his_normal_beginscan(scanState->ss_currentRelation, xmin_ts * 1000000, xmax_ts * 1000000);
	else
		node->rks_ScanDesc = kvengine_beginscan(scanState->ss_currentRelation,
 										   scanState->ps.state->es_snapshot);

 	// node->ss.scan_state = SCAN_SCAN;
	pfree(tmp_time);
}

void
EndScanKVRelation(ScanState* scanState)
{
	Assert(IsA(scanState, SeqScanState) ||
		   IsA(scanState, DynamicSeqScanState));
	SeqScanState *node = (SeqScanState *)scanState;

	Assert(node->rks_ScanDesc != NULL);

	kvengine_endscan(node->rks_ScanDesc);
}

void
ReScanKVRelation(ScanState* scanState)
{
	Assert(IsA(scanState, SeqScanState) ||
		   IsA(scanState, DynamicSeqScanState));
	SeqScanState *node = (SeqScanState *)scanState;

	Assert(node->rks_ScanDesc != NULL);

	kvengine_rescan(node->rks_ScanDesc);
}
