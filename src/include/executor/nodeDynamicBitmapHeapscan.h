/*-------------------------------------------------------------------------
 *
 * nodeDynamicBitmapHeapscan.h
 *
 * Portions Copyright (c) 2012 - present, EMC/Greenplum
 * Portions Copyright (c) 2012-Present Pivotal Software, Inc.
 *
 *
 * IDENTIFICATION
 *	    src/include/executor/nodeDynamicBitmapHeapscan.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef NODEDYNAMICBITMAPHEAPSCAN_H
#define NODEDYNAMICBITMAPHEAPSCAN_H

#include "nodes/execnodes.h"

extern DynamicBitmapHeapScanState *ExecInitDynamicBitmapHeapScan(DynamicBitmapHeapScan *node, EState *estate, int eflags);
extern TupleTableSlot *ExecDynamicBitmapHeapScan(DynamicBitmapHeapScanState *node);
extern void ExecEndDynamicBitmapHeapScan(DynamicBitmapHeapScanState *node);
extern void ExecReScanDynamicBitmapHeapScan(DynamicBitmapHeapScanState *node);

#endif
