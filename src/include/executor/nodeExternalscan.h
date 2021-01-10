/*-------------------------------------------------------------------------
 *
 * nodeExternalscan.h
 *
 * Portions Copyright (c) 2007-2008, Greenplum inc
 * Portions Copyright (c) 2012-Present Pivotal Software, Inc.
 *
 *
 * IDENTIFICATION
 *	    src/include/executor/nodeExternalscan.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef NODEEXTERNALSCAN_H
#define NODEEXTERNALSCAN_H

#include "nodes/execnodes.h"

extern ExternalScanState *ExecInitExternalScan(ExternalScan *node, EState *estate, int eflags);
extern TupleTableSlot *ExecExternalScan(ExternalScanState *node);
extern void ExecEndExternalScan(ExternalScanState *node);
extern void ExecReScanExternal(ExternalScanState *node);
extern void ExecSquelchExternalScan(ExternalScanState *node);

#endif   /* NODEEXTERNALSCAN_H */
