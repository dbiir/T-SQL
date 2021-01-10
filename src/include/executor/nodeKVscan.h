/* 
 * nodeKVscan.h
 *
 *
 *
 * Portions Copyright (c) 1996-2014, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/executor/nodeKVscan.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef NODEKVSCAN_H
#define NODEKVSCAN_H

#include "nodes/execnodes.h"
/*
 * prototypes from functions in execRocksScan.c
 */
extern TupleTableSlot *KVScanNext(ScanState *scanState);
extern void BeginScanKVRelation(ScanState *scanState);
extern void EndScanKVRelation(ScanState *scanState);
extern void ReScanKVRelation(ScanState *scanState);

#endif