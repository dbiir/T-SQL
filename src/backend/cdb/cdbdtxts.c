/*-------------------------------------------------------------------------
 *
 * cdbdtxcontextinfo.c
 *
 * Portions Copyright (c) 2007-2008, Greenplum inc
 * Portions Copyright (c) 2012-Present Pivotal Software, Inc.
 *
 *
 * IDENTIFICATION
 *	    src/backend/cdb/cdbdtxcontextinfo.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "cdb/cdbdistributedsnapshot.h"
#include "cdb/cdblocaldistribxact.h"
#include "cdb/cdbdtxts.h"
#include "miscadmin.h"
#include "access/transam.h"
#include "cdb/cdbvars.h"
#include "cdb/cdbtm.h"
#include "access/xact.h"
#include "utils/guc.h"
#include "utils/tqual.h"
#include "access/rwset.h"
#include "tdb/timestamp_transaction/http.h"

DtxTimeStamp global_tmp_timestamp = {0, 0, 0, (uint64) -1};
SequenceModeGTS* global_sequence_gts = NULL;

/* Queries for MS messages */
#define	LTS_ST "LST_ST"
int
DtxTimeStamp_SerializeSize(void)
{
	int	size = 0;
	size += sizeof(uint64) + sizeof(uint64) + sizeof(uint64) + sizeof(uint64);
	return size;
}

void
DtxTimeStamp_Serialize(char *buffer, DtxTimeStamp *dtxts)
{
	char	   *p = buffer;
    
    if (CurrentReadWriteSetSlot && transam_mode == TRANSAM_MODE_NEW_OCC)
    {
        dtxts->lts_low = CurrentReadWriteSetSlot->lower;
        dtxts->lts_high = CurrentReadWriteSetSlot->upper;
    }
	memcpy(p, &dtxts->start_ts, sizeof(uint64));
	p += sizeof(uint64);

	memcpy(p, &dtxts->commit_ts, sizeof(uint64));
	p += sizeof(uint64);

	memcpy(p, &dtxts->lts_low, sizeof(uint64));
	p += sizeof(uint64);

	memcpy(p, &dtxts->lts_high, sizeof(uint64));
	p += sizeof(uint64);
}

void
DtxTimeStamp_Reset(DtxTimeStamp *dtxts)
{
    if (consistency_mode != CONSISTENCY_MODE_CAUSAL)
        dtxts->start_ts = 0;
    dtxts->commit_ts = 0;
    dtxts->lts_low = 0;
    dtxts->lts_high = (uint64) -1;
}

void
DtxTimeStamp_Deserialize(const char *serializeddtxts,
									   int serializeddtxtslen,
									   DtxTimeStamp *dtxts)
{
	DtxTimeStamp_Reset(dtxts);
	if (serializeddtxtslen > 0)
	{
		const char *p = serializeddtxts;
		memcpy(&dtxts->start_ts, p, sizeof(uint64));
		p += sizeof(uint64);
		memcpy(&dtxts->commit_ts, p, sizeof(uint64));
		p += sizeof(uint64);
		memcpy(&dtxts->lts_low, p, sizeof(uint64));
		p += sizeof(uint64);
		memcpy(&dtxts->lts_high, p, sizeof(uint64));
		p += sizeof(uint64);
	}
}

void
UpdateLowerUpperFromDtxTimeStamp(uint64 lower, uint64 upper,
								 DtxTimeStamp *dtxts)
{
	if (lower != 0)
		dtxts->lts_low = dtxts->lts_low > lower ? dtxts->lts_low : lower;
	if (upper != 0)
		dtxts->lts_high = dtxts->lts_high > upper ? dtxts->lts_high : upper;
}

void 
DtxTimeStamp_StartGtsUpdate(DtxTimeStamp *dtxts, uint64 start_ts)
{
    if (dtxts == NULL)
		return;
    if (consistency_mode == CONSISTENCY_MODE_CAUSAL &&
        dtxts->start_ts < start_ts)
    {
        dtxts->start_ts = start_ts + 1;
    }
}

void
SequenceGTSShmemInit(void)
{
	bool found;
	global_sequence_gts = (SequenceModeGTS*)
		ShmemInitStruct("Sequence GTS", sizeof(SequenceModeGTS), &found);

	if (!IsUnderPostmaster)
	{
		Assert(!found);
		global_sequence_gts->start_ts = GetTimeStamp();
        SpinLockInit(&global_sequence_gts->ts_lock);
	}
	else
	{
		Assert(found);
	}
}

Size
SequenceGTSShmemSize(void)
{
	return sizeof(SequenceModeGTS);
}

void
SequenceGTSUpdate(uint64 commit_ts)
{
    SpinLockAcquire(&global_sequence_gts->ts_lock);
    if (commit_ts > global_sequence_gts->start_ts)
    {
        global_sequence_gts->start_ts = commit_ts;
    }
    SpinLockRelease(&global_sequence_gts->ts_lock);
}

uint64
SequenceGTSGet(void)
{
    uint64 ts = 0;
    SpinLockAcquire(&global_sequence_gts->ts_lock);
    ts = global_sequence_gts->start_ts;
    SpinLockRelease(&global_sequence_gts->ts_lock);
    return ts;
}