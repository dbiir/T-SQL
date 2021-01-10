/*-------------------------------------------------------------------------
 *
 * cdbdtxcontextinfo.h
 *
 * Portions Copyright (c) 2007-2008, Greenplum inc
 * Portions Copyright (c) 2012-Present Pivotal Software, Inc.
 *
 *
 * IDENTIFICATION
 *	    src/include/cdb/cdbdtxcontextinfo.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef CDBDTXTS_H
#define CDBDTXTS_H
typedef unsigned long int uint64;

typedef struct DtxTimeStamp
{
    uint64  start_ts;
    uint64  commit_ts;
    uint64  lts_low;
    uint64  lts_high;
} DtxTimeStamp;

typedef struct SequenceModeGTS
{
    uint64  start_ts;
    slock_t ts_lock;
} SequenceModeGTS;

extern DtxTimeStamp global_tmp_timestamp;
extern SequenceModeGTS *global_sequence_gts;

extern void DtxTimeStamp_Reset(DtxTimeStamp *dtxts);
extern int DtxTimeStamp_SerializeSize(void);

extern void DtxTimeStamp_Serialize(char *buffer, DtxTimeStamp *dtxts);
extern void DtxTimeStamp_Deserialize(const char *serializeddtxts,
									   int serializeddtxtslen,
									   DtxTimeStamp *dtxts);
extern void UpdateLowerUpperFromDtxTimeStamp(uint64 lower, uint64 upper,
								 DtxTimeStamp *dtxts);
extern void DtxTimeStamp_StartGtsUpdate(DtxTimeStamp *dtxts, uint64 start_ts);

extern void SequenceGTSShmemInit(void);
extern Size SequenceGTSShmemSize(void);
extern void SequenceGTSUpdate(uint64 commit_ts);
extern uint64 SequenceGTSGet(void);
#endif   /* CDBDTXCONTEXTINFO_H */
