#include "postgres.h"
#include <stdlib.h>
#include <unistd.h>
#include <curl/curl.h>
#include <fcntl.h>
#include <unistd.h>
#include "utils/guc.h"
#include "storage/shmem.h"
#include "miscadmin.h"
#include "libtcp/libtcpforc.h"
#include "tdb/storage_param.h"
#include "tdb/timestamp_transaction/http.h"
#include "tdb/timestamp_transaction/hlc.h"

HLCTime *hlc_ts = NULL;

inline static int64_t CurrentSeconds() {
	int64_t ts = 0;
	struct timeval tv;
	if (gettimeofday(&tv, NULL)) {
		fprintf(stderr, "TDSQL: get time of day failed");
	} else {
		ts = (uint64_t)(tv.tv_sec);
	}
	return ts;
}
inline static uint64_t uint64_max(uint64_t a, uint64_t b, uint64_t c) {
	uint64_t m;
	m = a > b ? a : b;
	m = m > c ? m : c;
	return m;
}

void initNULLHLC(HLCTime *hlc) 
{
	hlc->m_wall_time_ = CurrentSeconds() << physicalShift;
	hlc->m_logic_time_ = 0;
}

void initHLC(HLCTime *hlc, uint64_t wall_time, uint64_t logic_time)
{
	hlc->m_wall_time_ = wall_time;
	hlc->m_logic_time_ = logic_time;
}

void hlcsend(HLCTime *hlc) {
	SpinLockAcquire(&hlc->mutex_);
	uint64_t pt = CurrentSeconds() << physicalShift;
	if (hlc->m_wall_time_ < pt) {
		hlc->m_wall_time_ = pt;
		hlc->m_logic_time_ = 0;
	} else {
		hlc->m_logic_time_ ++;
	}
 	SpinLockRelease(&hlc->mutex_);
}

void hlcupdate(HLCTime *chlc, HLCTime hlc) {
	SpinLockAcquire(&chlc->mutex_);
	uint64_t pt = CurrentSeconds() << physicalShift;
	uint64_t old_wall_time = chlc->m_wall_time_;
	chlc->m_wall_time_ = uint64_max(chlc->m_wall_time_, pt, hlc.m_wall_time_);
	if (chlc->m_wall_time_ == hlc.m_wall_time_ && chlc->m_wall_time_ == old_wall_time) {
		chlc->m_logic_time_ = chlc->m_logic_time_ > hlc.m_logic_time_ ? 
						chlc->m_logic_time_ + 1 : hlc.m_logic_time_ + 1;
	} else if (chlc->m_wall_time_ == old_wall_time) {
		chlc->m_logic_time_ ++;
	} else if (chlc->m_wall_time_ == hlc.m_wall_time_) {
		chlc->m_logic_time_ = hlc.m_logic_time_ + 1;
	} else if (chlc->m_wall_time_ == pt) {
		chlc->m_logic_time_ = 0;
	}
	SpinLockRelease(&chlc->mutex_);
}

void update_with_cts(uint64_t hlc) {
	uint64_t wall_time = hlc >> physicalShift << physicalShift;
	uint64_t logic_time = hlc << (64 - physicalShift) >> (64 - physicalShift);
	HLCTime newhlc;
	newhlc.m_wall_time_ = wall_time;
	newhlc.m_logic_time_ = logic_time;
	hlcupdate(hlc_ts, newhlc);
}

uint64_t getCurrentHLC() {
	hlcsend(hlc_ts);
	uint64_t hlc = hlc_ts->m_wall_time_ | hlc_ts->m_logic_time_;
	return hlc;
}


void getHLCShmemInit(void)
{
    bool found;
    hlc_ts = (HLCTime *)
        ShmemInitStruct("hlc_timestamp", sizeof(HLCTime), &found);

    if (!IsUnderPostmaster)
    {
        Assert(!found);
        initNULLHLC(hlc_ts);
        SpinLockInit(&hlc_ts->mutex_);
    }
    else
    {
        Assert(found);
    }
}

Size getHLCShmemSize(void)
{
    return sizeof(HLCTime);
}
