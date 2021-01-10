#ifndef HLC_H
#define HLC_H

/* C standard header files */
#include "postgres.h"
#include <stdlib.h>
#include <unistd.h>
#include <unistd.h>
#include "storage/spin.h"
static int physicalShift = 24;

typedef struct HLCTime
{
	uint64_t m_wall_time_;
	uint64_t m_logic_time_;
	slock_t mutex_;
}HLCTime;

extern HLCTime *hlc_ts;
extern uint64_t getCurrentHLC();
extern void update_with_cts(uint64_t hlc);

extern Size getHLCShmemSize(void);
extern void getHLCShmemInit(void);
#endif
