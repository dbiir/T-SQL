#include <stdlib.h>
#include <unistd.h>
#include "postgres.h"

#include <curl/curl.h>
#include "storage/spin.h"
#ifndef HTTP_TS_H
#define HTTP_TS_H
/*
 * HTTP_TYPE
 * 0 means master local timestamp
 * 1 means use curl to get LTS timestamp
 * 2 means use tcp to get LTS timestamp
 * 3 means use hlc to get LTS timestamp
 * 4 means ss use hlc, sss use tcp to get LTS timestamp
 */
#define HTTP_TYPE 3
typedef struct Lts_timestamp
{
    uint64 lts;
    slock_t lts_lock;
}Lts_timestamp;
extern uint64 GetTimeStamp(void);
extern void getltsShmemInit(void);
extern Size getltsShmemSize(void);
extern Lts_timestamp* lts_timestamp;

extern void CloseToLts(void);
extern void ConnectToLts(void);
#endif
