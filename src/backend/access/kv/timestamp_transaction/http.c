/*-------------------------------------------------------------------------
 *
 * http.c
 *		get timestamp
 *
 * Copyright (c) 2019-Present, TDSQL
 *
 * IDENTIFICATION
 *		src/backend/access/kv/timestamp_transaction/http.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"
#include <stdlib.h>
#include <unistd.h>
#include <curl/curl.h>
#include <fcntl.h>
#include <unistd.h>
#include "tdb/timestamp_transaction/http.h"
#include "utils/guc.h"
#include "storage/shmem.h"
#include "miscadmin.h"
#include "libtcp/libtcpforc.h"
#include "tdb/storage_param.h"
#include "tdb/timestamp_transaction/http.h"
#include "libtcp/ip.h"
#include "tdb/timestamp_transaction/hlc.h"

static size_t WriteTimeStampCallBack(void *ptr, size_t size, size_t nmemb, void *data);
Lts_timestamp *lts_timestamp = NULL;

static uint64 stouint64(char *ptr)
{
    uint64 num = 0;
    int i = 0;
    while (ptr[i])
    {
        num = num * 10 + (ptr[i++] - '0');
    }
    return num;
}

size_t WriteTimeStampCallBack(void *ptr, size_t size, size_t nmemb, void *data)
{
    size_t realsize = size * nmemb;
    uint64 *ts = (uint64 *)data;
    *ts = stouint64((char *)ptr);
    return realsize;
}

#if (HTTP_TYPE == 2) || (HTTP_TYPE == 4)
void ConnectToLts(void)
{
    int result = 0;
    for (int retry = 0; retry < 10; retry++)
    {
        result = connectTo(LTS_TCP_IP, 62389);
        if (result == 0)
            break;
    }

    if (result != 0)
    {
        ereport(ERROR, (errmsg("LTS: connection failed")));
    }
}

void CloseToLts(void)
{
    int result = closeConnection();
    if (result != 0)
    {
        ereport(ERROR, (errmsg("LTS: close failed")));
    }
}
#else
void ConnectToLts(void)
{
}
void CloseToLts(void) {}
#endif

static char curlip[100];

uint64 GetTimeStamp(void)
{
#if (HTTP_TYPE == 0)
	switch (http_delay)
	{
	case HTTP_DELAY_5MS:
		usleep(5000);
		break;
	case HTTP_DELAY_3MS:
		usleep(3000);
		break;
	case HTTP_DELAY_1MS:
		usleep(1000);
		break;
	case HTTP_DELAY_0MS:
	default:
		break;
	}
    SpinLockAcquire(&lts_timestamp->lts_lock);
    lts_timestamp->lts++;
    uint64 ts = lts_timestamp->lts;
    SpinLockRelease(&lts_timestamp->lts_lock);
    return ts;
#elif (HTTP_TYPE == 1)
    CURL *curl;
    CURLcode res;

    curl = curl_easy_init();
    if (!curl)
    {
        fprintf(stderr, "curl_easy_init() error");
        return 1;
    }
    uint64 ts;
    curl_easy_setopt(curl, CURLOPT_WRITEDATA, &ts);
    curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, WriteTimeStampCallBack);

	snprintf(curlip, "http://%s:62379/lts-cluster/api/ts/1", LTS_TCP_IP);
    curl_easy_setopt(curl, CURLOPT_URL, curlip);
    res = curl_easy_perform(curl);
    if (res != CURLE_OK)
    {
        fprintf(stderr, "curl_easy_perform() error: %s\n", curl_easy_strerror(res));
    }
    curl_easy_cleanup(curl);
#elif (HTTP_TYPE == 2)
	uint64 ts = getTimestamp();
	if (ts == (uint64)-1)
	{
		core_dump();
	}
	return ts;
#elif (HTTP_TYPE == 3)
	if (consistency_mode != CONSISTENCY_MODE_SEQUENCE)
	{
		switch (http_delay)
		{
		case HTTP_DELAY_10MS:
			usleep(10000);
			break;
		case HTTP_DELAY_5MS:
			usleep(5000);
			break;
		case HTTP_DELAY_3MS:
			usleep(3000);
			break;
		case HTTP_DELAY_1MS:
			usleep(1000);
			break;
		case HTTP_DELAY_03MS:
			usleep(300);
			break;			
		case HTTP_DELAY_0MS:
		default:
			break;
		}
	}
	return getCurrentHLC();
#elif (HTTP_TYPE == 4)
	if (consistency_mode == CONSISTENCY_MODE_SEQUENCE)
		return getCurrentHLC();
	else
	{
		switch (http_delay)
		{
		case HTTP_DELAY_10MS:
			usleep(10000);
			break;
		case HTTP_DELAY_5MS:
			usleep(5000);
			break;
		case HTTP_DELAY_3MS:
			usleep(3000);
			break;
		case HTTP_DELAY_1MS:
			usleep(1000);
			break;
		case HTTP_DELAY_03MS:
			usleep(300);
			break;			
		case HTTP_DELAY_0MS:
		default:
			break;
		}
		uint64 ts = getTimestamp();
		if (ts == (uint64)-1)
		{
			core_dump();
		}
		return ts;
	}	
#endif
}

void getltsShmemInit(void)
{
    bool found;
    lts_timestamp = (Lts_timestamp *)
        ShmemInitStruct("lts_timestamp", sizeof(Lts_timestamp), &found);

    if (!IsUnderPostmaster)
    {
        Assert(!found);
        lts_timestamp->lts = 0;
        SpinLockInit(&lts_timestamp->lts_lock);
    }
    else
    {
        Assert(found);
    }
}

Size getltsShmemSize(void)
{
    return sizeof(Lts_timestamp);
}
