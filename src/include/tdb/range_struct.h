/*----------------------------------
 *
 * range_struct.h
 *
 * Copyright (c) 2019-Present, TDSQL
 *
 * src/include/tdb/range_struct.h
 *----------------------------------
 */
#ifndef RANGE_STRUCT
#define RANGE_STRUCT

#include "c.h"
#include "tdb/kv_struct.h"

#define MAXRANGECOUNT (1000)
#define MAXSEGCOUNT (50)
#define DEFAULT_LOCATION_LEVEL (3)
#define MAX_RANGEDESC_SIZE (512)

typedef Oid RangeID;
typedef int32 SegmentID;
typedef uint32 ReplicaID;
typedef uint32 Version;
typedef uint32 KeySize;
typedef uint32 KeyCount;
typedef uint64 SegmentSize;

extern int RootRangeLevel;
extern RangeID rootRouteRangeID;
extern RangeID Rangeidvalue;
extern int DEFAULT_REPLICA_NUM;

typedef enum SystemRangeRelID
{
	ROOTRANGELEVEL,
	HOTRANGEROUTE,
	ROOTRANGEROUTE,
	META = 5,
	RANGEDESC,
	RANGEID,
    USER_DATA_START = 10000,
}SystemRangeRelID;

typedef enum ReplicaState
{
	replica_init,
	follow_online,
	leader_takeover,  /* no use */
	leader_prepareing,
	leader_working,

	split_prepare,
	split_barrier,

	merge_prepare,
	merge_barrier,

	follower_working, /* no use */
	follower_offline, /* no use */
	follower_remove  /* no use */
}ReplicaState;

typedef struct
{
	SegmentID segmentID;
	ReplicaID replicaID;
	ReplicaID LeaderReplicaID;
	ReplicaState replica_state;
} ReplicaDesc;

typedef ReplicaDesc* Replica;

typedef struct
{
	RangeID rangeID;
	TupleKeySlice startkey;
	TupleKeySlice endkey;
	Version version;
	int replica_num;
	Replica *replica;
} RangeDesc;

typedef struct
{
	RangeID rangeID;
	KeySize keybytes;
	KeyCount keycount;
	KeySize valuebytes;
	KeyCount valuecount;
	KeySize writeSize;
	KeySize readSize;
} RangeSatistics;

typedef struct
{
	char  address[10][50];
	Size  location_len[10];
	Size  location_level;
} seg_location;

/* This structure currently manages only the storage of rocksdb */
typedef struct
{
	SegmentID segmentID;
	/* Represents the location of the seg. */
	/*ps: country - city - mechanical room - server - disk */
	seg_location location;
	/* we can set the segment's max size, usually the max size is the disk capacity */
	SegmentSize totalSize;
	SegmentSize availableSize;
	SegmentSize userSize;
	KeyCount rangeCount;
    KeyCount leaderCount;
	KeySize readSize;
	KeySize writeSize;
} SegmentSatistics;

typedef struct
{
	bool statistics_init;
	RangeSatistics range_statistics[MAXRANGECOUNT];
	Size range_count;
	SegmentSatistics seg_statistics[MAXSEGCOUNT];
	Size seg_count;
    char ip[MAXSEGCOUNT][20];
} SSM_Statistics;

typedef struct
{
	int  range_count;
	char buffer[MAXRANGECOUNT][MAX_RANGEDESC_SIZE];	/* every ceil store a Range */
	Size rangeBufferSize[MAXRANGECOUNT];
} RangeList;

extern RangeList *ssm_rangelist;
extern SSM_Statistics *ssm_statistics;

#endif
