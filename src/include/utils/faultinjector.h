/*
 *  faultinjector.h
 *  
 *
 *  Copyright 2009-2010, Greenplum Inc. All rights reserved.
 *
 */

#ifndef FAULTINJECTOR_H
#define FAULTINJECTOR_H

#include "pg_config_manual.h"

#define FAULTINJECTOR_MAX_SLOTS	16

#define FAULT_NAME_MAX_LENGTH	256

#define INFINITE_END_OCCURRENCE -1

#define Natts_fault_message_response 1
#define Anum_fault_message_response_status 0

/* Fault name that matches all faults */
#define FaultInjectorNameAll "all"

typedef enum FaultInjectorType_e {
#define FI_TYPE(id, str) id,
#include "utils/faultinjector_lists.h"
#undef FI_TYPE
	FaultInjectorTypeMax
} FaultInjectorType_e;

/*
 *
 */
typedef enum DDLStatement_e {
#define FI_DDL_STATEMENT(id, str) id,
#include "utils/faultinjector_lists.h"
#undef FI_DDL_STATEMENT
	DDLMax
} DDLStatement_e;

/*
 *
 */
typedef enum FaultInjectorState_e {
#define FI_STATE(id, str) id,
#include "utils/faultinjector_lists.h"
#undef FI_STATE
	FaultInjectorStateMax
} FaultInjectorState_e;


/*
 *
 */
typedef struct FaultInjectorEntry_s {
	
	char						faultName[FAULT_NAME_MAX_LENGTH];

	FaultInjectorType_e		faultInjectorType;
	
	int						extraArg;
		/* in seconds, in use if fault injection type is sleep */
		
	DDLStatement_e			ddlStatement;
	
	char					databaseName[NAMEDATALEN];
	
	char					tableName[NAMEDATALEN];
	
	volatile	int			startOccurrence;
	volatile	int			endOccurrence;
	volatile	 int	numTimesTriggered;
	volatile	FaultInjectorState_e	faultInjectorState;

		/* the state of the fault injection */
	char					bufOutput[2500];
	
} FaultInjectorEntry_s;


extern Size FaultInjector_ShmemSize(void);

extern void FaultInjector_ShmemInit(void);

extern FaultInjectorType_e FaultInjector_InjectFaultIfSet(
							   const char*				 faultName,
							   DDLStatement_e			 ddlStatement,
							   const char*				 databaseName,
							   const char*				 tableName);

extern char *InjectFault(
	char *faultName, char *type, char *ddlStatement, char *databaseName,
	char *tableName, int startOccurrence, int endOccurrence, int extraArg);

extern void HandleFaultMessage(const char* msg);

#ifdef FAULT_INJECTOR
extern bool am_faulthandler;
#define IsFaultHandler am_faulthandler
#define SIMPLE_FAULT_INJECTOR(FaultName) \
	FaultInjector_InjectFaultIfSet(FaultName, DDLNotSpecified, "", "")
#else
#define IsFaultHandler false
#define SIMPLE_FAULT_INJECTOR(FaultName)
#endif

#endif	/* FAULTINJECTOR_H */
