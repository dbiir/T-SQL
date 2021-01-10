#include "postgres.h"

#include "funcapi.h"
#include "miscadmin.h"

#include "access/heapam.h"
#include "access/genam.h"
#include "catalog/indexing.h"
#include "cdb/cdbdisp_query.h"
#include "cdb/cdbvars.h"
#include "libpq/ip.h"
#include "libpq-fe.h"
#include "postmaster/postmaster.h"
#include "utils/builtins.h"
#include "utils/faultinjector.h"
#include "utils/fmgroids.h"
#include "utils/snapmgr.h"

PG_MODULE_MAGIC;

extern Datum gp_inject_fault(PG_FUNCTION_ARGS);

static void
getHostnameAndPort(int dbid, char **hostname, int *port)
{
	HeapTuple	tuple;
	Relation    configrel;
	ScanKeyData scankey[1];
	SysScanDesc scan;
	Datum       attr;
	bool        isNull;

	configrel = heap_open(GpSegmentConfigRelationId, AccessShareLock);
	ScanKeyInit(&scankey[0],
				Anum_gp_segment_configuration_dbid,
				BTEqualStrategyNumber, F_INT2EQ,
				Int16GetDatum(dbid));
	scan = systable_beginscan(configrel, GpSegmentConfigDbidIndexId, true,
							  NULL, 1, scankey);

	tuple = systable_getnext(scan);

	if (HeapTupleIsValid(tuple))
	{
		attr = heap_getattr(tuple, Anum_gp_segment_configuration_hostname,
							RelationGetDescr(configrel), &isNull);
		Assert(!isNull);
		*hostname = TextDatumGetCString(attr);

		attr = heap_getattr(tuple, Anum_gp_segment_configuration_port,
							RelationGetDescr(configrel), &isNull);
		Assert(!isNull);
		*port = DatumGetInt16(attr);
	}
	else
		elog(ERROR, "dbid %d not found", dbid);

	systable_endscan(scan);
	heap_close(configrel, NoLock);
}

PG_FUNCTION_INFO_V1(gp_inject_fault);
Datum
gp_inject_fault(PG_FUNCTION_ARGS)
{
	char	*faultName = TextDatumGetCString(PG_GETARG_DATUM(0));
	char	*type = TextDatumGetCString(PG_GETARG_DATUM(1));
	char	*ddlStatement = TextDatumGetCString(PG_GETARG_DATUM(2));
	char	*databaseName = TextDatumGetCString(PG_GETARG_DATUM(3));
	char	*tableName = TextDatumGetCString(PG_GETARG_DATUM(4));
	int		startOccurrence = PG_GETARG_INT32(5);
	int		endOccurrence = PG_GETARG_INT32(6);
	int		extraArg = PG_GETARG_INT32(7);
	int		dbid = PG_GETARG_INT32(8);
	char	*hostname;
	int		port;
	char	*response;


	/* Fast path if injecting fault in our postmaster. */
	if (GpIdentity.dbid == dbid)
	{
		response = InjectFault(
			faultName, type, ddlStatement, databaseName,
			tableName, startOccurrence, endOccurrence, extraArg);
		if (!response)
			elog(ERROR, "failed to inject fault locally (dbid %d)", dbid);
		if (strncmp(response, "Success:",  strlen("Success:")) != 0)
			elog(ERROR, "%s", response);
	}
	else
	{
		char conninfo[1024];
		char msg[1024];
		PGconn *conn;
		PGresult *res;

		getHostnameAndPort(dbid, &hostname, &port);
		snprintf(conninfo, 1024, "host=%s port=%d %s=%s",
				 hostname, port, GPCONN_TYPE, GPCONN_TYPE_FAULT);
		conn = PQconnectdb(conninfo);
		if (PQstatus(conn) != CONNECTION_OK)
			elog(ERROR, "connection to dbid %d %s:%d failed", dbid, hostname, port);

		/*
		 * If ddl, dbname or tablename is not specified, send '#' instead.
		 * This allows sscanf to be used on the receiving end to parse the
		 * message.
		 */
		if (!ddlStatement || ddlStatement[0] == '\0')
			ddlStatement = "#";
		if (!databaseName || databaseName[0] == '\0')
			databaseName = "#";
		if (!tableName || tableName[0] == '\0')
			tableName = "#";
		snprintf(msg, 1024, "faultname=%s type=%s ddl=%s db=%s table=%s "
				 "start=%d end=%d extra=%d",
				 faultName, type,
				 ddlStatement,
				 databaseName,
				 tableName,
				 startOccurrence,
				 endOccurrence,
				 extraArg);
		res = PQexec(conn, msg);
		if (PQresultStatus(res) != PGRES_TUPLES_OK)
			elog(ERROR, "failed to inject fault: %s", PQerrorMessage(conn));

		if (PQntuples(res) != 1)
		{
			PQclear(res);
			PQfinish(conn);
			elog(ERROR, "invalid response from %s:%d", hostname, port);
		}

		response = PQgetvalue(res, 0, Anum_fault_message_response_status);
		if (strncmp(response, "Success:",  strlen("Success:")) != 0)
		{
			PQclear(res);
			PQfinish(conn);
			elog(ERROR, "%s", response);
		}

		PQclear(res);
		PQfinish(conn);
	}
	PG_RETURN_TEXT_P(cstring_to_text(response));
}
