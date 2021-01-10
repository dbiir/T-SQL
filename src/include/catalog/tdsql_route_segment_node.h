/*-------------------------------------------------------------------------
 *
 * tdsql_route_segment_node.h
 *	  definition of the system "TDSQL segment node" relation
 *
 *
 * Portions Copyright (c) 2018- TDSQL
 *
 * src/include/catalog/tdsql_route_segment_node.h
 *
 * NOTES
 *    the genbki.pl script reads this file and generates .bki
 *    information from the DATA() statements.
 *
 *-------------------------------------------------------------------------
 */
#ifndef TDSQL_ROUTE_SEGMENT_NODE_H
#define TDSQL_ROUTE_SEGMENT_NODE_H

#include "catalog/genbki.h"

#define TdsqlRouteSegmentNodeRelationId  9005

CATALOG(tdsql_route_segment_node,9005)
{
	/*
	 * Node identifier to be used at places where a fixed length node identification is required
	 */
	int32		segment_id;

	/*
	 * Node identifier to be used at places where a fixed length node identification is required
	 */
	NameData	segment_name;

	/*
	 * Port number of the node to connect to
	 */
	int32		segment_port;

	/*
	 * Host name of IP address of the node to connect to
	 */
	NameData	segment_host;

	/*
	 * Database name
	 */
	NameData	db_name;

	/*
	 * Database oid
	 */
	int32		db_oid;

	/*
	 * Table name
	 */
	NameData	table_name;

	/*
	 * Table oid
	 */
	int32		table_oid;

	/*
	 * Possible data distribution types, must be one of TDSQL_ROUTE_DISTRIBUTED_XX,
	 * ONLY TDSQL_ROUTE_DISTRIBUTED_LIST make sense
	 */
	char		distributed_type;

	/*
	 * Column defined in SQL with distributed by (xx)
	 */
	NameData	distributed_column;

	/*
	 * The value of distributed column, for example, distributed by (gender),
	 * the value can be male or female
	 */
	NameData	column_value;

	/*
	 * Rack id of this segment node, positioning segment node
	 */
	int32		segment_rackid;
} FormData_tdsql_route_segment_node;

typedef FormData_tdsql_route_segment_node *Form_tdsql_route_segment_node;

#define Natts_tdsql_route_segment_node				12

#define Anum_tdsql_route_segment_node_id			1
#define Anum_tdsql_route_segment_node_name			2
#define Anum_tdsql_route_segment_node_port			3
#define Anum_tdsql_route_segment_node_host			4
#define Anum_tdsql_route_segment_node_db_name			5
#define Anum_tdsql_route_segment_node_db_oid			6
#define Anum_tdsql_route_segment_node_table_name		7
#define Anum_tdsql_route_segment_node_table_oid			8
#define Anum_tdsql_route_segment_node_distributed_type		9
#define Anum_tdsql_route_segment_node_distributed_column	10
#define Anum_tdsql_route_segment_node_column_value		11
#define Anum_tdsql_route_segment_node_rackid			12

/* Possible types of nodes */
#define TDSQL_ROUTE_DISTRIBUTED_HASH				'H'
#define TDSQL_ROUTE_DISTRIBUTED_LIST				'L'
#define TDSQL_ROUTE_DISTRIBUTED_RANDOM				'R'
#define TDSQL_ROUTE_DISTRIBUTED_NONE				'N'


#endif   /* TDSQL_ROUTE_SEGMENT_NODE_H */

