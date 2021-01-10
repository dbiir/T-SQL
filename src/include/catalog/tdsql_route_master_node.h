/*-------------------------------------------------------------------------
 *
 * tdsql_route_master_node.h
 *	  definition of the system "TDSQL master node" relation
 *
 *
 * Portions Copyright (c) 2018- TDSQL
 *
 * src/include/catalog/tdsql_route_master_node.h
 *
 * NOTES
 *    the genbki.pl script reads this file and generates .bki
 *    information from the DATA() statements.
 *  
 *-------------------------------------------------------------------------
 */
#ifndef TDSQL_ROUTE_MASTER_NODE_H
#define TDSQL_ROUTE_MASTER_NODE_H

#include "catalog/genbki.h"

#define TdsqlRouteMasterNodeRelationId  9000

CATALOG(tdsql_route_master_node,9000) BKI_SHARED_RELATION
{
	/*
	 * Node identifier to be used at places where a fixed length node identification is required
	 */
	int32		master_id;

	/*
	 * Node identifier to be used at places where a fixed length node identification is required
	 */
	NameData	master_name;
	/*
	 * Port number of the node to connect to
	 */
	int32		master_port;
	
	/*
	 * Host name of IP address of the node to connect to
	 */
	NameData	master_host;

	/*
	 * Possible node types are defined as follows
	 * Types are defined below TDSQL_NODES_XXXX
	 */
	char		master_type;
	
	/*
	 * Rack id of this master node, positioning master node
	 */
	int32		master_rackid;
} FormData_tdsql_route_master_node;

typedef FormData_tdsql_route_master_node *Form_tdsql_route_master_node;

#define Natts_tdsql_route_master_node		6

#define Anum_tdsql_route_master_node_id		1
#define Anum_tdsql_route_master_node_name	2
#define Anum_tdsql_route_master_node_port	3
#define Anum_tdsql_route_master_node_host	4
#define Anum_tdsql_route_master_node_type	5
#define Anum_tdsql_route_master_node_rackid	6

/* Possible types of nodes */
#define TDSQL_ROUTE_MASTER_NODE_PRIMARY		'P'
#define TDSQL_ROUTE_MASTER_NODE_SECONDARY	'S'
#define TDSQL_ROUTE_MASTER_NODE_NONE		'N'

#endif   /* TDSQL_ROUTE_MASTER_NODE_H */

