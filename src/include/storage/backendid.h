/*-------------------------------------------------------------------------
 *
 * backendid.h
 *	  POSTGRES backend id communication definitions
 *
 *
 * Portions Copyright (c) 1996-2014, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/storage/backendid.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef BACKENDID_H
#define BACKENDID_H

/* ----------------
 *		-cim 8/17/90
 * ----------------
 */
typedef int BackendId;			/* unique currently active backend identifier */

#define InvalidBackendId		(-1)

/*
 * TempRelBackendId is used in GPDB in place of a real backend ID in some
 * places where we deal with a temporary tables.
 *
 * Future enhancement: To align closer with upstream, the TempRelBackendId
 * constant should be replaced with gp_session_id.  In order for it to work,
 * the BufferTag needs to be augmented with session ID so that we can get rid
 * of two further GPDB-specific hacks: (1) BM_TEMP flag added to distinguish
 * shared buffers belonging to temp relations from non-temp relations and (2)
 * the additional check for collition in GetNewRelFileNode.
 *
 * Based on this discussion adding gp_session_id to BufferTag is considered a
 * performance overhead:
 * http://www.postgresql-archive.org/Keeping-temporary-tables-in-shared-buffers-td6022361.html
 *
 * The advantage we would get is BufferTag can never hash to the same value for
 * a temp and a non-temp relation having the same RelFileNode.
 */
#define TempRelBackendId		(-2)

extern PGDLLIMPORT BackendId MyBackendId;		/* backend id of this backend */

#endif   /* BACKENDID_H */
