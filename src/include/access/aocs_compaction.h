/*------------------------------------------------------------------------------
 *
 * aocs_compaction.h
 *
 * Copyright (c) 2013-Present Pivotal Software, Inc.
 *
 *
 * IDENTIFICATION
 *	    src/include/access/aocs_compaction.h
 *
 *------------------------------------------------------------------------------
 */
#ifndef AOCS_COMPACTION_H
#define AOCS_COMPACTION_H

#include "nodes/pg_list.h"
#include "utils/rel.h"

extern void AOCSDrop(Relation aorel,
		 List *compaction_segno);
extern void AOCSCompact(Relation aorel,
			List *compaction_segno_list,
			int insert_segno,
			bool isFull);
extern void AOCSTruncateToEOF(Relation aorel);
#endif
