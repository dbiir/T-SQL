/*-------------------------------------------------------------------------
 *
 * kvindex.c
 *
 * NOTES
 *	  The function in this file is responsible for
 *    handling the index in rocksdb.
 *
 * Copyright (c) 2019-Present, TDSQL
 *
 * IDENTIFICATION
 *	  src/backend/access/kv/kvindex.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/heapam_xlog.h"
#include "access/nbtree.h"
#include "access/relscan.h"
#include "catalog/index.h"
#include "catalog/pg_namespace.h"
#include "commands/vacuum.h"
#include "storage/indexfsm.h"
#include "storage/ipc.h"
#include "storage/lmgr.h"
#include "storage/smgr.h"
#include "tcop/tcopprot.h"
#include "utils/memutils.h"
#include "utils/guc.h"

#include "catalog/indexing.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "storage/predicate.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "tdb/tdbkvam.h"
#include "pgstat.h"
#include "tdb/storage_param.h"

#include "tdb/historical_transfer.h"

static bool kvengine_get_index_start_key(IndexScanDesc scan, TupleKeySlice* start_keys, TupleKeySlice* end_keys, bool *isforward, bool *startequal, bool *endequal, bool *issecond);

static bool kvengine_get_index_history_start_key(IndexScanDesc scan, 
							GenericSlice* start_keys, GenericSlice* end_keys,
							bool *isforward, long xmin_ts, long xmax_ts);
/*
 *	kvengine_indexgetnext() -- Get the next tuple that satisfies the condition
 */
void
kvengine_indexgetnext(KVEngineScanDesc scan, TupleTableSlot* slot)
{
	kvengine_getnext(scan,ForwardScanDirection, slot);
}

KVEngineScanDesc
kvengine_index_begin_scan(IndexScanDesc indexscandesc)
{
	//RelationIncrementReferenceCount(indexscandesc->heapRelation);
	TupleKeySlice *startkey = palloc0(sizeof(TupleKeySlice));
	TupleKeySlice *endkey = palloc0(sizeof(TupleKeySlice));
	bool isforward = true;
	bool startequal = false;
	bool endequal = false;
	bool issecond = false;
	kvengine_get_index_start_key(indexscandesc, startkey, endkey, &isforward,
														&startequal, &endequal, &issecond);

	KVEngineScanDesc indexscan = kvengine_beginscan_by_key(indexscandesc->heapRelation,
								indexscandesc->indexRelation->rd_id,
								indexscandesc->xs_snapshot, startkey, endkey, isforward);
	indexscan->issecond = issecond;
	return indexscan;
}

bool
kvengine_get_index_start_key(IndexScanDesc scan,
							TupleKeySlice* start_keys, TupleKeySlice* end_keys,
							bool *isforward, bool *startequal, bool *endequal, bool *issecond)
{
	Relation	rel = scan->indexRelation;
	Relation 	rocksrel = scan->heapRelation;
	BTScanOpaque so = (BTScanOpaque) scan->opaque;
	bool		goback;
	bool		leftequal = false;
	bool		rightequal = false;
	ScanKeyData notnullkeys[INDEX_MAX_KEYS];
	int			keysCount = 0;
	int			i;
	ScanKey		greaterKeys[INDEX_MAX_KEYS];
	ScanKey		lessKeys[INDEX_MAX_KEYS];
	int			greaterkeysCount = 0;
	int			lesskeysCount = 0;
	StrategyNumber strat_total;
	ScanDirection dir = ForwardScanDirection;

	pgstat_count_index_scan(rel);

	/*
	 * Examine the scan keys and eliminate any redundant keys; also mark the
	 * keys that must be matched to continue the scan.
	 */
	_bt_preprocess_keys(scan);

	/*
	 * Quit now if _bt_preprocess_keys() discovered that the scan keys can
	 * never be satisfied (eg, x == 1 AND x > 2).
	 */
	if (!so->qual_ok)
		return false;

	/*----------
	 * Examine the scan keys to discover where we need to start the scan.
	 *
	 * We want to identify the keys that can be used as starting boundaries;
	 * these are =, >, or >= keys for a forward scan or =, <, <= keys for
	 * a backwards scan.  We can use keys for multiple attributes so long as
	 * the prior attributes had only =, >= (resp. =, <=) keys.  Once we accept
	 * a > or < boundary or find an attribute with no boundary (which can be
	 * thought of as the same as "> -infinity"), we can't use keys for any
	 * attributes to its right, because it would break our simplistic notion
	 * of what initial positioning strategy to use.
	 *
	 * When the scan keys include cross-type operators, _bt_preprocess_keys
	 * may not be able to eliminate redundant keys; in such cases we will
	 * arbitrarily pick a usable one for each attribute.  This is correct
	 * but possibly not optimal behavior.  (For example, with keys like
	 * "x >= 4 AND x >= 5" we would elect to scan starting at x=4 when
	 * x=5 would be more efficient.)  Since the situation only arises given
	 * a poorly-worded query plus an incomplete opfamily, live with it.
	 *
	 * When both equality and inequality keys appear for a single attribute
	 * (again, only possible when cross-type operators appear), we *must*
	 * select one of the equality keys for the starting point, because
	 * _bt_checkkeys() will stop the scan as soon as an equality qual fails.
	 * For example, if we have keys like "x >= 4 AND x = 10" and we elect to
	 * start at x=4, we will fail and stop before reaching x=10.  If multiple
	 * equality quals survive preprocessing, however, it doesn't matter which
	 * one we use --- by definition, they are either redundant or
	 * contradictory.
	 *
	 * Any regular (not SK_SEARCHNULL) key implies a NOT NULL qualifier.
	 * If the index stores nulls at the end of the index we'll be starting
	 * from, and we have no boundary key for the column (which means the key
	 * we deduced NOT NULL from is an inequality key that constrains the other
	 * end of the index), then we cons up an explicit SK_SEARCHNOTNULL key to
	 * use as a boundary key.  If we didn't do this, we might find ourselves
	 * traversing a lot of null entries at the start of the scan.
	 *
	 * In this loop, row-comparison keys are treated the same as keys on their
	 * first (leftmost) columns.  We'll add on lower-order columns of the row
	 * comparison below, if possible.
	 *
	 * The selected scan keys (at most one per index column) are remembered by
	 * storing their addresses into the local startKeys[] array.
	 *----------
	 */
	strat_total = BTEqualStrategyNumber;
	if (so->numberOfKeys > 0)
	{
		AttrNumber	curattr;
		ScanKey		chosen;
		ScanKey 	greaterkey;
		ScanKey 	equalkey;
		ScanKey 	lesskey;
		ScanKey		impliesNN;
		ScanKey		cur;

		/*
		 * chosen is the so-far-chosen key for the current attribute, if any.
		 * We don't cast the decision in stone until we reach keys for the
		 * next attribute.
		 */
		curattr = 1;
		chosen = NULL;
		greaterkey = NULL;
		lesskey = NULL;
		equalkey = NULL;
		/* Also remember any scankey that implies a NOT NULL constraint */
		impliesNN = NULL;

		/*
		 * Loop iterates from 0 to numberOfKeys inclusive; we use the last
		 * pass to handle after-last-key processing.  Actual exit from the
		 * loop is at one of the "break" statements below.
		 */
		for (cur = so->keyData, i = 0;; cur++, i++)
		{
			if (i >= so->numberOfKeys || cur->sk_attno != curattr)
			{
				/*
				 * Done looking at keys for curattr.  If we didn't find a
				 * usable boundary key, see if we can deduce a NOT NULL key.
				 */
				if (greaterkey == NULL && equalkey == NULL && lesskey == NULL &&
					impliesNN != NULL &&
					((impliesNN->sk_flags & SK_BT_NULLS_FIRST) ?
					 ScanDirectionIsForward(dir) :
					 ScanDirectionIsBackward(dir)))
				{
					/* Yes, so build the key in notnullkeys[keysCount] */
					chosen = &notnullkeys[keysCount];
					ScanKeyEntryInitialize(chosen,
										   (SK_SEARCHNOTNULL | SK_ISNULL |
											(impliesNN->sk_flags &
										  (SK_BT_DESC | SK_BT_NULLS_FIRST))),
										   curattr,
								 ((impliesNN->sk_flags & SK_BT_NULLS_FIRST) ?
								  BTGreaterStrategyNumber :
								  BTLessStrategyNumber),
										   InvalidOid,
										   InvalidOid,
										   InvalidOid,
										   (Datum) 0);
				}

				/*
				 * If we still didn't find a usable boundary key, quit; else
				 * save the boundary key pointer in startKeys.
				 */
				if (equalkey == NULL && lesskey ==NULL && greaterkey ==NULL)
					break;
				if (equalkey != NULL)
				{
					lesskey = NULL;
					greaterkey = NULL;
					lessKeys[lesskeysCount++] = equalkey;
					greaterKeys[greaterkeysCount++] = equalkey;
				}
				if (lesskey != NULL)
				{
					lessKeys[lesskeysCount++] = lesskey;
				}
				if (greaterkey != NULL)
				{
					greaterKeys[greaterkeysCount++] = greaterkey;
				}

				/*
				 * Done if that was the last attribute, or if next key is not
				 * in sequence (implying no boundary key is available for the
				 * next attribute).
				 */
				if (i >= so->numberOfKeys ||
					cur->sk_attno != curattr + 1)
					break;

				/*
				 * Reset for next attr.
				 */
				curattr = cur->sk_attno;
				chosen = NULL;
				impliesNN = NULL;
				lesskey = NULL;
				greaterkey = NULL;
				equalkey = NULL;
			}

			/*
			 * Can we use this key as a starting boundary for this attr?
			 *
			 * If not, does it imply a NOT NULL constraint?  (Because
			 * SK_SEARCHNULL keys are always assigned BTEqualStrategyNumber,
			 * *any* inequality key works for that; we need not test.)
			 */
			switch (cur->sk_strategy)
			{
				case BTLessEqualStrategyNumber:
					rightequal = true;
				case BTLessStrategyNumber:
					if (lesskey == NULL)
					{
						lesskey = cur;
						if (ScanDirectionIsBackward(dir))
							lesskey = cur;
						else
							impliesNN = cur;
					}
					break;
				case BTEqualStrategyNumber:
					/* override any non-equality choice */
					equalkey = cur;
					break;
				case BTGreaterEqualStrategyNumber:
					leftequal = true;
				case BTGreaterStrategyNumber:
					if (greaterkey == NULL)
					{
						greaterkey = cur;
						if (ScanDirectionIsForward(dir))
							greaterkey = cur;
						else
							impliesNN = cur;
					}
					break;
			}
		}
	}

	/*
	 * If we found no usable boundary keys, we have to start from one end of
	 * the tree.  Walk down that edge to the first or last key, and scan from
	 * there.
	 */
	/*
	 * In KV engine, if we found no usable boundary keys, we can generate full
	 * table startkey and endkey.
	 */
	if (greaterkeysCount == 0 && lesskeysCount == 0)
		goback = false;

	/*----------
	 * Examine the selected initial-positioning strategy to determine exactly
	 * where we need to start the scan, and set flag variables to control the
	 * code below.
	 *
	 * If nextkey = false, _bt_search and _bt_binsrch will locate the first
	 * item >= scan key.  If nextkey = true, they will locate the first
	 * item > scan key.
	 *
	 * If goback = true, we will then step back one item, while if
	 * goback = false, we will start the scan on the located item.
	 *----------
	 */
	else if (greaterkeysCount == 0)
	{
		goback = true;
	}
	else if (lesskeysCount == 0)
	{
		goback = false;
	}
	else
	{
		// if (transam_mode == TRANSAM_MODE_BOCC ||
		// 	transam_mode == TRANSAM_MODE_OCC/* ||
		// 	consistency_mode == CONSISTENCY_MODE_CAUSAL*/)
		// 	goback = false;
		// else
		if (rocksdb_scan_foward == ROCKSDBSCAN_FORWARD)
			goback = false;
		else
			goback = true;
	}
	
	*isforward = !goback;

	if (!goback)
	{
		*startequal = leftequal;
		*endequal = rightequal;
	}
	else
	{
		*startequal = rightequal;
		*endequal = leftequal;
	}

	/* Determine whether it is the primary key */
	int pk_natts = 0;
	Oid pk_oid = get_pk_oid(rocksrel);
	int *pk_colnos = get_index_colnos(pk_oid, &pk_natts);
	Relation pkrel = relation_open(pk_oid, AccessShareLock);

	InitKeyDesc initstartkey;
    switch (transaction_type)
    {
    case KVENGINE_ROCKSDB:
        initstartkey = init_basis_in_keydesc(KEY_WITH_XMINCMIN);
        break;
    case KVENGINE_TRANSACTIONDB:
        initstartkey = init_basis_in_keydesc(RAW_KEY);
        break;
    default:
        break;
    }
	initstartkey.rel_id = rocksrel->rd_id;
	if (greaterkeysCount != 0)
	{
		initstartkey.isend = false;
		if (rel->rd_id != pk_oid)
		{
			*issecond = true;
			initstartkey.init_type = SECONDE_KEY;
			Datum* pk_value = palloc0(mul_size(sizeof(Datum), pk_natts));
			bool *pkisnull = palloc0(mul_size(sizeof(bool), pk_natts));
			for(int i = 0; i < pk_natts; i++)
			{
				pk_value[i] = 0;
				pkisnull[i] = true;
			}
			init_pk_in_keydesc(&initstartkey, pk_oid, get_attr_type(pkrel, pk_colnos, pk_natts), pk_value, pkisnull, pk_natts);

			Datum *second_value = palloc0(mul_size(sizeof(Datum), greaterkeysCount));
			int *second_cols = palloc0(mul_size(sizeof(int), greaterkeysCount));
			bool *secondisnull = palloc0(mul_size(sizeof(bool), greaterkeysCount));

			for(int i = 0; i < greaterkeysCount; i++)
			{
				second_cols[i] = greaterKeys[i]->sk_attno;
				second_value[i] = greaterKeys[i]->sk_argument;
				secondisnull[i] = false;
			}
			init_sk_in_keydesc(&initstartkey, rel->rd_id, get_attr_type(rel, second_cols, greaterkeysCount), second_value, secondisnull, greaterkeysCount);

			TupleKeySlice startKeyArray = build_key(initstartkey);
			start_keys->data = startKeyArray.data;
            start_keys->len = startKeyArray.len;
            if (startKeyArray.len > MAX_SLICE_LEN)
            {
                core_dump();
            }

			pfree(second_value);
			pfree(second_cols);
			pfree(secondisnull);
			pfree(pk_value);
			pfree(pkisnull);
		}
		else
		{
			initstartkey.init_type = PRIMARY_KEY;
			Datum *primary_value = palloc0(mul_size(sizeof(Datum), greaterkeysCount));
			int *primary_cols = palloc0(mul_size(sizeof(int), greaterkeysCount));
			bool *primaryisnull = palloc0(mul_size(sizeof(bool), greaterkeysCount));

			for(int i = 0; i < greaterkeysCount; i++)
			{
				primary_cols[i] = greaterKeys[i]->sk_attno;
				primary_value[i] = greaterKeys[i]->sk_argument;
				primaryisnull[i] = false;
			}
			init_pk_in_keydesc(&initstartkey, rel->rd_id, get_attr_type(rel, primary_cols, greaterkeysCount), primary_value, primaryisnull, greaterkeysCount);

			TupleKeySlice startKeyArray = build_key(initstartkey);       
            if (startKeyArray.len > MAX_SLICE_LEN)
            {
                core_dump();
            }

			start_keys->data = startKeyArray.data;
            start_keys->len = startKeyArray.len;
			pfree(primary_value);
			pfree(primary_cols);
			pfree(primaryisnull);
		}
	}
	else
	{
		initstartkey.isend = false;
		initstartkey.init_type = VALUE_NULL_KEY;
		initstartkey.pk_id = rel->rd_id;
		TupleKeySlice startKeyArray = build_key(initstartkey);   
        if (startKeyArray.len > MAX_SLICE_LEN)
        {
            core_dump();
        }

        start_keys->data = startKeyArray.data;
        start_keys->len = startKeyArray.len;
	}
	if (lesskeysCount != 0)
	{
		initstartkey.isend = true;
		if (rel->rd_id != pk_oid)
		{
			*issecond = true;
			initstartkey.init_type = SECONDE_KEY;
			initstartkey.cid = 0xffff;
			initstartkey.xid = 0xffff;
			Datum* pk_value;
			bool *pkisnull;
			pk_value = palloc0(mul_size(sizeof(Datum), pk_natts));
			pkisnull = palloc0(mul_size(sizeof(bool), pk_natts));
			for(int i = 0; i < pk_natts; i++)
			{
				pk_value[i] = 0;
				pkisnull[i] = true;
			}
			init_pk_in_keydesc(&initstartkey, pk_oid, get_attr_type(pkrel, pk_colnos, pk_natts), pk_value, pkisnull, pk_natts);

			Datum *second_value = palloc0(mul_size(sizeof(Datum), lesskeysCount));
			int *second_cols = palloc0(mul_size(sizeof(int), lesskeysCount));
			bool *secondisnull = palloc0(mul_size(sizeof(bool), lesskeysCount));
			for(int i = 0; i < lesskeysCount; i++)
			{
				second_cols[i] = lessKeys[i]->sk_attno;
				second_value[i] = lessKeys[i]->sk_argument;
				secondisnull[i] = false;
			}
			init_sk_in_keydesc(&initstartkey, rel->rd_id, get_attr_type(rel, second_cols, lesskeysCount), second_value, secondisnull, lesskeysCount);

			TupleKeySlice endKeyArray = build_key(initstartkey);
            if (endKeyArray.len > MAX_SLICE_LEN)
            {
                core_dump();
            }

        	end_keys->data = endKeyArray.data;
            end_keys->len = endKeyArray.len;
			pfree(second_value);
			pfree(second_cols);
			pfree(secondisnull);
			pfree(pk_value);
			pfree(pkisnull);
		}
		else
		{
			initstartkey.init_type = PRIMARY_KEY;
			initstartkey.cid = 0xffff;
			initstartkey.xid = 0xffff;
			Datum *primary_value = palloc0(mul_size(sizeof(Datum), lesskeysCount));
			int *primary_cols = palloc0(mul_size(sizeof(int), lesskeysCount));
			bool *primaryisnull = palloc0(mul_size(sizeof(bool), lesskeysCount));

			for(int i = 0; i < lesskeysCount; i++)
			{
				primary_cols[i] = lessKeys[i]->sk_attno;
				primary_value[i] = lessKeys[i]->sk_argument;
				primaryisnull[i] = false;
			}
			init_pk_in_keydesc(&initstartkey, rel->rd_id, get_attr_type(rel, primary_cols, lesskeysCount), primary_value, primaryisnull, lesskeysCount);

			TupleKeySlice endKeyArray = build_key(initstartkey);
            if (endKeyArray.len > MAX_SLICE_LEN)
            {
                core_dump();
            }

        	end_keys->data = endKeyArray.data;
            end_keys->len = endKeyArray.len;
			pfree(primary_value);
			pfree(primary_cols);
			pfree(primaryisnull);
		}
	}
	else
	{
		initstartkey.isend = true;
		initstartkey.init_type = VALUE_NULL_KEY;
		initstartkey.pk_id = rel->rd_id;
		TupleKeySlice endKeyArray = build_key(initstartkey);
        if (endKeyArray.len > MAX_SLICE_LEN)
        {
            core_dump();
        }

        end_keys->data = endKeyArray.data;
        end_keys->len = endKeyArray.len;
	}
	relation_close(pkrel, AccessShareLock);

	return true;
}

bool
kv_index_insert(Relation indexRelation,
			 TupleTableSlot *slot,
			 ItemPointer heap_t_ctid,
			 Relation rocksRelation,
			 IndexUniqueCheck checkUnique)
{
	Oid pk_index_oid = get_pk_oid(rocksRelation);
	if (pk_index_oid == indexRelation->rd_id)
		return true;
	if (!OidIsValid(pk_index_oid))
		return false;
	bool ischeckUnique = false;
	if (checkUnique != UNIQUE_CHECK_NO)
	{
		ischeckUnique = true;
	}
	KVEngineInsertDesc indexinsertdesc = kvengine_insert_init(rocksRelation);

	bool success = kvengine_index_insert(indexinsertdesc, slot, indexRelation->rd_id, ischeckUnique);

	kvengine_insert_finish(indexinsertdesc);

	return success;
}

KVEngineScanDesc
kvengine_history_beginscan(IndexScanDesc indexscandesc, long xmin_ts, long xmax_ts)
{
	RelationIncrementReferenceCount(indexscandesc->heapRelation);
	/* Alloc and initial scan description. */
	KVEngineScanDesc scan = palloc0(sizeof(*scan));
	scan->rks_rel = indexscandesc->heapRelation;
	scan->time = time;
	scan->scan_index = 0;
	scan->scan_num = 0;

	GenericSlice startkey = {0};
	GenericSlice endkey = {0};
	bool isforward = true;
	kvengine_get_index_history_start_key(indexscandesc, &startkey, &endkey, &isforward, xmin_ts, xmax_ts);
	scan->isforward = isforward;
	initial_next_key(scan, (TupleKeySlice*) &startkey);
	initial_end_key(scan, (TupleKeySlice*) &endkey);

	return scan;
}

bool 
kvengine_get_index_history_start_key(IndexScanDesc scan, 
							GenericSlice* start_keys, GenericSlice* end_keys,
							bool *isforward, long xmin_ts, long xmax_ts)
{
	Relation	rel = scan->indexRelation;
	Relation 	rocksrel = scan->heapRelation;
	BTScanOpaque so = (BTScanOpaque) scan->opaque;
	bool		goback;
	ScanKeyData notnullkeys[INDEX_MAX_KEYS];
	int			keysCount = 0;
	int			i;
	ScanKey		greaterKeys[INDEX_MAX_KEYS];
	ScanKey		lessKeys[INDEX_MAX_KEYS];	
	int			greaterkeysCount = 0;
	int			lesskeysCount = 0;
	StrategyNumber strat_total;
	ScanDirection dir = ForwardScanDirection;

	pgstat_count_index_scan(rel);

	/*
	 * Examine the scan keys and eliminate any redundant keys; also mark the
	 * keys that must be matched to continue the scan.
	 */
	_bt_preprocess_keys(scan);

	/*
	 * Quit now if _bt_preprocess_keys() discovered that the scan keys can
	 * never be satisfied (eg, x == 1 AND x > 2).
	 */
	if (!so->qual_ok)
		return false;

	strat_total = BTEqualStrategyNumber;
	if (so->numberOfKeys > 0)
	{
		AttrNumber	curattr;
		ScanKey		chosen;
		ScanKey 	greaterkey;
		ScanKey 	equalkey;
		ScanKey 	lesskey;
		ScanKey		impliesNN;
		ScanKey		cur;

		/*
		 * chosen is the so-far-chosen key for the current attribute, if any.
		 * We don't cast the decision in stone until we reach keys for the
		 * next attribute.
		 */
		curattr = 1;
		chosen = NULL;
		greaterkey = NULL;
		lesskey = NULL;
		equalkey = NULL;
		/* Also remember any scankey that implies a NOT NULL constraint */
		impliesNN = NULL;

		/*
		 * Loop iterates from 0 to numberOfKeys inclusive; we use the last
		 * pass to handle after-last-key processing.  Actual exit from the
		 * loop is at one of the "break" statements below.
		 */
		for (cur = so->keyData, i = 0;; cur++, i++)
		{
			if (i >= so->numberOfKeys || cur->sk_attno != curattr)
			{
				/*
				 * Done looking at keys for curattr.  If we didn't find a
				 * usable boundary key, see if we can deduce a NOT NULL key.
				 */
				if (greaterkey == NULL && equalkey == NULL && lesskey == NULL &&
					impliesNN != NULL &&
					((impliesNN->sk_flags & SK_BT_NULLS_FIRST) ?
					 ScanDirectionIsForward(dir) :
					 ScanDirectionIsBackward(dir)))
				{
					/* Yes, so build the key in notnullkeys[keysCount] */
					chosen = &notnullkeys[keysCount];
					ScanKeyEntryInitialize(chosen,
										   (SK_SEARCHNOTNULL | SK_ISNULL |
											(impliesNN->sk_flags &
										  (SK_BT_DESC | SK_BT_NULLS_FIRST))),
										   curattr,
								 ((impliesNN->sk_flags & SK_BT_NULLS_FIRST) ?
								  BTGreaterStrategyNumber :
								  BTLessStrategyNumber),
										   InvalidOid,
										   InvalidOid,
										   InvalidOid,
										   (Datum) 0);
				}

				/*
				 * If we still didn't find a usable boundary key, quit; else
				 * save the boundary key pointer in startKeys.
				 */
				if (equalkey == NULL && lesskey ==NULL && greaterkey ==NULL)
					break;
				if (equalkey != NULL)
				{
					lesskey = NULL;
					greaterkey = NULL;
					lessKeys[lesskeysCount++] = equalkey;
					greaterKeys[greaterkeysCount++] = equalkey;
				}
				if (lesskey != NULL)
				{
					lessKeys[lesskeysCount++] = lesskey;
				}
				if (greaterkey != NULL)
				{
					greaterKeys[greaterkeysCount++] = greaterkey;
				}

				/*
				 * Done if that was the last attribute, or if next key is not
				 * in sequence (implying no boundary key is available for the
				 * next attribute).
				 */
				if (i >= so->numberOfKeys ||
					cur->sk_attno != curattr + 1)
					break;

				/*
				 * Reset for next attr.
				 */
				curattr = cur->sk_attno;
				chosen = NULL;
				impliesNN = NULL;
				lesskey = NULL;
				greaterkey = NULL;
				equalkey = NULL;
			}

			/*
			 * Can we use this key as a starting boundary for this attr?
			 *
			 * If not, does it imply a NOT NULL constraint?  (Because
			 * SK_SEARCHNULL keys are always assigned BTEqualStrategyNumber,
			 * *any* inequality key works for that; we need not test.)
			 */
			switch (cur->sk_strategy)
			{
				case BTLessStrategyNumber:
				case BTLessEqualStrategyNumber:
					if (lesskey == NULL)
					{
						lesskey = cur;
						if (ScanDirectionIsBackward(dir))
							lesskey = cur;
						else
							impliesNN = cur;
					}
					break;
				case BTEqualStrategyNumber:
					/* override any non-equality choice */
					equalkey = cur;
					break;
				case BTGreaterEqualStrategyNumber:
				case BTGreaterStrategyNumber:
					if (greaterkey == NULL)
					{
						greaterkey = cur;
						if (ScanDirectionIsForward(dir))
							greaterkey = cur;
						else
							impliesNN = cur;
					}
					break;
			}
		}
	}


	if (greaterkeysCount == 0 && lesskeysCount == 0)
		return false;

	else if (greaterkeysCount == 0)
	{
		goback = true;
	}
	else if (lesskeysCount == 0)
	{
		goback = false;
	}
	else
	{
		goback = false;
	}
	*isforward = !goback;

	/* Determine whether it is the primary key */
	int pk_natts = 0;
	Oid pk_oid = get_pk_oid(rocksrel);
	Assert(rel->rd_id == pk_oid);
	int *pk_colnos = get_index_colnos(pk_oid, &pk_natts);

	int *cols;
	Datum* pk_value;
	bool *isnull;
	if (greaterkeysCount != 0)
	{
		cols = palloc0(mul_size(sizeof(int), greaterkeysCount)); 
		pk_value = palloc0(mul_size(sizeof(Datum), greaterkeysCount));
		isnull = palloc0(mul_size(sizeof(bool), greaterkeysCount));
		memset(isnull, 1, mul_size(sizeof(bool), greaterkeysCount));
		for(int i = 0; i < greaterkeysCount; i++)
		{
			cols[i] = greaterKeys[i]->sk_attno;
			pk_value[i] = greaterKeys[i]->sk_argument;
			isnull[i] = false;
		}
		GenericSlice startKeyArray = encode_comp_scan_historical_key(rocksrel, pk_value, isnull, cols, greaterkeysCount, 0, 0, false);
		*start_keys = startKeyArray;
		
		pfree(cols);
		pfree(pk_value);
	}
	else
	{
		GenericSlice startKeyArray = encode_comp_scan_historical_key(rocksrel, NULL, NULL, NULL, 0, 0, 0, false);
		*start_keys = startKeyArray;
	}
	if (lesskeysCount != 0)
	{
		cols = palloc0(mul_size(sizeof(int), lesskeysCount)); 
		pk_value = palloc0(mul_size(sizeof(Datum), lesskeysCount));
		isnull = palloc0(mul_size(sizeof(bool), lesskeysCount));
		for(int i = 0; i < lesskeysCount; i++)
		{
			cols[i] = lessKeys[i]->sk_attno;
			pk_value[i] = lessKeys[i]->sk_argument;
			isnull[i] = false;
		}
		GenericSlice endKeyArray = encode_comp_scan_historical_key(rocksrel, pk_value, isnull, cols, lesskeysCount, xmin_ts + 1, xmax_ts + 1, true);
		*end_keys = endKeyArray;
		
		pfree(cols);
		pfree(pk_value);
	}
	else
	{
		GenericSlice endKeyArray = encode_comp_scan_historical_key(rocksrel, NULL, 
									NULL, NULL, 0, xmin_ts + 1, xmax_ts + 1, true);
		*end_keys = endKeyArray;
	}

	return true;
}
