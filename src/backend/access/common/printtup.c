/*-------------------------------------------------------------------------
 *
 * printtup.c
 *	  Routines to print out tuples to the destination (both frontend
 *	  clients and standalone backends are supported here).
 *
 *
 * Portions Copyright (c) 1996-2014, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/backend/access/common/printtup.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/printtup.h"
#include "libpq/libpq.h"
#include "libpq/pqformat.h"
#include "tcop/pquery.h"
#include "utils/lsyscache.h"
#include "utils/memdebug.h"
#include "utils/memutils.h"


static void printtup_startup(DestReceiver *self, int operation,
				 TupleDesc typeinfo);
static void printtup(TupleTableSlot *slot, DestReceiver *self);
static void printtup_20(TupleTableSlot *slot, DestReceiver *self);
static void printtup_internal_20(TupleTableSlot *slot, DestReceiver *self);
static void printtup_shutdown(DestReceiver *self);
static void printtup_destroy(DestReceiver *self);


/* ----------------------------------------------------------------
 *		printtup / debugtup support
 * ----------------------------------------------------------------
 */

/* ----------------
 *		Private state for a printtup destination object
 *
 * NOTE: finfo is the lookup info for either typoutput or typsend, whichever
 * we are using for this column.
 * ----------------
 */
typedef struct
{								/* Per-attribute information */
	Oid			typoutput;		/* Oid for the type's text output fn */
	Oid			typsend;		/* Oid for the type's binary output fn */
	bool		typisvarlena;	/* is it varlena (ie possibly toastable)? */
	int16		format;			/* format code for this column */
	FmgrInfo	finfo;			/* Precomputed call info for output fn */
} PrinttupAttrInfo;

typedef struct
{
	DestReceiver pub;			/* publicly-known function pointers */
	Portal		portal;			/* the Portal we are printing from */
	bool		sendDescrip;	/* send RowDescription at startup? */
	TupleDesc	attrinfo;		/* The attr info we are set up for */
	int			nattrs;
	PrinttupAttrInfo *myinfo;	/* Cached info about each attr */
	MemoryContext tmpcontext;	/* Memory context for per-row workspace */
} DR_printtup;

/* ----------------
 *		Initialize: create a DestReceiver for printtup
 * ----------------
 */
DestReceiver *
printtup_create_DR(CommandDest dest)
{
	DR_printtup *self = (DR_printtup *) palloc0(sizeof(DR_printtup));

	self->pub.receiveSlot = printtup;	/* might get changed later */
	self->pub.rStartup = printtup_startup;
	self->pub.rShutdown = printtup_shutdown;
	self->pub.rDestroy = printtup_destroy;
	self->pub.mydest = dest;

	/*
	 * Send T message automatically if DestRemote, but not if
	 * DestRemoteExecute
	 */
	self->sendDescrip = (dest == DestRemote);

	self->attrinfo = NULL;
	self->nattrs = 0;
	self->myinfo = NULL;
	self->tmpcontext = NULL;

	return (DestReceiver *) self;
}

/*
 * Set parameters for a DestRemote (or DestRemoteExecute) receiver
 */
void
SetRemoteDestReceiverParams(DestReceiver *self, Portal portal)
{
	DR_printtup *myState = (DR_printtup *) self;

	Assert(myState->pub.mydest == DestRemote ||
		   myState->pub.mydest == DestRemoteExecute);

	myState->portal = portal;

	if (PG_PROTOCOL_MAJOR(FrontendProtocol) < 3)
	{
		/*
		 * In protocol 2.0 the Bind message does not exist, so there is no way
		 * for the columns to have different print formats; it's sufficient to
		 * look at the first one.
		 */
		if (portal->formats && portal->formats[0] != 0)
			myState->pub.receiveSlot = printtup_internal_20;
		else
			myState->pub.receiveSlot = printtup_20;
	}
}

static void
printtup_startup(DestReceiver *self, int operation __attribute__((unused)), TupleDesc typeinfo)
{
	DR_printtup *myState = (DR_printtup *) self;
	Portal		portal = myState->portal;

	/*
	 * Create a temporary memory context that we can reset once per row to
	 * recover palloc'd memory.  This avoids any problems with leaks inside
	 * datatype output routines, and should be faster than retail pfree's
	 * anyway.
	 */
	myState->tmpcontext = AllocSetContextCreate(CurrentMemoryContext,
												"printtup",
												ALLOCSET_DEFAULT_MINSIZE,
												ALLOCSET_DEFAULT_INITSIZE,
												ALLOCSET_DEFAULT_MAXSIZE);

	if (PG_PROTOCOL_MAJOR(FrontendProtocol) < 3)
	{
		/*
		 * Send portal name to frontend (obsolete cruft, gone in proto 3.0)
		 *
		 * If portal name not specified, use "blank" portal.
		 */
		const char *portalName = portal->name;

		if (portalName == NULL || portalName[0] == '\0')
			portalName = "blank";

		pq_puttextmessage('P', portalName);
	}

	/*
	 * If we are supposed to emit row descriptions, then send the tuple
	 * descriptor of the tuples.
	 */
	if (myState->sendDescrip)
		SendRowDescriptionMessage(typeinfo,
								  FetchPortalTargetList(portal),
								  portal->formats);

	/* ----------------
	 * We could set up the derived attr info at this time, but we postpone it
	 * until the first call of printtup, for 2 reasons:
	 * 1. We don't waste time (compared to the old way) if there are no
	 *	  tuples at all to output.
	 * 2. Checking in printtup allows us to handle the case that the tuples
	 *	  change type midway through (although this probably can't happen in
	 *	  the current executor).
	 * ----------------
	 */
}

/*
 * SendRowDescriptionMessage --- send a RowDescription message to the frontend
 *
 * Notes: the TupleDesc has typically been manufactured by ExecTypeFromTL()
 * or some similar function; it does not contain a full set of fields.
 * The targetlist will be NIL when executing a utility function that does
 * not have a plan.  If the targetlist isn't NIL then it is a Query node's
 * targetlist; it is up to us to ignore resjunk columns in it.  The formats[]
 * array pointer might be NULL (if we are doing Describe on a prepared stmt);
 * send zeroes for the format codes in that case.
 */
void
SendRowDescriptionMessage(TupleDesc typeinfo, List *targetlist, int16 *formats)
{
	Form_pg_attribute *attrs = typeinfo->attrs;
	int			natts = typeinfo->natts;
	int			proto = PG_PROTOCOL_MAJOR(FrontendProtocol);
	int			i;
	StringInfoData buf;
	ListCell   *tlist_item = list_head(targetlist);

	pq_beginmessage(&buf, 'T'); /* tuple descriptor message type */
	pq_sendint(&buf, natts, 2); /* # of attrs in tuples */

	for (i = 0; i < natts; ++i)
	{
		Oid			atttypid = attrs[i]->atttypid;
		int32		atttypmod = attrs[i]->atttypmod;

		pq_sendstring(&buf, NameStr(attrs[i]->attname));
		/* column ID info appears in protocol 3.0 and up */
		if (proto >= 3)
		{
			/* Do we have a non-resjunk tlist item? */
			while (tlist_item &&
				   ((TargetEntry *) lfirst(tlist_item))->resjunk)
				tlist_item = lnext(tlist_item);
			if (tlist_item)
			{
				TargetEntry *tle = (TargetEntry *) lfirst(tlist_item);

				pq_sendint(&buf, tle->resorigtbl, 4);
				pq_sendint(&buf, tle->resorigcol, 2);
				tlist_item = lnext(tlist_item);
			}
			else
			{
				/* No info available, so send zeroes */
				pq_sendint(&buf, 0, 4);
				pq_sendint(&buf, 0, 2);
			}
		}
		/* If column is a domain, send the base type and typmod instead */
		atttypid = getBaseTypeAndTypmod(atttypid, &atttypmod);
		pq_sendint(&buf, (int) atttypid, sizeof(atttypid));
		pq_sendint(&buf, attrs[i]->attlen, sizeof(attrs[i]->attlen));
		/* typmod appears in protocol 2.0 and up */
		if (proto >= 2)
			pq_sendint(&buf, atttypmod, sizeof(atttypmod));
		/* format info appears in protocol 3.0 and up */
		if (proto >= 3)
		{
			if (formats)
				pq_sendint(&buf, formats[i], 2);
			else
				pq_sendint(&buf, 0, 2);
		}
	}
	pq_endmessage(&buf);
}

/*
 * Get the lookup info that printtup() needs
 */
static void
printtup_prepare_info(DR_printtup *myState, TupleDesc typeinfo, int numAttrs)
{
	int16	   *formats = myState->portal->formats;
	int			i;

	/* get rid of any old data */
	if (myState->myinfo)
		pfree(myState->myinfo);
	myState->myinfo = NULL;

	myState->attrinfo = typeinfo;
	myState->nattrs = numAttrs;
	if (numAttrs <= 0)
		return;

	myState->myinfo = (PrinttupAttrInfo *)
		palloc0(numAttrs * sizeof(PrinttupAttrInfo));

	for (i = 0; i < numAttrs; i++)
	{
		PrinttupAttrInfo *thisState = myState->myinfo + i;
		int16		format = (formats ? formats[i] : 0);

		thisState->format = format;
		if (format == 0)
		{
			getTypeOutputInfo(typeinfo->attrs[i]->atttypid,
							  &thisState->typoutput,
							  &thisState->typisvarlena);
			fmgr_info(thisState->typoutput, &thisState->finfo);
		}
		else if (format == 1)
		{
			getTypeBinaryOutputInfo(typeinfo->attrs[i]->atttypid,
									&thisState->typsend,
									&thisState->typisvarlena);
			fmgr_info(thisState->typsend, &thisState->finfo);
		}
		else
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("unsupported format code: %d", format)));
	}
}

/* ----------------
 *		printtup --- print a tuple in protocol 3.0
 * ----------------
 */
static void
printtup(TupleTableSlot *slot, DestReceiver *self)
{
	TupleDesc	typeinfo = slot->tts_tupleDescriptor;
	DR_printtup *myState = (DR_printtup *) self;
	MemoryContext oldcontext;
	StringInfoData buf;
	int			natts = typeinfo->natts;
	int			i;

	/* Set or update my derived attribute info, if needed */
	if (myState->attrinfo != typeinfo || myState->nattrs != natts)
		printtup_prepare_info(myState, typeinfo, natts);

	/* Make sure the tuple is fully deconstructed */
	slot_getallattrs(slot);

	/* Switch into per-row context so we can recover memory below */
	oldcontext = MemoryContextSwitchTo(myState->tmpcontext);

	/*
	 * Prepare a DataRow message (note buffer is in per-row context)
	 */
	pq_beginmessage(&buf, 'D');

	pq_sendint(&buf, natts, 2);

	/*
	 * send the attributes of this tuple
	 */
	for (i = 0; i < natts; ++i)
	{
		PrinttupAttrInfo *thisState = myState->myinfo + i;
		bool 		isnull;
		Datum		attr = slot_getattr(slot, i+1, &isnull);
		
		if (isnull)
		{
			/* -1 is the same in both byte orders.  This is the same as pg_sendint */
			int32 n32 = -1;
			appendBinaryStringInfo(&buf, (char *) &n32, 4);
			continue;
		}

		/*
		 * Here we catch undefined bytes in datums that are returned to the
		 * client without hitting disk; see comments at the related check in
		 * PageAddItem().  This test is most useful for uncompressed,
		 * non-external datums, but we're quite likely to see such here when
		 * testing new C functions.
		 */
		if (thisState->typisvarlena)
			VALGRIND_CHECK_MEM_IS_DEFINED(DatumGetPointer(attr),
										  VARSIZE_ANY(attr));

		if (thisState->format == 0)
		{
			/* Text output */
			char str[256];
			int32 n32;

			switch (typeinfo->attrs[i]->atttypid)
			{
				case INT2OID: /* int2 */
				case INT4OID: /* int4 */
				{
					/* 
					 * The standard postgres way is to call the output function, but that involves one or more pallocs,
					 * and a call to sprintf, followed by a conversion to client charset.  
					 * Do a fast conversion to string instead.
					 */
					char tmp[33];
					char *tp = tmp;
					char *sp;
					long li = 0;
					unsigned long v;
					long value;
					bool sign;
					if (typeinfo->attrs[i]->atttypid == INT2OID)
						value =(long)DatumGetInt16(attr);
					else
						value =(long)DatumGetInt32(attr);
					sign = (value < 0);
					if (sign)
						v = -value;
					else
						v = (unsigned long)value;
					while (v || tp == tmp)
					{
						li = v % 10;
						v = v / 10;
						*tp++ = li+'0';
					}
					sp = str;
					if (sign)
						*sp++ = '-';
					while (tp > tmp)
						*sp++ = *--tp;
					*sp = 0;
					/* send the size as a 4 byte big-endian int (same as pg_sendint) */
#if BYTE_ORDER == LITTLE_ENDIAN
					n32 = (uint32)((sp-str) << 24);  /* We know len is < 127, so this works, replaces htonl */
#elif BYTE_ORDER == BIG_ENDIAN
					n32 = (uint32) (sp-str);
#else
#error BYTE_ORDER must be BIG_ENDIAN or LITTLE_ENDIAN
#endif
					appendBinaryStringInfo(&buf, (char *) &n32, 4);
					appendBinaryStringInfo(&buf, str, strlen(str));
				}
				break;
			case INT8OID: /* int8 */
				{
					char tmp[33];
					char *tp = tmp;
					char *sp;
					uint64 v;
					long li = 0;
					int64 value =DatumGetInt64(attr);
					bool sign = (value < 0);
					if (sign)
						v = -value;
					else
						v = (uint64)value;
					while (v || tp == tmp)
					{
						li = v % 10;
						v = v / 10;
						*tp++ = li+'0';
					}
					sp = str;
					if (sign)
						*sp++ = '-';
					while (tp > tmp)
						*sp++ = *--tp;
					*sp = 0;
#if BYTE_ORDER == LITTLE_ENDIAN
					n32 = (uint32)((sp-str) << 24);  /* We know len is < 127, so this works, replaces htonl */
#else
					n32 = (uint32) (sp-str);
#endif
					appendBinaryStringInfo(&buf, (char *) &n32, 4);
					appendBinaryStringInfo(&buf, str, strlen(str));
				}
				break;

			case VARCHAROID:
			case TEXTOID:
			case BPCHAROID:
				{
					const char *s;
					char *p;
					int len;
					struct varlena *dptr;

					/*
					 * Detoast if needed. Packed, unaligned data is OK.
					 * (This is a substitute for calling textout())
					 */
					dptr = PG_DETOAST_DATUM_PACKED(attr);
					s = VARDATA_ANY(dptr);
					len = VARSIZE_ANY_EXHDR(dptr);

					/* inlined version of pq_sendcountedtext() */
					p = pg_server_to_client(s, len);
					if (p != s)				/* actual conversion has been done? */
					{
						len = strlen(p);
						n32 = htonl((uint32) len);
						appendBinaryStringInfo(&buf, (char *) &n32, 4);
						appendBinaryStringInfo(&buf, p, len);
						pfree(p);
					}
					else
					{
						n32 = htonl((uint32) len);
						appendBinaryStringInfo(&buf, (char *) &n32, 4);
						appendBinaryStringInfo(&buf, s, len);
					}
				}
				break;

			default:
				{
					char *outputstr;
					outputstr = OutputFunctionCall(&thisState->finfo, attr);
					pq_sendcountedtext(&buf, outputstr, strlen(outputstr), false);
				}
			}
		}
		else
		{
			/* Binary output */
			int32 n32;

			switch (typeinfo->attrs[i]->atttypid)
			{
			case INT2OID: /* int2 */
				{
					short int2 = DatumGetInt16(attr);
#if BYTE_ORDER == LITTLE_ENDIAN
                    int2 = htons(int2);
					n32 = (uint32)(2 << 24);   /* replaced htonl */
#else
					n32 = (uint32) 2;
#endif
					appendBinaryStringInfo(&buf, (char *) &n32, 4);
					appendBinaryStringInfo(&buf, &int2, 2);
				}
				break;
			case INT4OID: /* int4 */
				{
					int32 int4 = DatumGetInt32(attr);
#if BYTE_ORDER == LITTLE_ENDIAN
                    int4 = htonl(int4);
					n32 = (uint32)(4 << 24);   /* replaced htonl */
#else
					n32 = (uint32) 4;
#endif
					appendBinaryStringInfo(&buf, (char *) &n32, 4);
					appendBinaryStringInfo(&buf, &int4, 4);
				}
				break;
			case INT8OID: /* int8 */
				{
					int64 int8 = DatumGetInt64(attr);
#if BYTE_ORDER == LITTLE_ENDIAN
#define local_htonll(n)  ((((uint64) htonl(n)) << 32LL) | htonl((n) >> 32LL))
#define local_ntohll(n)  ((((uint64) ntohl(n)) << 32LL) | (uint32) ntohl(((uint64)n) >> 32LL))
                    int8 = local_htonll(int8);
					n32 = (uint32)(8 << 24);   /* replaced htonl */
#else
					n32 = (uint32) 8;
#endif
					appendBinaryStringInfo(&buf, (char *) &n32, 4);
					appendBinaryStringInfo(&buf, &int8, 8);
				}
				break;

			case VARCHAROID:
			case TEXTOID:
			case BPCHAROID:
				{
					const char *s;
					char *p;
					int len;
					struct varlena *dptr;

					/*
					 * Detoast if needed. Packed, unaligned data is OK.
					 * (This is a substitute for calling textsend())
					 */
					dptr = PG_DETOAST_DATUM_PACKED(attr);
					s = VARDATA_ANY(dptr);
					len = VARSIZE_ANY_EXHDR(dptr);

					/* inlined version of pq_sendtext(), as done by textsend() */
					p = pg_server_to_client(s, len);
					if (p != s)				/* actual conversion has been done? */
					{
						len = strlen(p);
						n32 = htonl((uint32) len);
						appendBinaryStringInfo(&buf, (char *) &n32, 4);
						appendBinaryStringInfo(&buf, p, len);
						pfree(p);
					}
					else
					{
						n32 = htonl((uint32) len);
						appendBinaryStringInfo(&buf, (char *) &n32, 4);
						appendBinaryStringInfo(&buf, s, len);
					}
				}
				break;

			default:
				{
					bytea *outputbytes;
					outputbytes = SendFunctionCall(&thisState->finfo, attr);
					pq_sendint(&buf, VARSIZE(outputbytes) - VARHDRSZ, 4);
					pq_sendbytes(&buf, VARDATA(outputbytes),
								 VARSIZE(outputbytes) - VARHDRSZ);
				}
			}
		}
	}

	pq_endmessage(&buf);

	/* Return to caller's context, and flush row's temporary memory */
	MemoryContextSwitchTo(oldcontext);
	MemoryContextReset(myState->tmpcontext);
}

/* ----------------
 *		printtup_20 --- print a tuple in protocol 2.0
 * ----------------
 */
static void
printtup_20(TupleTableSlot *slot, DestReceiver *self)
{
	TupleDesc	typeinfo = slot->tts_tupleDescriptor;
	DR_printtup *myState = (DR_printtup *) self;
	MemoryContext oldcontext;
	StringInfoData buf;
	int			natts = typeinfo->natts;
	int			i,
				j,
				k;

	/* Set or update my derived attribute info, if needed */
	if (myState->attrinfo != typeinfo || myState->nattrs != natts)
		printtup_prepare_info(myState, typeinfo, natts);

	/* Make sure the tuple is fully deconstructed */
	slot_getallattrs(slot);

	/* Switch into per-row context so we can recover memory below */
	oldcontext = MemoryContextSwitchTo(myState->tmpcontext);

	/*
	 * tell the frontend to expect new tuple data (in ASCII style)
	 */
	pq_beginmessage(&buf, 'D');

	/*
	 * send a bitmap of which attributes are not null
	 */
	j = 0;
	k = 1 << 7;
	for (i = 0; i < natts; ++i)
	{
        bool 		orignull;
		slot_getattr(slot, i+1, &orignull);

		if (!orignull) 
			j |= k;				/* set bit if not null */
		k >>= 1;
		if (k == 0)				/* end of byte? */
		{
			pq_sendint(&buf, j, 1);
			j = 0;
			k = 1 << 7;
		}
	}
	if (k != (1 << 7))			/* flush last partial byte */
		pq_sendint(&buf, j, 1);

	/*
	 * send the attributes of this tuple
	 */
	for (i = 0; i < natts; ++i)
	{
		PrinttupAttrInfo *thisState = myState->myinfo + i;
		bool		isnull;
		Datum		attr = slot_getattr(slot, i+1, &isnull);
		char	   *outputstr;

		if (isnull)
			continue;

		Assert(thisState->format == 0);

		outputstr = OutputFunctionCall(&thisState->finfo, attr);
		pq_sendcountedtext(&buf, outputstr, strlen(outputstr), true);
	}

	pq_endmessage(&buf);

	/* Return to caller's context, and flush row's temporary memory */
	MemoryContextSwitchTo(oldcontext);
	MemoryContextReset(myState->tmpcontext);
}

/* ----------------
 *		printtup_shutdown
 * ----------------
 */
static void
printtup_shutdown(DestReceiver *self)
{
	DR_printtup *myState = (DR_printtup *) self;

	if (myState->myinfo)
		pfree(myState->myinfo);
	myState->myinfo = NULL;

	myState->attrinfo = NULL;

	if (myState->tmpcontext)
		MemoryContextDelete(myState->tmpcontext);
	myState->tmpcontext = NULL;
}

/* ----------------
 *		printtup_destroy
 * ----------------
 */
static void
printtup_destroy(DestReceiver *self)
{
	pfree(self);
}

/* ----------------
 *		printatt
 * ----------------
 */
static void
printatt(unsigned attributeId,
		 Form_pg_attribute attributeP,
		 char *value)
{
	printf("\t%2d: %s%s%s%s\t(typeid = %u, len = %d, typmod = %d, byval = %c)\n",
		   attributeId,
		   NameStr(attributeP->attname),
		   value != NULL ? " = \"" : "",
		   value != NULL ? value : "",
		   value != NULL ? "\"" : "",
		   (unsigned int) (attributeP->atttypid),
		   attributeP->attlen,
		   attributeP->atttypmod,
		   attributeP->attbyval ? 't' : 'f');
}

/* ----------------
 *		debugStartup - prepare to print tuples for an interactive backend
 * ----------------
 */
void
debugStartup(DestReceiver *self __attribute__((unused)), int operation __attribute__((unused)), TupleDesc typeinfo)
{
	int			natts = typeinfo->natts;
	Form_pg_attribute *attinfo = typeinfo->attrs;
	int			i;

	/*
	 * show the return type of the tuples
	 */
	for (i = 0; i < natts; ++i)
		printatt((unsigned) i + 1, attinfo[i], NULL);
	printf("\t----\n");
}

/* ----------------
 *		debugtup - print one tuple for an interactive backend
 * ----------------
 */
void
debugtup(TupleTableSlot *slot, DestReceiver *self __attribute__((unused)))
{
	TupleDesc	typeinfo = slot->tts_tupleDescriptor;
	int			natts = typeinfo->natts;
	int			i;
	Datum		attr;
	char	   *value;
	bool		isnull;
	Oid			typoutput;
	bool		typisvarlena;

	for (i = 0; i < natts; ++i)
	{
		attr = slot_getattr(slot, i + 1, &isnull);
		if (isnull)
			continue;
		getTypeOutputInfo(typeinfo->attrs[i]->atttypid,
						  &typoutput, &typisvarlena);

		value = OidOutputFunctionCall(typoutput, attr);

		printatt((unsigned) i + 1, typeinfo->attrs[i], value);
	}
	printf("\t----\n");
}

/* ----------------
 *		printtup_internal_20 --- print a binary tuple in protocol 2.0
 *
 * We use a different message type, i.e. 'B' instead of 'D' to
 * indicate a tuple in internal (binary) form.
 *
 * This is largely same as printtup_20, except we use binary formatting.
 * ----------------
 */
static void
printtup_internal_20(TupleTableSlot *slot, DestReceiver *self)
{
	TupleDesc	typeinfo = slot->tts_tupleDescriptor;
	DR_printtup *myState = (DR_printtup *) self;
	MemoryContext oldcontext;
	StringInfoData buf;
	int			natts = typeinfo->natts;
	int			i,
				j,
				k;

	/* Set or update my derived attribute info, if needed */
	if (myState->attrinfo != typeinfo || myState->nattrs != natts)
		printtup_prepare_info(myState, typeinfo, natts);

	/* Make sure the tuple is fully deconstructed */
	slot_getallattrs(slot);

	/* Switch into per-row context so we can recover memory below */
	oldcontext = MemoryContextSwitchTo(myState->tmpcontext);

	/*
	 * tell the frontend to expect new tuple data (in binary style)
	 */
	pq_beginmessage(&buf, 'B');

	/*
	 * send a bitmap of which attributes are not null
	 */
	j = 0;
	k = 1 << 7;
	for (i = 0; i < natts; ++i)
	{
        bool 		orignull;
		slot_getattr(slot, i+1, &orignull);
		if (!orignull)
			j |= k;				/* set bit if not null */
		k >>= 1;
		if (k == 0)				/* end of byte? */
		{
			pq_sendint(&buf, j, 1);
			j = 0;
			k = 1 << 7;
		}
	}
	if (k != (1 << 7))			/* flush last partial byte */
		pq_sendint(&buf, j, 1);

	/*
	 * send the attributes of this tuple
	 */
	for (i = 0; i < natts; ++i)
	{
		PrinttupAttrInfo *thisState = myState->myinfo + i;
		bool		isnull;
		Datum		attr = slot_getattr(slot, i+1, &isnull);
		bytea	   *outputbytes;

		if (isnull)
			continue;

		Assert(thisState->format == 1);

		outputbytes = SendFunctionCall(&thisState->finfo, attr);
		pq_sendint(&buf, VARSIZE(outputbytes) - VARHDRSZ, 4);
		pq_sendbytes(&buf, VARDATA(outputbytes),
					 VARSIZE(outputbytes) - VARHDRSZ);
	}

	pq_endmessage(&buf);

	/* Return to caller's context, and flush row's temporary memory */
	MemoryContextSwitchTo(oldcontext);
	MemoryContextReset(myState->tmpcontext);
}
