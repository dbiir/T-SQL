/*-------------------------------------------------------------------------
 *
 * collationcmds.c
 *	  collation-related commands support code
 *
 * Portions Copyright (c) 1996-2014, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/commands/collationcmds.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/heapam.h"
#include "access/htup_details.h"
#include "access/xact.h"
#include "catalog/dependency.h"
#include "catalog/indexing.h"
#include "catalog/namespace.h"
#include "catalog/oid_dispatch.h"
#include "catalog/pg_collation.h"
#include "catalog/pg_collation_fn.h"
#include "commands/alter.h"
#include "commands/collationcmds.h"
#include "commands/dbcommands.h"
#include "commands/defrem.h"
#include "mb/pg_wchar.h"
#include "miscadmin.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/pg_locale.h"
#include "utils/rel.h"
#include "utils/syscache.h"

#include "cdb/cdbvars.h"
#include "cdb/cdbdisp_query.h"
#include "storage/fd.h"

typedef struct
{
	char	   *localename;		/* name of locale, as per "locale -a" */
	char	   *alias;			/* shortened alias for same */
	int			enc;			/* encoding */
} CollAliasData;

Datum pg_import_system_collations(PG_FUNCTION_ARGS);

/*
 * CREATE COLLATION
 */
Oid
DefineCollation(List *names, List *parameters, bool if_not_exists)
{
	char	   *collName;
	Oid			collNamespace;
	AclResult	aclresult;
	ListCell   *pl;
	DefElem    *fromEl = NULL;
	DefElem    *localeEl = NULL;
	DefElem    *lccollateEl = NULL;
	DefElem    *lcctypeEl = NULL;
	char	   *collcollate = NULL;
	char	   *collctype = NULL;
	Oid			newoid;

	collNamespace = QualifiedNameGetCreationNamespace(names, &collName);

	aclresult = pg_namespace_aclcheck(collNamespace, GetUserId(), ACL_CREATE);
	if (aclresult != ACLCHECK_OK)
		aclcheck_error(aclresult, ACL_KIND_NAMESPACE,
					   get_namespace_name(collNamespace));

	foreach(pl, parameters)
	{
		DefElem    *defel = (DefElem *) lfirst(pl);
		DefElem   **defelp;

		if (pg_strcasecmp(defel->defname, "from") == 0)
			defelp = &fromEl;
		else if (pg_strcasecmp(defel->defname, "locale") == 0)
			defelp = &localeEl;
		else if (pg_strcasecmp(defel->defname, "lc_collate") == 0)
			defelp = &lccollateEl;
		else if (pg_strcasecmp(defel->defname, "lc_ctype") == 0)
			defelp = &lcctypeEl;
		else
		{
			ereport(ERROR,
					(errcode(ERRCODE_SYNTAX_ERROR),
					 errmsg("collation attribute \"%s\" not recognized",
							defel->defname)));
			break;
		}

		*defelp = defel;
	}

	if ((localeEl && (lccollateEl || lcctypeEl))
		|| (fromEl && list_length(parameters) != 1))
		ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
				 errmsg("conflicting or redundant options")));

	if (fromEl)
	{
		Oid			collid;
		HeapTuple	tp;

		collid = get_collation_oid(defGetQualifiedName(fromEl), false);
		tp = SearchSysCache1(COLLOID, ObjectIdGetDatum(collid));
		if (!HeapTupleIsValid(tp))
			elog(ERROR, "cache lookup failed for collation %u", collid);

		collcollate = pstrdup(NameStr(((Form_pg_collation) GETSTRUCT(tp))->collcollate));
		collctype = pstrdup(NameStr(((Form_pg_collation) GETSTRUCT(tp))->collctype));

		ReleaseSysCache(tp);
	}

	if (localeEl)
	{
		collcollate = defGetString(localeEl);
		collctype = defGetString(localeEl);
	}

	if (lccollateEl)
		collcollate = defGetString(lccollateEl);

	if (lcctypeEl)
		collctype = defGetString(lcctypeEl);

	if (!collcollate)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
				 errmsg("parameter \"lc_collate\" must be specified")));

	if (!collctype)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
				 errmsg("parameter \"lc_ctype\" must be specified")));

	check_encoding_locale_matches(GetDatabaseEncoding(), collcollate, collctype);

	newoid = CollationCreate(collName,
							 collNamespace,
							 GetUserId(),
							 GetDatabaseEncoding(),
							 collcollate,
							 collctype,
							 if_not_exists,
							 false);	/* not quiet */

	if (!OidIsValid(newoid))
		return InvalidOid;

	/* check that the locales can be loaded */
	CommandCounterIncrement();
	(void) pg_newlocale_from_collation(newoid);

	if (Gp_role == GP_ROLE_DISPATCH)
	{
		DefineStmt * stmt = makeNode(DefineStmt);
		stmt->kind = OBJECT_COLLATION;
		stmt->oldstyle = false;
		stmt->defnames = names;
		stmt->args = NIL;
		stmt->definition = parameters;
		stmt->trusted = false;
		CdbDispatchUtilityStatement((Node *) stmt,
		                            DF_CANCEL_ON_ERROR|
			                        DF_WITH_SNAPSHOT|
			                        DF_NEED_TWO_PHASE,
		                            GetAssignedOidsForDispatch(),
		                            NULL);
	}

	return newoid;
}

/*
 * Subroutine for ALTER COLLATION SET SCHEMA and RENAME
 *
 * Is there a collation with the same name of the given collation already in
 * the given namespace?  If so, raise an appropriate error message.
 */
void
IsThereCollationInNamespace(const char *collname, Oid nspOid)
{
	/* make sure the name doesn't already exist in new schema */
	if (SearchSysCacheExists3(COLLNAMEENCNSP,
							  CStringGetDatum(collname),
							  Int32GetDatum(GetDatabaseEncoding()),
							  ObjectIdGetDatum(nspOid)))
		ereport(ERROR,
				(errcode(ERRCODE_DUPLICATE_OBJECT),
				 errmsg("collation \"%s\" for encoding \"%s\" already exists in schema \"%s\"",
						collname, GetDatabaseEncodingName(),
						get_namespace_name(nspOid))));

	/* mustn't match an any-encoding entry, either */
	if (SearchSysCacheExists3(COLLNAMEENCNSP,
							  CStringGetDatum(collname),
							  Int32GetDatum(-1),
							  ObjectIdGetDatum(nspOid)))
		ereport(ERROR,
				(errcode(ERRCODE_DUPLICATE_OBJECT),
				 errmsg("collation \"%s\" already exists in schema \"%s\"",
						collname, get_namespace_name(nspOid))));
}


/* will we use "locale -a" in pg_import_system_collations? */
#if defined(HAVE_LOCALE_T) && !defined(WIN32)
#define READ_LOCALE_A_OUTPUT
#endif

#ifdef READ_LOCALE_A_OUTPUT
/*
 * "Normalize" a locale name, stripping off encoding tags such as
 * ".utf8" (e.g., "en_US.utf8" -> "en_US", but "br_FR.iso885915@euro"
 * -> "br_FR@euro").  Return true if a new, different name was
 * generated.
 */
static bool
normalize_locale_name(char *new, const char *old)
{
	char	   *n = new;
	const char *o = old;
	bool		changed = false;

	while (*o)
	{
		if (*o == '.')
		{
			/* skip over encoding tag such as ".utf8" or ".UTF-8" */
			o++;
			while ((*o >= 'A' && *o <= 'Z')
				   || (*o >= 'a' && *o <= 'z')
				   || (*o >= '0' && *o <= '9')
				   || (*o == '-'))
				o++;
			changed = true;
		}
		else
			*n++ = *o++;
	}
	*n = '\0';

	return changed;
}

/*
 * qsort comparator for CollAliasData items
 */
static int
cmpaliases(const void *a, const void *b)
{
	const CollAliasData *ca = (const CollAliasData *) a;
	const CollAliasData *cb = (const CollAliasData *) b;

	/* comparing localename is enough because other fields are derived */
	return strcmp(ca->localename, cb->localename);
}
#endif							/* READ_LOCALE_A_OUTPUT */

static void
DispatchCollationCreate(char *alias, char *locale, Oid nspid, int encoding)
{
	Assert(Gp_role == GP_ROLE_DISPATCH);

	List *names = NIL;
	Value *schemaname = makeString(get_namespace_name(nspid));
	Value *relname = makeString(alias);

	names = lappend(names, schemaname);
	names = lappend(names, relname);

	List *parameters = NIL;
	DefElem *defstring = makeNode(DefElem);

	defstring->defname = "locale";
	defstring->defaction = DEFELEM_UNSPEC;
	defstring->arg = (Node*) makeString(locale);

	parameters = lappend(parameters, defstring);

	DefineStmt * stmt = makeNode(DefineStmt);
	stmt->kind = OBJECT_COLLATION;
	stmt->oldstyle = false;
	stmt->defnames = names;
	stmt->args = NIL;
	stmt->definition = parameters;
	stmt->trusted = false;
	CdbDispatchUtilityStatement((Node *) stmt,
	                            DF_CANCEL_ON_ERROR|
		                        DF_WITH_SNAPSHOT|
		                        DF_NEED_TWO_PHASE,
	                            GetAssignedOidsForDispatch(),
	                            NULL);
}

/*
 * pg_import_system_collations: add known system collations to pg_collation
 */
Datum
pg_import_system_collations(PG_FUNCTION_ARGS)
{
	Oid			nspid = PG_GETARG_OID(0);
	int			ncreated = 0;

	/* silence compiler warning if we have no locale implementation at all */
	(void) nspid;

	if (!superuser())
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 (errmsg("must be superuser to import system collations"))));

	if (Gp_role != GP_ROLE_DISPATCH)
		ereport(ERROR,
					(errmsg("must be dispatcher to import system collations")));

	/* Load collations known to libc, using "locale -a" to enumerate them */
#ifdef READ_LOCALE_A_OUTPUT
	{
		FILE	   *locale_a_handle;
		char		localebuf[NAMEDATALEN]; /* we assume ASCII so this is fine */
		int			nvalid = 0;
		Oid			collid;
		CollAliasData *aliases;
		int			naliases,
					maxaliases,
					i;

		/* expansible array of aliases */
		maxaliases = 100;
		aliases = (CollAliasData *) palloc(maxaliases * sizeof(CollAliasData));
		naliases = 0;

		locale_a_handle = OpenPipeStream("locale -a", "r");
		if (locale_a_handle == NULL)
			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("could not execute command \"%s\": %m",
							"locale -a")));

		while (fgets(localebuf, sizeof(localebuf), locale_a_handle))
		{
			size_t		len;
			int			enc;
			bool		skip;
			char		alias[NAMEDATALEN];

			len = strlen(localebuf);

			if (len == 0 || localebuf[len - 1] != '\n')
			{
				elog(DEBUG1, "locale name too long, skipped: \"%s\"", localebuf);
				continue;
			}
			localebuf[len - 1] = '\0';

			/*
			 * Some systems have locale names that don't consist entirely of
			 * ASCII letters (such as "bokm&aring;l" or "fran&ccedil;ais").
			 * This is pretty silly, since we need the locale itself to
			 * interpret the non-ASCII characters. We can't do much with
			 * those, so we filter them out.
			 */
			skip = false;
			for (i = 0; i < len; i++)
			{
				if (IS_HIGHBIT_SET(localebuf[i]))
				{
					skip = true;
					break;
				}
			}
			if (skip)
			{
				elog(DEBUG1, "locale name has non-ASCII characters, skipped: \"%s\"", localebuf);
				continue;
			}

			enc = pg_get_encoding_from_locale(localebuf, false);
			if (enc < 0)
			{
				/* error message printed by pg_get_encoding_from_locale() */
				continue;
			}
			if (!PG_VALID_BE_ENCODING(enc))
				continue;		/* ignore locales for client-only encodings */
			if (enc == PG_SQL_ASCII)
				continue;		/* C/POSIX are already in the catalog */
			if (enc != GetDatabaseEncoding())
				continue;

			/* count valid locales found in operating system */
			nvalid++;

			/*
			 * Create a collation named the same as the locale, but quietly
			 * doing nothing if it already exists.  This is the behavior we
			 * need even at initdb time, because some versions of "locale -a"
			 * can report the same locale name more than once.  And it's
			 * convenient for later import runs, too, since you just about
			 * always want to add on new locales without a lot of chatter
			 * about existing ones.
			 */
			collid = CollationCreate(localebuf, nspid, GetUserId(),
									 enc, localebuf, localebuf,
									 true, true);

			if (OidIsValid(collid))
			{
				DispatchCollationCreate(localebuf, localebuf, nspid, enc);

				ncreated++;

				/* Must do CCI between inserts to handle duplicates correctly */
				CommandCounterIncrement();
			}

			/*
			 * Generate aliases such as "en_US" in addition to "en_US.utf8"
			 * for ease of use.  Note that collation names are unique per
			 * encoding only, so this doesn't clash with "en_US" for LATIN1,
			 * say.
			 *
			 * However, it might conflict with a name we'll see later in the
			 * "locale -a" output.  So save up the aliases and try to add them
			 * after we've read all the output.
			 */
			if (normalize_locale_name(alias, localebuf))
			{
				if (naliases >= maxaliases)
				{
					maxaliases *= 2;
					aliases = (CollAliasData *)
						repalloc(aliases, maxaliases * sizeof(CollAliasData));
				}
				aliases[naliases].localename = pstrdup(localebuf);
				aliases[naliases].alias = pstrdup(alias);
				aliases[naliases].enc = enc;
				naliases++;
			}
		}

		ClosePipeStream(locale_a_handle);

		/*
		 * Before processing the aliases, sort them by locale name.  The point
		 * here is that if "locale -a" gives us multiple locale names with the
		 * same encoding and base name, say "en_US.utf8" and "en_US.utf-8", we
		 * want to pick a deterministic one of them.  First in ASCII sort
		 * order is a good enough rule.  (Before PG 10, the code corresponding
		 * to this logic in initdb.c had an additional ordering rule, to
		 * prefer the locale name exactly matching the alias, if any.  We
		 * don't need to consider that here, because we would have already
		 * created such a pg_collation entry above, and that one will win.)
		 */
		if (naliases > 1)
			qsort((void *) aliases, naliases, sizeof(CollAliasData), cmpaliases);

		/* Now add aliases, ignoring any that match pre-existing entries */
		for (i = 0; i < naliases; i++)
		{
			char	   *locale = aliases[i].localename;
			char	   *alias = aliases[i].alias;
			int			enc = aliases[i].enc;

			collid = CollationCreate(alias, nspid, GetUserId(),
									 enc, locale, locale,
									 true, true);

			if (OidIsValid(collid))
			{
				DispatchCollationCreate(alias, locale, nspid, enc);

				ncreated++;

				CommandCounterIncrement();
			}
		}

		/* Give a warning if "locale -a" seems to be malfunctioning */
		if (nvalid == 0)
			ereport(WARNING,
					(errmsg("no usable system locales were found")));
	}
#endif							/* READ_LOCALE_A_OUTPUT */

	PG_RETURN_INT32(ncreated);
}

