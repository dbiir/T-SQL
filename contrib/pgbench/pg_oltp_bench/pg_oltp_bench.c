/*
 * contrib/pg_oltp_bench/pg_oltp_bench.c
 */
#include "postgres.h"

#include "fmgr.h"
#include "mb/pg_wchar.h"
#include "utils/datum.h"

PG_MODULE_MAGIC;
PG_FUNCTION_INFO_V1(sb_rand_str);

Datum
sb_rand_str(PG_FUNCTION_ARGS)
{
	text	   *result;
	char	   *p,
			   *end;

	result = (text *) DatumGetPointer(datumCopy(PG_GETARG_DATUM(0), false, -1));

	p = VARDATA_ANY(result);
	end = p + VARSIZE_ANY_EXHDR(result);

	while (p < end)
	{
		int len = pg_mblen(p);

		if (len == 1)
		{
			if (*p == '#')
				*p = (random() % ('9' - '0' + 1)) + '0';
			else if (*p == '@')
				*p = (random() % ('z' - 'a' + 1)) + 'a';
		}
		p += len;
	}

	PG_RETURN_POINTER(result);
}
