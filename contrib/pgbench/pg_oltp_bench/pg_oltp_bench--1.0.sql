/* contrib/pg_oltp_bench/pg_oltp_bench--1.0.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION pg_oltp_bench" to load this file. \quit

CREATE FUNCTION sb_rand_str(text)
RETURNS text
AS 'MODULE_PATHNAME'
LANGUAGE C VOLATILE STRICT;
