/*
 * oro_pg_catalog.cpp — PG catalog compatibility layer for psql meta-commands.
 */

#include "oro_pg_catalog.h"
#include "sqlite3.h"

#include <cstring>
#include <cstdio>
#include <regex>
#include <string>

// =====================================================================
// SQL functions registered with SQLite
// =====================================================================

static void fn_pg_get_userbyid(sqlite3_context* c, int, sqlite3_value**) {
    sqlite3_result_text(c, "oro", -1, SQLITE_STATIC);
}

static void fn_pg_table_is_visible(sqlite3_context* c, int, sqlite3_value**) {
    sqlite3_result_int(c, 1);  // everything is visible
}

static void fn_format_type(sqlite3_context* c, int argc, sqlite3_value** argv) {
    if (argc < 1) { sqlite3_result_text(c, "text", -1, SQLITE_STATIC); return; }
    int oid = sqlite3_value_int(argv[0]);
    const char* t = "text";
    switch (oid) {
        case 16: t = "boolean"; break;
        case 20: t = "bigint"; break;
        case 21: t = "smallint"; break;
        case 23: t = "integer"; break;
        case 25: t = "text"; break;
        case 700: t = "real"; break;
        case 701: t = "double precision"; break;
        case 17: t = "bytea"; break;
        case 1043: t = "varchar"; break;
        default:  t = "text"; break;
    }
    sqlite3_result_text(c, t, -1, SQLITE_STATIC);
}

static void fn_version(sqlite3_context* c, int, sqlite3_value**) {
    sqlite3_result_text(c,
        "PostgreSQL 15.0 (oro-db MOT) on x86_64-linux-gnu",
        -1, SQLITE_STATIC);
}

static void fn_current_schema(sqlite3_context* c, int, sqlite3_value**) {
    sqlite3_result_text(c, "public", -1, SQLITE_STATIC);
}

static void fn_current_schemas(sqlite3_context* c, int, sqlite3_value**) {
    sqlite3_result_text(c, "{public}", -1, SQLITE_STATIC);
}

static void fn_pg_encoding_to_char(sqlite3_context* c, int, sqlite3_value**) {
    sqlite3_result_text(c, "UTF8", -1, SQLITE_STATIC);
}

static void fn_pg_get_indexdef(sqlite3_context* c, int argc, sqlite3_value** argv) {
    if (argc >= 1) {
        int oid = sqlite3_value_int(argv[0]);
        char buf[128];
        snprintf(buf, sizeof(buf), "CREATE INDEX oid_%d ON ...", oid);
        sqlite3_result_text(c, buf, -1, SQLITE_TRANSIENT);
    } else {
        sqlite3_result_null(c);
    }
}

static void fn_pg_get_expr(sqlite3_context* c, int, sqlite3_value**) {
    sqlite3_result_null(c);
}

static void fn_obj_description(sqlite3_context* c, int, sqlite3_value**) {
    sqlite3_result_null(c);
}

static void fn_col_description(sqlite3_context* c, int, sqlite3_value**) {
    sqlite3_result_null(c);
}

static void fn_pg_get_viewdef(sqlite3_context* c, int, sqlite3_value**) {
    sqlite3_result_text(c, "", -1, SQLITE_STATIC);
}

static void fn_pg_get_constraintdef(sqlite3_context* c, int, sqlite3_value**) {
    sqlite3_result_text(c, "", -1, SQLITE_STATIC);
}

static void fn_pg_get_function_identity_arguments(
    sqlite3_context* c, int, sqlite3_value**) {
    sqlite3_result_text(c, "", -1, SQLITE_STATIC);
}

static void fn_pg_get_function_arguments(
    sqlite3_context* c, int, sqlite3_value**) {
    sqlite3_result_text(c, "", -1, SQLITE_STATIC);
}

static void fn_pg_get_function_result(
    sqlite3_context* c, int, sqlite3_value**) {
    sqlite3_result_text(c, "text", -1, SQLITE_STATIC);
}

// array_to_string(arr, sep) — we store "arrays" as comma-separated strings
static void fn_array_to_string(sqlite3_context* c, int argc, sqlite3_value** argv) {
    if (argc < 1) { sqlite3_result_text(c, "", -1, SQLITE_STATIC); return; }
    sqlite3_result_value(c, argv[0]);
}

// REGEXP function used for "x REGEXP y" (and for rewritten "~" operators).
// Supports simple anchored patterns (^foo, bar$) and general substrings.
static void fn_regexp(sqlite3_context* c, int argc, sqlite3_value** argv) {
    if (argc < 2) { sqlite3_result_int(c, 0); return; }
    const char* pat = (const char*)sqlite3_value_text(argv[0]);
    const char* str = (const char*)sqlite3_value_text(argv[1]);
    if (!pat || !str) { sqlite3_result_int(c, 0); return; }
    try {
        std::regex re(pat);
        sqlite3_result_int(c, std::regex_search(str, re) ? 1 : 0);
    } catch (...) {
        sqlite3_result_int(c, 0);
    }
}

// =====================================================================
// Catalog views DDL
// =====================================================================

// DDL to create the catalog tables (static ones with literal data).
// Dynamic tables (pg_class, pg_attribute, etc.) are populated separately
// with CREATE TABLE AS SELECT which can reference main.sqlite_schema.
static const char* PG_CATALOG_STATIC_DDL =
    "CREATE TABLE pg_catalog.pg_namespace AS "
    "SELECT 2200 AS oid, 'public' AS nspname, 10 AS nspowner "
    "UNION ALL SELECT 11, 'pg_catalog', 10 "
    "UNION ALL SELECT 99, 'pg_toast', 10 "
    "UNION ALL SELECT 12, 'information_schema', 10;"

    "CREATE TABLE pg_catalog.pg_am AS "
    "SELECT 403 AS oid, 'btree' AS amname, 'i' AS amtype "
    "UNION ALL SELECT 405, 'hash', 'i' "
    "UNION ALL SELECT 2, 'heap', 't';"

    "CREATE TABLE pg_catalog.pg_type AS "
    "SELECT oid, typname, typnamespace, 10 AS typowner, typlen, "
    "       typbyval, typtype, typcategory, typispreferred, 1 AS typisdefined, "
    "       ',' AS typdelim, 0 AS typrelid, 0 AS typsubscript, 0 AS typelem, "
    "       0 AS typarray, '-' AS typinput, '-' AS typoutput, "
    "       '-' AS typreceive, '-' AS typsend, '-' AS typmodin, "
    "       '-' AS typmodout, '-' AS typanalyze, 'p' AS typalign, "
    "       'p' AS typstorage, 0 AS typnotnull, 0 AS typbasetype, "
    "       -1 AS typtypmod, 0 AS typndims, 0 AS typcollation, "
    "       NULL AS typdefaultbin, NULL AS typdefault, NULL AS typacl "
    "FROM (SELECT 16 AS oid, 'bool' AS typname, 2200 AS typnamespace, 1 AS typlen, 1 AS typbyval, 'b' AS typtype, 'B' AS typcategory, 1 AS typispreferred "
    "      UNION ALL SELECT 20, 'int8',   2200, 8, 1, 'b', 'N', 0 "
    "      UNION ALL SELECT 21, 'int2',   2200, 2, 1, 'b', 'N', 0 "
    "      UNION ALL SELECT 23, 'int4',   2200, 4, 1, 'b', 'N', 0 "
    "      UNION ALL SELECT 25, 'text',   2200, -1, 0, 'b', 'S', 1 "
    "      UNION ALL SELECT 700,'float4', 2200, 4, 1, 'b', 'N', 0 "
    "      UNION ALL SELECT 701,'float8', 2200, 8, 1, 'b', 'N', 0 "
    "      UNION ALL SELECT 17, 'bytea',  2200, -1, 0, 'b', 'U', 0 "
    "      UNION ALL SELECT 1043,'varchar',2200, -1, 0, 'b', 'S', 0 "
    "      UNION ALL SELECT 1700,'numeric',2200, -1, 0, 'b', 'N', 0);"

    "CREATE TABLE pg_catalog.pg_database AS "
    "SELECT 16384 AS oid, 'oro' AS datname, 10 AS datdba, 6 AS encoding, "
    "       'c' AS datlocprovider, 1 AS datistemplate, 1 AS datallowconn, "
    "       -1 AS datconnlimit, 0 AS datlastsysoid, 0 AS datfrozenxid, "
    "       0 AS datminmxid, 1663 AS dattablespace, 'C' AS datcollate, "
    "       'C' AS datctype, '' AS daticulocale, '' AS datcollversion, "
    "       NULL AS datacl;"

    "CREATE TABLE pg_catalog.pg_roles AS "
    "SELECT 10 AS oid, 'oro' AS rolname, 1 AS rolsuper, 1 AS rolinherit, "
    "       1 AS rolcreaterole, 1 AS rolcreatedb, 1 AS rolcanlogin, "
    "       1 AS rolreplication, 0 AS rolconnlimit, NULL AS rolpassword, "
    "       NULL AS rolvaliduntil, 0 AS rolbypassrls, NULL AS rolconfig;"

    "CREATE TABLE pg_catalog.pg_user AS "
    "SELECT 'oro' AS usename, 10 AS usesysid, 1 AS usecreatedb, 1 AS usesuper, "
    "       1 AS userepl, 0 AS usebypassrls, '' AS passwd, NULL AS valuntil, "
    "       NULL AS useconfig;"

    "CREATE TABLE pg_catalog.pg_description ("
    "  objoid INTEGER, classoid INTEGER, objsubid INTEGER, description TEXT);"

    "CREATE TABLE pg_catalog.pg_tablespace AS "
    "SELECT 1663 AS oid, 'pg_default' AS spcname, 10 AS spcowner, "
    "       NULL AS spcacl, NULL AS spcoptions;"

    "CREATE TABLE pg_catalog.pg_settings (name TEXT, setting TEXT);"

    "CREATE TABLE pg_catalog.pg_auth_members ("
    "  roleid INTEGER, member INTEGER, grantor INTEGER, "
    "  admin_option INTEGER);"

    // pg_proc — empty (no user-defined functions)
    "CREATE TABLE pg_catalog.pg_proc ("
    "  oid INTEGER, proname TEXT, pronamespace INTEGER, proowner INTEGER, "
    "  prolang INTEGER, prokind TEXT, prorettype INTEGER);"

    // pg_constraint — empty
    "CREATE TABLE pg_catalog.pg_constraint ("
    "  oid INTEGER, conname TEXT, connamespace INTEGER, contype TEXT, "
    "  conrelid INTEGER, conindid INTEGER, conkey TEXT);"

    // pg_attrdef — column defaults (empty; we don't track them)
    "CREATE TABLE pg_catalog.pg_attrdef ("
    "  oid INTEGER, adrelid INTEGER, adnum INTEGER, adbin TEXT);"

    // pg_inherits — table inheritance (empty)
    "CREATE TABLE pg_catalog.pg_inherits ("
    "  inhrelid INTEGER, inhparent INTEGER, inhseqno INTEGER, "
    "  inhdetachpending INTEGER);"

    // pg_trigger — triggers (empty)
    "CREATE TABLE pg_catalog.pg_trigger ("
    "  oid INTEGER, tgrelid INTEGER, tgname TEXT, tgfoid INTEGER, "
    "  tgtype INTEGER, tgenabled TEXT);"

    // pg_rewrite — rules (empty)
    "CREATE TABLE pg_catalog.pg_rewrite ("
    "  oid INTEGER, rulename TEXT, ev_class INTEGER, ev_type TEXT);"

    // pg_policy — row-level security (empty with all columns)
    "CREATE TABLE pg_catalog.pg_policy ("
    "  oid INTEGER, polname TEXT, polrelid INTEGER, polcmd TEXT, "
    "  polpermissive INTEGER, polroles TEXT, polqual TEXT, polwithcheck TEXT);"

    // pg_collation — collations
    "CREATE TABLE pg_catalog.pg_collation AS "
    "SELECT 100 AS oid, 'default' AS collname, 11 AS collnamespace, "
    "       10 AS collowner, 100 AS collprovider, 1 AS collisdeterministic, "
    "       -1 AS collencoding, 'C' AS collcollate, 'C' AS collctype;"

    // pg_foreign_table, pg_partitioned_table (empty)
    "CREATE TABLE pg_catalog.pg_foreign_table ("
    "  ftrelid INTEGER, ftserver INTEGER);"
    "CREATE TABLE pg_catalog.pg_partitioned_table ("
    "  partrelid INTEGER, partstrat TEXT, partnatts INTEGER);"

    // pg_statistic_ext — extended stats (empty)
    "CREATE TABLE pg_catalog.pg_statistic_ext ("
    "  oid INTEGER, stxrelid INTEGER, stxname TEXT, stxnamespace INTEGER, "
    "  stxowner INTEGER, stxstattarget INTEGER, stxkeys TEXT, stxkind TEXT, "
    "  stxexprs TEXT);"

    // pg_statistic (empty)
    "CREATE TABLE pg_catalog.pg_statistic ("
    "  starelid INTEGER, staattnum INTEGER);"

    // pg_sequence (empty)
    "CREATE TABLE pg_catalog.pg_sequence ("
    "  seqrelid INTEGER, seqtypid INTEGER, seqstart INTEGER, "
    "  seqincrement INTEGER, seqmax INTEGER, seqmin INTEGER, "
    "  seqcache INTEGER, seqcycle INTEGER);"

    // pg_publication / pg_subscription (empty, with common columns)
    "CREATE TABLE pg_catalog.pg_publication ("
    "  oid INTEGER, pubname TEXT, pubowner INTEGER, puballtables INTEGER, "
    "  pubinsert INTEGER, pubupdate INTEGER, pubdelete INTEGER, "
    "  pubtruncate INTEGER, pubviaroot INTEGER);"
    "CREATE TABLE pg_catalog.pg_subscription ("
    "  oid INTEGER, subname TEXT, subowner INTEGER, subenabled INTEGER, "
    "  subconninfo TEXT, subslotname TEXT, subsynccommit TEXT, "
    "  subpublications TEXT);"

    // pg_extension (empty)
    "CREATE TABLE pg_catalog.pg_extension ("
    "  oid INTEGER, extname TEXT, extnamespace INTEGER);"

    // pg_language (empty)
    "CREATE TABLE pg_catalog.pg_language ("
    "  oid INTEGER, lanname TEXT);"

    // pg_event_trigger (empty)
    "CREATE TABLE pg_catalog.pg_event_trigger ("
    "  oid INTEGER, evtname TEXT);"

    // pg_ts_config / pg_ts_parser / pg_ts_dict / pg_ts_template (empty)
    "CREATE TABLE pg_catalog.pg_ts_config (oid INTEGER, cfgname TEXT);"
    "CREATE TABLE pg_catalog.pg_ts_parser (oid INTEGER, prsname TEXT);"
    "CREATE TABLE pg_catalog.pg_ts_dict (oid INTEGER, dictname TEXT);"
    "CREATE TABLE pg_catalog.pg_ts_template (oid INTEGER, tmplname TEXT);"

    // pg_operator, pg_opclass, pg_opfamily (empty)
    "CREATE TABLE pg_catalog.pg_operator (oid INTEGER, oprname TEXT);"
    "CREATE TABLE pg_catalog.pg_opclass (oid INTEGER, opcname TEXT);"
    "CREATE TABLE pg_catalog.pg_opfamily (oid INTEGER, opfname TEXT);"

    // pg_cast, pg_conversion (empty)
    "CREATE TABLE pg_catalog.pg_cast (oid INTEGER);"
    "CREATE TABLE pg_catalog.pg_conversion (oid INTEGER, conname TEXT);"

    // pg_aggregate (empty)
    "CREATE TABLE pg_catalog.pg_aggregate (aggfnoid INTEGER);"

    // pg_default_acl (empty)
    "CREATE TABLE pg_catalog.pg_default_acl ("
    "  oid INTEGER, defaclrole INTEGER, defaclnamespace INTEGER, "
    "  defaclobjtype TEXT, defaclacl TEXT);"

    // pg_largeobject, pg_largeobject_metadata (empty)
    "CREATE TABLE pg_catalog.pg_largeobject (loid INTEGER);"
    "CREATE TABLE pg_catalog.pg_largeobject_metadata (oid INTEGER);"

    // pg_enum (empty)
    "CREATE TABLE pg_catalog.pg_enum ("
    "  oid INTEGER, enumtypid INTEGER, enumsortorder REAL, enumlabel TEXT);"

    // pg_range (empty)
    "CREATE TABLE pg_catalog.pg_range ("
    "  rngtypid INTEGER, rngsubtype INTEGER);"

    // Replication / publication (empty)
    "CREATE TABLE pg_catalog.pg_publication_rel ("
    "  oid INTEGER, prpubid INTEGER, prrelid INTEGER);"
    "CREATE TABLE pg_catalog.pg_publication_tables ("
    "  pubname TEXT, schemaname TEXT, tablename TEXT);"
    "CREATE TABLE pg_catalog.pg_subscription_rel ("
    "  srsubid INTEGER, srrelid INTEGER, srsubstate TEXT);"
    "CREATE TABLE pg_catalog.pg_shdescription ("
    "  objoid INTEGER, classoid INTEGER, description TEXT);"
    "CREATE TABLE pg_catalog.pg_seclabel ("
    "  objoid INTEGER, classoid INTEGER, objsubid INTEGER, "
    "  provider TEXT, label TEXT);"
    "CREATE TABLE pg_catalog.pg_shseclabel ("
    "  objoid INTEGER, classoid INTEGER, provider TEXT, label TEXT);"
    ;

// Dynamic DDL — uses main.sqlite_schema, run via CREATE TABLE AS SELECT
static const char* PG_CATALOG_DYNAMIC_DDL =
    "CREATE TABLE pg_catalog.pg_class AS "
    "SELECT rowid AS oid, "
    "       name AS relname, "
    "       2200 AS relnamespace, "
    "       0 AS reltype, "
    "       0 AS reloftype, "
    "       10 AS relowner, "
    "       403 AS relam, "
    "       0 AS relfilenode, "
    "       0 AS reltablespace, "
    "       0 AS relpages, "
    "       0.0 AS reltuples, "
    "       0 AS relallvisible, "
    "       0 AS reltoastrelid, "
    "       CASE type WHEN 'index' THEN 1 ELSE 0 END AS relhasindex, "
    "       0 AS relisshared, "
    "       'p' AS relpersistence, "
    "       CASE type WHEN 'table' THEN 'r' "
    "                 WHEN 'view' THEN 'v' "
    "                 WHEN 'index' THEN 'i' "
    "                 ELSE 'r' END AS relkind, "
    "       0 AS relnatts, "
    "       0 AS relchecks, "
    "       0 AS relhasrules, "
    "       0 AS relhastriggers, "
    "       0 AS relhassubclass, "
    "       0 AS relrowsecurity, "
    "       0 AS relforcerowsecurity, "
    "       1 AS relispopulated, "
    "       'p' AS relreplident, "
    "       0 AS relispartition, "
    "       0 AS relrewrite, "
    "       0 AS relfrozenxid, "
    "       0 AS relminmxid, "
    "       NULL AS relacl, "
    "       NULL AS reloptions, "
    "       NULL AS relpartbound "
    "FROM main.sqlite_schema "
    "WHERE name NOT LIKE 'sqlite_%' AND name <> '_mot_wal';"

    "CREATE TABLE pg_catalog.pg_attribute AS "
    "SELECT m.rowid AS attrelid, "
    "       p.name AS attname, "
    "       25 AS atttypid, "
    "       p.cid + 1 AS attnum, "
    "       -1 AS attlen, "
    "       -1 AS attndims, "
    "       -1 AS attcacheoff, "
    "       -1 AS atttypmod, "
    "       1 AS attbyval, "
    "       'p' AS attstorage, "
    "       'p' AS attalign, "
    "       CASE WHEN p.\"notnull\" = 1 THEN 1 ELSE 0 END AS attnotnull, "
    "       CASE WHEN p.dflt_value IS NULL THEN 0 ELSE 1 END AS atthasdef, "
    "       0 AS atthasmissing, "
    "       '' AS attidentity, "
    "       '' AS attgenerated, "
    "       0 AS attisdropped, "
    "       1 AS attislocal, "
    "       0 AS attinhcount, "
    "       100 AS attcollation, "
    "       NULL AS attacl, "
    "       NULL AS attoptions, "
    "       NULL AS attfdwoptions "
    "FROM main.sqlite_schema m, pragma_table_info(m.name) p "
    "WHERE m.type = 'table' AND m.name NOT LIKE 'sqlite_%' "
    "  AND m.name <> '_mot_wal';"

    "CREATE TABLE pg_catalog.pg_index AS "
    "SELECT rowid AS indexrelid, "
    "       (SELECT rowid FROM main.sqlite_schema WHERE name = m.tbl_name) AS indrelid, "
    "       0 AS indnatts, 0 AS indnkeyatts, 0 AS indisunique, "
    "       0 AS indisprimary, 0 AS indisexclusion, 1 AS indimmediate, "
    "       0 AS indisclustered, 1 AS indisvalid, 1 AS indcheckxmin, "
    "       1 AS indisready, 1 AS indislive, 0 AS indisreplident, "
    "       '' AS indkey, '' AS indcollation, '' AS indclass, "
    "       '' AS indoption, NULL AS indexprs, NULL AS indpred "
    "FROM main.sqlite_schema m WHERE type='index';"

    "CREATE TABLE pg_catalog.pg_tables AS "
    "SELECT 'public' AS schemaname, name AS tablename, 'oro' AS tableowner, "
    "       NULL AS tablespace, 0 AS hasindexes, 0 AS hasrules, 0 AS hastriggers, "
    "       0 AS rowsecurity "
    "FROM main.sqlite_schema WHERE type='table' AND name NOT LIKE 'sqlite_%' "
    "  AND name <> '_mot_wal';"

    "CREATE TABLE pg_catalog.pg_views AS "
    "SELECT 'public' AS schemaname, name AS viewname, 'oro' AS viewowner, "
    "       sql AS definition "
    "FROM main.sqlite_schema WHERE type='view';"

    "CREATE TABLE pg_catalog.pg_indexes AS "
    "SELECT 'public' AS schemaname, tbl_name AS tablename, name AS indexname, "
    "       NULL AS tablespace, sql AS indexdef "
    "FROM main.sqlite_schema WHERE type='index';"
    ;

// =====================================================================
// Query rewriting (text-level)
// =====================================================================

// Replace a prefix like "ARRAY(SELECT..." through its matching ')' with repl.
// Repeats until no more matches (handles nested cases).
static void replaceBalanced(std::string& s, const char* prefix, const char* repl) {
    size_t plen = strlen(prefix);
    while (true) {
        size_t start = s.find(prefix);
        if (start == std::string::npos) break;
        size_t openParen = start + plen - 1;  // the '(' in the prefix
        int depth = 1;
        size_t i = openParen + 1;
        while (i < s.size() && depth > 0) {
            if (s[i] == '(') depth++;
            else if (s[i] == ')') depth--;
            i++;
        }
        if (depth != 0) break;  // unbalanced; bail
        s.replace(start, i - start, repl);
    }
}

std::string oroPgRewriteQuery(const char* sql) {
    if (!sql) return "";
    std::string s = sql;

    // Strip casts — handle both bare and pg_catalog-qualified forms.
    // Matches ::TYPE and ::pg_catalog.TYPE
    static const std::regex castRe(
        R"(::(?:pg_catalog\.)?(?:regclass|regtype|regproc|regoper|regnamespace|oid|name|text|int2vector|oidvector|"char"|char))",
        std::regex::optimize);
    s = std::regex_replace(s, castRe, "");

    // SQLite doesn't support schema-qualified function calls.
    // Strip pg_catalog. prefix when immediately before an opening paren.
    //   pg_catalog.pg_get_userbyid(x)  →  pg_get_userbyid(x)
    // Table references like pg_catalog.pg_class stay qualified (our ATTACH works).
    static const std::regex fnPrefixRe(
        R"(pg_catalog\.(\w+)\s*\()",
        std::regex::optimize);
    s = std::regex_replace(s, fnPrefixRe, "$1(");

    // OPERATOR(pg_catalog.~)  → REGEXP
    // OPERATOR(pg_catalog.!~) → NOT REGEXP
    // OPERATOR(pg_catalog.=)  → =
    // OPERATOR(pg_catalog.<>) → <>   (etc.)
    static const std::regex opTildeRe(
        R"(OPERATOR\s*\(\s*pg_catalog\s*\.\s*~\s*\))",
        std::regex::optimize);
    s = std::regex_replace(s, opTildeRe, " REGEXP ");
    static const std::regex opBangTildeRe(
        R"(OPERATOR\s*\(\s*pg_catalog\s*\.\s*!~\s*\))",
        std::regex::optimize);
    s = std::regex_replace(s, opBangTildeRe, " NOT REGEXP ");
    static const std::regex opGenericRe(
        R"(OPERATOR\s*\(\s*pg_catalog\s*\.\s*([<>=!@#%^&*|/+\-]+)\s*\))",
        std::regex::optimize);
    s = std::regex_replace(s, opGenericRe, " $1 ");

    // COLLATE pg_catalog.default | COLLATE "default" | COLLATE C  → (strip)
    static const std::regex collateRe(
        R"(COLLATE\s+(?:pg_catalog\.default|"default"|C|"C"))",
        std::regex::optimize | std::regex::icase);
    s = std::regex_replace(s, collateRe, "");

    // E'...'  →  '...' (strip E-string prefix; SQLite doesn't support PG escapes)
    static const std::regex estringRe(
        R"(\bE'([^']*)')",
        std::regex::optimize);
    s = std::regex_replace(s, estringRe, "'$1'");

    // ARRAY(SELECT ...) → '' — handle nested parens by manual matching
    replaceBalanced(s, "ARRAY(", "''");
    replaceBalanced(s, "array(", "''");

    // ARRAY[a, b, ...] → '' (bracket form)
    static const std::regex arrayLitRe(
        R"(ARRAY\s*\[[^\]]*\])",
        std::regex::optimize);
    s = std::regex_replace(s, arrayLitRe, "''");

    // x = ANY(expr) → x = (expr)    — ANY isn't a SQLite keyword
    // Similarly for ALL, SOME. This is a rough approximation.
    static const std::regex anyRe(
        R"(\b(?:ANY|ALL|SOME)\s*\()",
        std::regex::optimize | std::regex::icase);
    s = std::regex_replace(s, anyRe, "(");

    // Handle: X !~ 'pattern'   →  X NOT REGEXP 'pattern'
    //         X ~ 'pattern'    →  X REGEXP 'pattern'
    // Use our registered regexp() function.
    static const std::regex notMatchRe(
        R"(([A-Za-z_][A-Za-z_0-9\.]*)\s*!~\s*('[^']*'))",
        std::regex::optimize);
    s = std::regex_replace(s, notMatchRe, "$1 NOT REGEXP $2");

    static const std::regex matchRe(
        R"(([A-Za-z_][A-Za-z_0-9\.]*)\s*~\s*('[^']*'))",
        std::regex::optimize);
    s = std::regex_replace(s, matchRe, "$1 REGEXP $2");

    // Also handle case-insensitive variants (~*, !~*) — we just use REGEXP
    // for both (SQLite's registered function uses std::regex default flags).
    static const std::regex notIMatchRe(
        R"(([A-Za-z_][A-Za-z_0-9\.]*)\s*!~\*\s*('[^']*'))",
        std::regex::optimize);
    s = std::regex_replace(s, notIMatchRe, "$1 NOT REGEXP $2");

    static const std::regex iMatchRe(
        R"(([A-Za-z_][A-Za-z_0-9\.]*)\s*~\*\s*('[^']*'))",
        std::regex::optimize);
    s = std::regex_replace(s, iMatchRe, "$1 REGEXP $2");

    return s;
}

// =====================================================================
// Init
// =====================================================================

int oroPgCatalogInit(sqlite3* db) {
    if (!db) return 1;

    // Attach an in-memory DB named pg_catalog so "pg_catalog.foo" resolves.
    // SQLite forbids views in attached DBs from referencing main, so we use
    // CREATE TABLE AS SELECT to snapshot instead. This is fresh per connection.
    char* err = nullptr;
    if (sqlite3_exec(db,
            "ATTACH DATABASE ':memory:' AS pg_catalog",
            nullptr, nullptr, &err) != SQLITE_OK) {
        if (err) { fprintf(stderr, "pg_catalog attach: %s\n", err); sqlite3_free(err); }
        return 1;
    }

    if (sqlite3_exec(db, PG_CATALOG_STATIC_DDL, nullptr, nullptr, &err) != SQLITE_OK) {
        if (err) { fprintf(stderr, "pg_catalog static ddl: %s\n", err); sqlite3_free(err); }
        return 1;
    }
    if (sqlite3_exec(db, PG_CATALOG_DYNAMIC_DDL, nullptr, nullptr, &err) != SQLITE_OK) {
        if (err) { fprintf(stderr, "pg_catalog dynamic ddl: %s\n", err); sqlite3_free(err); }
        return 1;
    }

    // Register functions under multiple names (psql uses both qualified and
    // unqualified forms).
    auto reg = [&](const char* name, int nargs, void (*fn)(sqlite3_context*, int, sqlite3_value**)) {
        sqlite3_create_function(db, name, nargs, SQLITE_UTF8, nullptr, fn, nullptr, nullptr);
    };

    reg("pg_get_userbyid", 1, fn_pg_get_userbyid);
    reg("pg_table_is_visible", 1, fn_pg_table_is_visible);
    reg("format_type", 2, fn_format_type);
    reg("format_type", 1, fn_format_type);
    reg("version", 0, fn_version);
    reg("current_schema", 0, fn_current_schema);
    reg("current_schemas", 1, fn_current_schemas);
    reg("current_schemas", 0, fn_current_schemas);
    reg("pg_encoding_to_char", 1, fn_pg_encoding_to_char);
    reg("pg_get_indexdef", 1, fn_pg_get_indexdef);
    reg("pg_get_indexdef", 3, fn_pg_get_indexdef);
    reg("pg_get_expr", 2, fn_pg_get_expr);
    reg("pg_get_expr", 3, fn_pg_get_expr);
    reg("obj_description", 1, fn_obj_description);
    reg("obj_description", 2, fn_obj_description);
    reg("col_description", 2, fn_col_description);
    reg("pg_get_viewdef", 1, fn_pg_get_viewdef);
    reg("pg_get_viewdef", 2, fn_pg_get_viewdef);
    reg("pg_get_constraintdef", 1, fn_pg_get_constraintdef);
    reg("pg_get_constraintdef", 2, fn_pg_get_constraintdef);
    reg("pg_get_function_identity_arguments", 1, fn_pg_get_function_identity_arguments);
    reg("pg_get_function_arguments", 1, fn_pg_get_function_arguments);
    reg("pg_get_function_result", 1, fn_pg_get_function_result);
    reg("array_to_string", 2, fn_array_to_string);
    reg("array_to_string", 3, fn_array_to_string);
    reg("regexp", 2, fn_regexp);  // SQLite's REGEXP operator calls this

    // Stat / misc functions — all no-op
    reg("pg_get_statisticsobjdef_columns", 1, fn_pg_get_viewdef);
    reg("pg_get_partkeydef", 1, fn_pg_get_viewdef);
    reg("pg_get_triggerdef", 1, fn_pg_get_viewdef);
    reg("pg_get_triggerdef", 2, fn_pg_get_viewdef);
    reg("pg_get_ruledef", 1, fn_pg_get_viewdef);
    reg("pg_get_ruledef", 2, fn_pg_get_viewdef);
    reg("pg_relation_is_publishable", 1, fn_pg_table_is_visible);
    reg("pg_function_is_visible", 1, fn_pg_table_is_visible);
    reg("pg_type_is_visible", 1, fn_pg_table_is_visible);
    reg("pg_operator_is_visible", 1, fn_pg_table_is_visible);
    reg("pg_opclass_is_visible", 1, fn_pg_table_is_visible);
    reg("pg_collation_is_visible", 1, fn_pg_table_is_visible);
    reg("pg_conversion_is_visible", 1, fn_pg_table_is_visible);
    reg("pg_ts_config_is_visible", 1, fn_pg_table_is_visible);
    reg("pg_ts_parser_is_visible", 1, fn_pg_table_is_visible);
    reg("pg_ts_dict_is_visible", 1, fn_pg_table_is_visible);
    reg("pg_ts_template_is_visible", 1, fn_pg_table_is_visible);
    reg("has_schema_privilege", 2, fn_pg_table_is_visible);
    reg("has_schema_privilege", 3, fn_pg_table_is_visible);
    reg("has_table_privilege", 2, fn_pg_table_is_visible);
    reg("has_table_privilege", 3, fn_pg_table_is_visible);
    reg("has_column_privilege", 3, fn_pg_table_is_visible);
    reg("has_database_privilege", 2, fn_pg_table_is_visible);
    reg("pg_tablespace_location", 1, fn_pg_get_viewdef);

    return 0;
}
