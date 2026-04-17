/*
 * oro_pg_catalog.h — PostgreSQL catalog compatibility layer
 *
 * Provides enough of pg_catalog to make psql meta-commands work
 * (\dt, \d, \l, \dn, \di, \dv, \du).
 *
 * Implementation strategy:
 *   1. ATTACH ':memory:' AS pg_catalog   — schema-like namespace
 *   2. Create views in pg_catalog that project from sqlite_schema and
 *      pragma_table_info
 *   3. Register functions (pg_get_userbyid, pg_table_is_visible, etc.)
 *   4. Server-side query rewriting for PG-only syntax (~, !~, ::regclass)
 */

#ifndef ORO_PG_CATALOG_H
#define ORO_PG_CATALOG_H

#include <string>

struct sqlite3;

/* Set up pg_catalog views and functions on a connection.
 * Call once per connection after sqlite3_open. */
int oroPgCatalogInit(sqlite3* db);

/* Rewrite PostgreSQL-specific syntax to SQLite-compatible form:
 *   x ~ 'pattern'   →  LIKE / REGEXP
 *   x !~ 'pattern'  →  NOT LIKE / NOT REGEXP
 *   ::regclass      →  (stripped)
 *   ::oid, ::text, ::name → (stripped)
 *   ANY(ARRAY[...]) → IN (...)
 */
std::string oroPgRewriteQuery(const char* sql);

#endif
