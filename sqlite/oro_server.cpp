/*
 * oro_server.cpp - PostgreSQL wire protocol server for oro-db
 *
 * Listens on a TCP port and translates PostgreSQL v3 simple-query protocol
 * into sqlite3_exec calls against the MOT+SQLite engine. Any PostgreSQL
 * client (psql, pgAdmin, DBeaver, DataGrip) can connect.
 *
 * Supports:
 *   - Simple query protocol (Q message → RowDescription + DataRow* + CommandComplete)
 *   - Multiple statements per query string (semicolon-separated)
 *   - NULL values, all SQLite types (sent as text)
 *   - Basic error reporting via ErrorResponse
 *   - Concurrent connections (one thread per client)
 *
 * Usage:
 *   ./oro_server                          # listen on :5432, in-memory DB
 *   ./oro_server --port 5433              # custom port
 *   ./oro_server --db /path/to/file.db    # persistent SQLite file
 *   ./oro_server --mot-config mot.conf    # explicit MOT config
 */

#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string>
#include <unordered_map>
#include <vector>
#include <thread>
#include <climits>
#include <unistd.h>
#include <libgen.h>
#include <signal.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <netinet/tcp.h>

#include "sqlite3.h"
#include "oro_mot_adapter.h"
#include "oro_pg_catalog.h"
#include "oro_auth.h"

// Global users table (read once at startup). Empty => trust mode.
static OroUsers g_users;

// =====================================================================
// PG wire protocol helpers
// =====================================================================

// Write buffer that accumulates a message before sending
struct PgMsg {
    std::vector<uint8_t> buf;

    void clear() { buf.clear(); }

    void putByte(uint8_t b) { buf.push_back(b); }

    void putInt16(int16_t v) {
        uint16_t n = htons((uint16_t)v);
        buf.insert(buf.end(), (uint8_t*)&n, (uint8_t*)&n + 2);
    }

    void putInt32(int32_t v) {
        uint32_t n = htonl((uint32_t)v);
        buf.insert(buf.end(), (uint8_t*)&n, (uint8_t*)&n + 4);
    }

    void putString(const char* s) {
        size_t len = strlen(s) + 1;  // include null terminator
        buf.insert(buf.end(), (const uint8_t*)s, (const uint8_t*)s + len);
    }

    void putBytes(const void* data, size_t len) {
        buf.insert(buf.end(), (const uint8_t*)data, (const uint8_t*)data + len);
    }

    // Finalize: write type byte + length prefix, then send
    bool send(int fd, char type) {
        uint32_t bodyLen = (uint32_t)buf.size() + 4;  // length includes itself
        uint32_t netLen = htonl(bodyLen);
        uint8_t hdr[5];
        hdr[0] = (uint8_t)type;
        memcpy(hdr + 1, &netLen, 4);

        if (write(fd, hdr, 5) != 5) return false;
        if (!buf.empty()) {
            size_t total = 0;
            while (total < buf.size()) {
                ssize_t n = write(fd, buf.data() + total, buf.size() - total);
                if (n <= 0) return false;
                total += n;
            }
        }
        return true;
    }
};

// Read exactly n bytes from fd
static bool readFull(int fd, void* buf, size_t n) {
    size_t total = 0;
    while (total < n) {
        ssize_t r = read(fd, (uint8_t*)buf + total, n - total);
        if (r <= 0) return false;
        total += r;
    }
    return true;
}

static int32_t readInt32(int fd) {
    uint32_t v;
    if (!readFull(fd, &v, 4)) return -1;
    return (int32_t)ntohl(v);
}

// =====================================================================
// PG protocol messages
// =====================================================================

static bool sendAuthOk(int fd) {
    PgMsg m;
    m.putInt32(0);  // auth ok
    return m.send(fd, 'R');
}

// AuthenticationMD5Password: int32(5) + 4-byte salt.
static bool sendAuthMd5(int fd, const uint8_t salt[4]) {
    PgMsg m;
    m.putInt32(5);
    m.putBytes(salt, 4);
    return m.send(fd, 'R');
}

static bool sendParameterStatus(int fd, const char* name, const char* value) {
    PgMsg m;
    m.putString(name);
    m.putString(value);
    return m.send(fd, 'S');
}

static bool sendBackendKeyData(int fd, int32_t pid, int32_t secret) {
    PgMsg m;
    m.putInt32(pid);
    m.putInt32(secret);
    return m.send(fd, 'K');
}

static bool sendReadyForQuery(int fd, char status) {
    PgMsg m;
    m.putByte((uint8_t)status);
    return m.send(fd, 'Z');
}

static bool sendErrorResponse(int fd, const char* severity,
                              const char* code, const char* message) {
    PgMsg m;
    m.putByte('S'); m.putString(severity);
    m.putByte('V'); m.putString(severity);
    m.putByte('C'); m.putString(code);
    m.putByte('M'); m.putString(message);
    m.putByte(0);  // terminator
    return m.send(fd, 'E');
}

// Forward decl — definition lives after sendEmptyQuery.
static int32_t pgTypeOidForStmtCol(sqlite3_stmt* stmt, int col);

static bool sendRowDescription(int fd, int ncols, const char** names) {
    PgMsg m;
    m.putInt16((int16_t)ncols);
    for (int i = 0; i < ncols; i++) {
        m.putString(names[i]);
        m.putInt32(0);       // table OID
        m.putInt16(0);       // column attribute number
        m.putInt32(25);      // type OID: text
        m.putInt16(-1);      // type size: variable
        m.putInt32(-1);      // type modifier
        m.putInt16(0);       // format: text
    }
    return m.send(fd, 'T');
}

// RowDescription with per-column type OIDs and format codes. Used by the
// extended protocol so clients can dispatch on declared types.
static bool sendRowDescriptionTyped(int fd, sqlite3_stmt* stmt,
                                    const std::vector<int16_t>& result_formats) {
    int ncols = sqlite3_column_count(stmt);
    PgMsg m;
    m.putInt16((int16_t)ncols);
    for (int i = 0; i < ncols; i++) {
        const char* name = sqlite3_column_name(stmt, i);
        m.putString(name ? name : "");
        m.putInt32(0);                                // table OID
        m.putInt16(0);                                // column attribute number
        m.putInt32(pgTypeOidForStmtCol(stmt, i));     // type OID
        m.putInt16(-1);                               // type size
        m.putInt32(-1);                               // type modifier
        int16_t fmt = 0;
        if (result_formats.size() == 1) {
            fmt = result_formats[0];
        } else if ((size_t)i < result_formats.size()) {
            fmt = result_formats[i];
        }
        m.putInt16(fmt);
    }
    return m.send(fd, 'T');
}

static bool sendDataRow(int fd, int ncols, const char** values) {
    PgMsg m;
    m.putInt16((int16_t)ncols);
    for (int i = 0; i < ncols; i++) {
        if (values[i] == nullptr) {
            m.putInt32(-1);  // NULL
        } else {
            int32_t len = (int32_t)strlen(values[i]);
            m.putInt32(len);
            m.putBytes(values[i], len);
        }
    }
    return m.send(fd, 'D');
}

static bool sendCommandComplete(int fd, const char* tag) {
    PgMsg m;
    m.putString(tag);
    return m.send(fd, 'C');
}

static bool sendEmptyQuery(int fd) {
    PgMsg m;
    return m.send(fd, 'I');
}

// Extended-protocol response messages.
static bool sendParseComplete(int fd)   { PgMsg m; return m.send(fd, '1'); }
static bool sendBindComplete(int fd)    { PgMsg m; return m.send(fd, '2'); }
static bool sendCloseComplete(int fd)   { PgMsg m; return m.send(fd, '3'); }
static bool sendNoData(int fd)          { PgMsg m; return m.send(fd, 'n'); }
static bool sendPortalSuspended(int fd) { PgMsg m; return m.send(fd, 's'); }

static bool sendParameterDescription(int fd, const std::vector<int32_t>& oids) {
    PgMsg m;
    m.putInt16((int16_t)oids.size());
    for (auto o : oids) m.putInt32(o);
    return m.send(fd, 't');
}

// =====================================================================
// SQLite affinity → PG type OID
// =====================================================================
//
// SQLite is dynamically typed per-row, so we infer an OID from the per-row
// column type. Used by RowDescription emitted in the extended protocol.
//   23  = int4   (we report INTEGER as int4 — small enough for most use)
//   20  = int8   (for large integers)
//   25  = text
//   701 = float8
//   17  = bytea
static int32_t pgTypeOidForStmtCol(sqlite3_stmt* stmt, int col) {
    const char* decl = sqlite3_column_decltype(stmt, col);
    if (decl) {
        if (strcasestr(decl, "INT"))   return 20;   // INTEGER, BIGINT → int8
        if (strcasestr(decl, "CHAR") ||
            strcasestr(decl, "TEXT") ||
            strcasestr(decl, "CLOB"))  return 25;   // text
        if (strcasestr(decl, "REAL") ||
            strcasestr(decl, "FLOA") ||
            strcasestr(decl, "DOUB"))  return 701;  // float8
        if (strcasestr(decl, "BLOB"))  return 17;   // bytea
    }
    return 25;  // default: text
}

// =====================================================================
// Query execution
// =====================================================================

struct QueryState {
    int fd;
    int ncols;
    int nrows;
    bool header_sent;
    bool error;
};

static int queryCallback(void* data, int ncols, char** values, char** names) {
    auto* qs = (QueryState*)data;
    if (qs->error) return 1;

    // Send RowDescription on first row
    if (!qs->header_sent) {
        qs->ncols = ncols;
        if (!sendRowDescription(qs->fd, ncols, (const char**)names)) {
            qs->error = true;
            return 1;
        }
        qs->header_sent = true;
    }

    // Send DataRow
    if (!sendDataRow(qs->fd, ncols, (const char**)values)) {
        qs->error = true;
        return 1;
    }
    qs->nrows++;
    return 0;
}

// Process a single prepared statement: send RowDescription upfront (even for
// 0-row results), then DataRow per row, then CommandComplete.
// Returns 0 on success, -1 on send failure.
static int execOneStmt(int fd, sqlite3* db, sqlite3_stmt* stmt, const char* firstWord) {
    int ncols = sqlite3_column_count(stmt);

    if (ncols > 0) {
        // SELECT-like: send RowDescription first
        std::vector<std::string> nameStore(ncols);
        std::vector<const char*> names(ncols);
        for (int i = 0; i < ncols; i++) {
            const char* n = sqlite3_column_name(stmt, i);
            nameStore[i] = n ? n : "";
            names[i] = nameStore[i].c_str();
        }
        if (!sendRowDescription(fd, ncols, names.data())) return -1;

        int64_t nrows = 0;
        while (true) {
            int rc = sqlite3_step(stmt);
            if (rc == SQLITE_ROW) {
                std::vector<std::string> valStore(ncols);
                std::vector<const char*> vals(ncols);
                for (int i = 0; i < ncols; i++) {
                    int type = sqlite3_column_type(stmt, i);
                    if (type == SQLITE_NULL) {
                        vals[i] = nullptr;
                    } else {
                        const char* v = (const char*)sqlite3_column_text(stmt, i);
                        valStore[i] = v ? v : "";
                        vals[i] = valStore[i].c_str();
                    }
                }
                if (!sendDataRow(fd, ncols, vals.data())) return -1;
                nrows++;
            } else if (rc == SQLITE_DONE) {
                break;
            } else {
                sendErrorResponse(fd, "ERROR", "42000", sqlite3_errmsg(db));
                return 0;
            }
        }
        char tag[64];
        snprintf(tag, sizeof(tag), "SELECT %ld", (long)nrows);
        sendCommandComplete(fd, tag);
    } else {
        // DDL/DML: step once
        int rc = sqlite3_step(stmt);
        if (rc != SQLITE_DONE && rc != SQLITE_ROW) {
            sendErrorResponse(fd, "ERROR", "42000", sqlite3_errmsg(db));
            return 0;
        }
        const char* tag = "OK";
        char buf[64];
        if (strncasecmp(firstWord, "INSERT", 6) == 0) {
            snprintf(buf, sizeof(buf), "INSERT 0 %d", sqlite3_changes(db));
            tag = buf;
        } else if (strncasecmp(firstWord, "UPDATE", 6) == 0) {
            snprintf(buf, sizeof(buf), "UPDATE %d", sqlite3_changes(db));
            tag = buf;
        } else if (strncasecmp(firstWord, "DELETE", 6) == 0) {
            snprintf(buf, sizeof(buf), "DELETE %d", sqlite3_changes(db));
            tag = buf;
        } else if (strncasecmp(firstWord, "CREATE", 6) == 0) {
            tag = "CREATE TABLE";
        } else if (strncasecmp(firstWord, "DROP", 4) == 0) {
            tag = "DROP TABLE";
        } else if (strncasecmp(firstWord, "BEGIN", 5) == 0) {
            tag = "BEGIN";
        } else if (strncasecmp(firstWord, "COMMIT", 6) == 0) {
            tag = "COMMIT";
        } else if (strncasecmp(firstWord, "ROLLBACK", 8) == 0) {
            tag = "ROLLBACK";
        }
        sendCommandComplete(fd, tag);
    }
    return 0;
}

static void handleQuery(int fd, sqlite3* db, const char* sql) {
    // Skip empty queries
    const char* p = sql;
    while (*p == ' ' || *p == '\t' || *p == '\n' || *p == '\r') p++;
    if (*p == '\0') {
        sendEmptyQuery(fd);
        sendReadyForQuery(fd, 'I');
        return;
    }

    // Special command: CHECKPOINT (compacts the MOT WAL)
    //   CHECKPOINT             -> run a checkpoint now
    //   CHECKPOINT AUTO <N>    -> arm auto-checkpoint at every N WAL writes
    //                             (N=0 disables); does NOT checkpoint immediately
    if (strncasecmp(p, "CHECKPOINT", 10) == 0 &&
        (p[10] == ';' || p[10] == '\0' || p[10] == ' ' || p[10] == '\t')) {
        const char* rest = p + 10;
        while (*rest == ' ' || *rest == '\t') rest++;
        if (strncasecmp(rest, "AUTO", 4) == 0 &&
            (rest[4] == ' ' || rest[4] == '\t')) {
            const char* nstr = rest + 4;
            while (*nstr == ' ' || *nstr == '\t') nstr++;
            uint64_t thr = strtoull(nstr, nullptr, 10);
            if (oroMotWalSetAutoCheckpoint((void*)db, thr) != 0) {
                sendErrorResponse(fd, "ERROR", "XX000",
                                  "set auto-checkpoint failed");
            } else {
                char tag[64];
                snprintf(tag, sizeof(tag), "CHECKPOINT AUTO %llu",
                         (unsigned long long)thr);
                sendCommandComplete(fd, tag);
            }
            sendReadyForQuery(fd, 'I');
            return;
        }
        int n = oroMotWalCheckpoint((void*)db);
        if (n < 0) {
            sendErrorResponse(fd, "ERROR", "XX000", "checkpoint failed");
        } else {
            char tag[64];
            snprintf(tag, sizeof(tag), "CHECKPOINT %d", n);
            sendCommandComplete(fd, tag);
        }
        sendReadyForQuery(fd, 'I');
        return;
    }

    // Rewrite PG-specific syntax (~, !~, ::regclass, etc.) to SQLite-compatible
    std::string rewritten = oroPgRewriteQuery(sql);

    QueryState qs = {fd, 0, 0, false, false};
    char* errmsg = nullptr;
    int rc = sqlite3_exec(db, rewritten.c_str(), queryCallback, &qs, &errmsg);
    if (rc != SQLITE_OK) {
        sendErrorResponse(fd, "ERROR", "42000", errmsg ? errmsg : "?");
        if (errmsg) sqlite3_free(errmsg);
    } else if (!qs.header_sent) {
        const char* tag = "OK";
        char buf[64];
        if (strncasecmp(p, "INSERT", 6) == 0) {
            snprintf(buf, sizeof(buf), "INSERT 0 %d", sqlite3_changes(db));
            tag = buf;
        } else if (strncasecmp(p, "UPDATE", 6) == 0) {
            snprintf(buf, sizeof(buf), "UPDATE %d", sqlite3_changes(db));
            tag = buf;
        } else if (strncasecmp(p, "DELETE", 6) == 0) {
            snprintf(buf, sizeof(buf), "DELETE %d", sqlite3_changes(db));
            tag = buf;
        } else if (strncasecmp(p, "CREATE", 6) == 0) tag = "CREATE TABLE";
        else if (strncasecmp(p, "DROP", 4) == 0) tag = "DROP TABLE";
        else if (strncasecmp(p, "BEGIN", 5) == 0) tag = "BEGIN";
        else if (strncasecmp(p, "COMMIT", 6) == 0) tag = "COMMIT";
        else if (strncasecmp(p, "ROLLBACK", 8) == 0) tag = "ROLLBACK";
        sendCommandComplete(fd, tag);
    } else {
        char tag[64]; snprintf(tag, sizeof(tag), "SELECT %d", qs.nrows);
        sendCommandComplete(fd, tag);
    }
    sendReadyForQuery(fd, 'I');
    return;
}

// Unused legacy path — reserved for fallback debugging
static void handleQueryLegacy(int fd, sqlite3* db, const char* sql) {
    const char* p = sql;
    while (*p == ' ' || *p == '\t' || *p == '\n' || *p == '\r') p++;
    if (*p == '\0') { sendEmptyQuery(fd); sendReadyForQuery(fd, 'I'); return; }
    std::string rewritten = oroPgRewriteQuery(sql);
    QueryState qs = {fd, 0, 0, false, false};
    char* errmsg = nullptr;
    int rc = sqlite3_exec(db, rewritten.c_str(), queryCallback, &qs, &errmsg);
    if (rc != SQLITE_OK) {
        const char* msg = errmsg ? errmsg : "unknown error";
        sendErrorResponse(fd, "ERROR", "42000", msg);
        if (errmsg) sqlite3_free(errmsg);
    } else {
        if (!qs.header_sent) {
            const char* tag = "OK";
            if (strncasecmp(p, "INSERT", 6) == 0) {
                tag = "INSERT 0 1";
            } else if (strncasecmp(p, "CREATE", 6) == 0) {
                tag = "CREATE TABLE";
            }
            sendCommandComplete(fd, tag);
        } else {
            char buf[64];
            snprintf(buf, sizeof(buf), "SELECT %d", qs.nrows);
            sendCommandComplete(fd, buf);
        }
    }

    sendReadyForQuery(fd, 'I');
}

// =====================================================================
// Extended query protocol (Parse/Bind/Describe/Execute/Sync/Close/Flush)
// =====================================================================
//
// Per-connection state for the extended protocol. Two kinds of slots:
//   - PreparedStmt: backs `Parse`; survives until explicit Close.
//   - Portal:       backs `Bind` (a prepared statement + parameter values +
//                   result format codes); survives until explicit Close or
//                   the connection ends. Holds the stepped sqlite3_stmt*.
//
// Empty name ("") is the "unnamed" slot per PG spec — re-used implicitly.
// `pending_error` is set when any extended-protocol op fails; subsequent ops
// are skipped until the next Sync, then it's cleared.

struct ExtPrepared {
    sqlite3_stmt* stmt = nullptr;
    int n_params = 0;
};

struct ExtPortal {
    sqlite3_stmt* stmt = nullptr;          // borrowed from ExtPrepared
    std::string stmt_name;                 // for tag generation
    std::vector<int16_t> result_formats;   // empty => all text
    bool active = false;                   // mid-iteration
    bool sent_header = false;              // RowDescription/NoData emitted
    int64_t nrows = 0;
};

struct ServerConn {
    sqlite3* db = nullptr;
    std::unordered_map<std::string, ExtPrepared> prepared;
    std::unordered_map<std::string, ExtPortal>   portals;
    bool pending_error = false;            // cleared by Sync
};

static void extClearError(ServerConn& sc) { sc.pending_error = false; }

static void extFlagError(ServerConn& sc, int fd,
                         const char* code, const char* msg) {
    sendErrorResponse(fd, "ERROR", code, msg);
    sc.pending_error = true;
}

static void closePrepared(ExtPrepared& p) {
    if (p.stmt) { sqlite3_finalize(p.stmt); p.stmt = nullptr; }
}

static void closePortal(ExtPortal& p) {
    // The portal borrows the stmt from a prepared slot — don't finalize here.
    p.stmt = nullptr;
    p.active = false;
}

// First word of a SQL string, uppercased — used to pick a command-complete
// tag (INSERT/UPDATE/DELETE/SELECT/...). Returns empty if SQL is blank.
static std::string firstWordUpper(const char* sql) {
    while (*sql == ' ' || *sql == '\t' || *sql == '\n' || *sql == '\r') sql++;
    std::string out;
    while (*sql && *sql != ' ' && *sql != '\t' && *sql != '\n' &&
           *sql != '\r' && *sql != ';') {
        char c = *sql++;
        if (c >= 'a' && c <= 'z') c = (char)(c - 32);
        out.push_back(c);
    }
    return out;
}

// PG Parse message:
//   [string: stmt_name][string: query][int16: n_param_types][n_param_types * int32: OID]
//
// We ignore declared parameter OIDs — SQLite is dynamically typed and we
// bind by ordinal. Returns 0 on success.
static int handleParse(ServerConn& sc, int fd,
                       const char* payload, int payloadLen) {
    if (sc.pending_error) return 0;
    const char* p = payload;
    const char* end = payload + payloadLen;
    auto readStr = [&](std::string& out) -> bool {
        const char* z = (const char*)memchr(p, 0, end - p);
        if (!z) return false;
        out.assign(p, z);
        p = z + 1;
        return true;
    };
    std::string name, query;
    if (!readStr(name) || !readStr(query)) {
        extFlagError(sc, fd, "08P01", "malformed Parse message");
        return 0;
    }
    // Skip parameter-type list — we don't use it.
    if (end - p < 2) {
        extFlagError(sc, fd, "08P01", "malformed Parse params");
        return 0;
    }
    int16_t nTypes;
    memcpy(&nTypes, p, 2); nTypes = (int16_t)ntohs(nTypes); p += 2;
    if (end - p < 4 * nTypes) {
        extFlagError(sc, fd, "08P01", "malformed Parse type OIDs");
        return 0;
    }

    std::string rewritten = oroPgRewriteQuery(query.c_str());
    sqlite3_stmt* stmt = nullptr;
    if (sqlite3_prepare_v2(sc.db, rewritten.c_str(), -1, &stmt, nullptr) != SQLITE_OK) {
        extFlagError(sc, fd, "42000", sqlite3_errmsg(sc.db));
        return 0;
    }
    auto it = sc.prepared.find(name);
    if (it != sc.prepared.end()) {
        closePrepared(it->second);
        sc.prepared.erase(it);
    }
    ExtPrepared ep;
    ep.stmt = stmt;
    ep.n_params = sqlite3_bind_parameter_count(stmt);
    sc.prepared[name] = ep;
    sendParseComplete(fd);
    return 0;
}

// PG Bind message:
//   [string: portal][string: stmt_name]
//   [int16: n_param_format_codes][n * int16: codes]
//   [int16: n_params][n * (int32 len + bytes)]
//   [int16: n_result_format_codes][n * int16: codes]
//
// We support text format for parameters (code 0). Binary INT2/4/8 are also
// decoded for psycopg/pgbench compatibility; anything else is rejected.
static int handleBind(ServerConn& sc, int fd,
                      const char* payload, int payloadLen) {
    if (sc.pending_error) return 0;
    const char* p = payload;
    const char* end = payload + payloadLen;
    auto readStr = [&](std::string& out) -> bool {
        const char* z = (const char*)memchr(p, 0, end - p);
        if (!z) return false;
        out.assign(p, z);
        p = z + 1;
        return true;
    };
    auto need = [&](size_t n) { return (size_t)(end - p) >= n; };

    std::string portalName, stmtName;
    if (!readStr(portalName) || !readStr(stmtName) || !need(2)) {
        extFlagError(sc, fd, "08P01", "malformed Bind");
        return 0;
    }
    auto pit = sc.prepared.find(stmtName);
    if (pit == sc.prepared.end()) {
        extFlagError(sc, fd, "26000",
                     ("prepared statement does not exist: \"" + stmtName + "\"").c_str());
        return 0;
    }
    sqlite3_stmt* stmt = pit->second.stmt;
    int n_params_expected = pit->second.n_params;

    int16_t nFmt;
    memcpy(&nFmt, p, 2); nFmt = (int16_t)ntohs(nFmt); p += 2;
    if (!need(2 * nFmt)) {
        extFlagError(sc, fd, "08P01", "malformed Bind param-format list");
        return 0;
    }
    std::vector<int16_t> paramFormats(nFmt);
    for (int i = 0; i < nFmt; i++) {
        int16_t v; memcpy(&v, p, 2); paramFormats[i] = (int16_t)ntohs(v); p += 2;
    }

    if (!need(2)) { extFlagError(sc, fd, "08P01", "malformed Bind nparams"); return 0; }
    int16_t nParams;
    memcpy(&nParams, p, 2); nParams = (int16_t)ntohs(nParams); p += 2;
    if (nParams != n_params_expected) {
        extFlagError(sc, fd, "08P01", "bind parameter count mismatch");
        return 0;
    }

    sqlite3_reset(stmt);
    sqlite3_clear_bindings(stmt);
    for (int i = 0; i < nParams; i++) {
        if (!need(4)) { extFlagError(sc, fd, "08P01", "Bind: short param length"); return 0; }
        int32_t len; memcpy(&len, p, 4); len = (int32_t)ntohl(len); p += 4;
        int16_t fmt = 0;
        if (paramFormats.size() == 1) fmt = paramFormats[0];
        else if ((size_t)i < paramFormats.size()) fmt = paramFormats[i];

        if (len < 0) {
            sqlite3_bind_null(stmt, i + 1);
            continue;
        }
        if (!need((size_t)len)) {
            extFlagError(sc, fd, "08P01", "Bind: short param data");
            return 0;
        }
        if (fmt == 0) {
            // Text format — pass the bytes through; SQLite reinterprets.
            sqlite3_bind_text(stmt, i + 1, p, len, SQLITE_TRANSIENT);
        } else if (fmt == 1) {
            // Binary: handle common integer widths. Unknown widths land as
            // BLOB (raw bytes) so they can still be retrieved.
            if (len == 8) {
                uint64_t v; memcpy(&v, p, 8);
                v = ((uint64_t)ntohl(v & 0xFFFFFFFF) << 32) |
                    ntohl((uint32_t)(v >> 32));
                sqlite3_bind_int64(stmt, i + 1, (int64_t)v);
            } else if (len == 4) {
                uint32_t v; memcpy(&v, p, 4); v = ntohl(v);
                sqlite3_bind_int(stmt, i + 1, (int32_t)v);
            } else if (len == 2) {
                uint16_t v; memcpy(&v, p, 2); v = ntohs(v);
                sqlite3_bind_int(stmt, i + 1, (int16_t)v);
            } else {
                sqlite3_bind_blob(stmt, i + 1, p, len, SQLITE_TRANSIENT);
            }
        } else {
            extFlagError(sc, fd, "08P01", "Bind: unknown format code");
            return 0;
        }
        p += len;
    }

    if (!need(2)) { extFlagError(sc, fd, "08P01", "Bind: result-format length"); return 0; }
    int16_t nRes;
    memcpy(&nRes, p, 2); nRes = (int16_t)ntohs(nRes); p += 2;
    if (!need(2 * nRes)) { extFlagError(sc, fd, "08P01", "Bind: result-format list"); return 0; }
    std::vector<int16_t> resultFormats(nRes);
    for (int i = 0; i < nRes; i++) {
        int16_t v; memcpy(&v, p, 2); resultFormats[i] = (int16_t)ntohs(v); p += 2;
    }

    auto pit2 = sc.portals.find(portalName);
    if (pit2 != sc.portals.end()) {
        closePortal(pit2->second);
        sc.portals.erase(pit2);
    }
    ExtPortal pt;
    pt.stmt = stmt;
    pt.stmt_name = stmtName;
    pt.result_formats = std::move(resultFormats);
    sc.portals[portalName] = pt;
    sendBindComplete(fd);
    return 0;
}

// PG Describe message:
//   [byte: 'S' or 'P'][string: name]
// 'S' describes a prepared statement: ParameterDescription + RowDescription/NoData
// 'P' describes a portal: RowDescription/NoData
static int handleDescribe(ServerConn& sc, int fd,
                          const char* payload, int payloadLen) {
    if (sc.pending_error) return 0;
    if (payloadLen < 2) { extFlagError(sc, fd, "08P01", "Describe: short"); return 0; }
    char kind = payload[0];
    std::string name(payload + 1);

    sqlite3_stmt* stmt = nullptr;
    if (kind == 'S') {
        auto it = sc.prepared.find(name);
        if (it == sc.prepared.end()) {
            extFlagError(sc, fd, "26000", "prepared statement does not exist");
            return 0;
        }
        stmt = it->second.stmt;
        // ParameterDescription — we don't track types, send int4 (23) for each.
        std::vector<int32_t> oids(it->second.n_params, 23);
        sendParameterDescription(fd, oids);
    } else if (kind == 'P') {
        auto it = sc.portals.find(name);
        if (it == sc.portals.end()) {
            extFlagError(sc, fd, "34000", "portal does not exist");
            return 0;
        }
        stmt = it->second.stmt;
    } else {
        extFlagError(sc, fd, "08P01", "Describe: unknown kind");
        return 0;
    }

    int ncols = sqlite3_column_count(stmt);
    if (ncols == 0) {
        sendNoData(fd);
    } else {
        std::vector<int16_t> fmts;
        if (kind == 'P') {
            auto it = sc.portals.find(name);
            if (it != sc.portals.end()) fmts = it->second.result_formats;
        }
        sendRowDescriptionTyped(fd, stmt, fmts);
    }
    return 0;
}

// PG Execute message:
//   [string: portal_name][int32: max_rows]
// max_rows == 0 means "no limit".
static int handleExecute(ServerConn& sc, int fd,
                         const char* payload, int payloadLen) {
    if (sc.pending_error) return 0;
    const char* z = (const char*)memchr(payload, 0, payloadLen);
    if (!z || (payload + payloadLen) - (z + 1) < 4) {
        extFlagError(sc, fd, "08P01", "Execute: malformed");
        return 0;
    }
    std::string portalName(payload, z);
    int32_t maxRows;
    memcpy(&maxRows, z + 1, 4); maxRows = (int32_t)ntohl(maxRows);

    auto it = sc.portals.find(portalName);
    if (it == sc.portals.end()) {
        extFlagError(sc, fd, "34000", "portal does not exist");
        return 0;
    }
    ExtPortal& pt = it->second;
    sqlite3_stmt* stmt = pt.stmt;
    int ncols = sqlite3_column_count(stmt);

    if (ncols == 0) {
        // DDL/DML — step once, emit CommandComplete.
        int rc = sqlite3_step(stmt);
        if (rc != SQLITE_DONE && rc != SQLITE_ROW) {
            extFlagError(sc, fd, "42000", sqlite3_errmsg(sc.db));
            return 0;
        }
        const char* sql = sqlite3_sql(stmt);
        std::string fw = sql ? firstWordUpper(sql) : "";
        char buf[64];
        const char* tag = "OK";
        if (fw == "INSERT") {
            snprintf(buf, sizeof(buf), "INSERT 0 %d", sqlite3_changes(sc.db));
            tag = buf;
        } else if (fw == "UPDATE") {
            snprintf(buf, sizeof(buf), "UPDATE %d", sqlite3_changes(sc.db));
            tag = buf;
        } else if (fw == "DELETE") {
            snprintf(buf, sizeof(buf), "DELETE %d", sqlite3_changes(sc.db));
            tag = buf;
        } else if (fw == "CREATE") tag = "CREATE TABLE";
        else if (fw == "DROP")    tag = "DROP TABLE";
        else if (fw == "BEGIN")   tag = "BEGIN";
        else if (fw == "COMMIT")  tag = "COMMIT";
        else if (fw == "ROLLBACK") tag = "ROLLBACK";
        sendCommandComplete(fd, tag);
        sqlite3_reset(stmt);
        return 0;
    }

    // SELECT — stream rows. Respect maxRows; emit PortalSuspended when more.
    while (true) {
        if (maxRows > 0 && pt.nrows >= maxRows) {
            sendPortalSuspended(fd);
            pt.active = true;
            return 0;
        }
        int rc = sqlite3_step(stmt);
        if (rc == SQLITE_ROW) {
            int nc = ncols;
            std::vector<std::string> valStore(nc);
            std::vector<const char*> vals(nc);
            for (int i = 0; i < nc; i++) {
                int type = sqlite3_column_type(stmt, i);
                if (type == SQLITE_NULL) {
                    vals[i] = nullptr;
                } else {
                    const char* v = (const char*)sqlite3_column_text(stmt, i);
                    valStore[i] = v ? v : "";
                    vals[i] = valStore[i].c_str();
                }
            }
            if (!sendDataRow(fd, nc, vals.data())) return 0;
            pt.nrows++;
        } else if (rc == SQLITE_DONE) {
            char buf[64];
            snprintf(buf, sizeof(buf), "SELECT %ld", (long)pt.nrows);
            sendCommandComplete(fd, buf);
            sqlite3_reset(stmt);
            pt.active = false;
            pt.nrows = 0;
            return 0;
        } else {
            extFlagError(sc, fd, "42000", sqlite3_errmsg(sc.db));
            sqlite3_reset(stmt);
            return 0;
        }
    }
}

// PG Close message:
//   [byte: 'S' or 'P'][string: name]
static int handleClose(ServerConn& sc, int fd,
                       const char* payload, int payloadLen) {
    if (payloadLen < 2) { extFlagError(sc, fd, "08P01", "Close: short"); return 0; }
    char kind = payload[0];
    std::string name(payload + 1);
    if (kind == 'S') {
        auto it = sc.prepared.find(name);
        if (it != sc.prepared.end()) {
            // Also drop any portals that borrow this stmt.
            for (auto pit = sc.portals.begin(); pit != sc.portals.end();) {
                if (pit->second.stmt == it->second.stmt) {
                    closePortal(pit->second);
                    pit = sc.portals.erase(pit);
                } else {
                    ++pit;
                }
            }
            closePrepared(it->second);
            sc.prepared.erase(it);
        }
    } else if (kind == 'P') {
        auto it = sc.portals.find(name);
        if (it != sc.portals.end()) {
            closePortal(it->second);
            sc.portals.erase(it);
        }
    }
    sendCloseComplete(fd);
    return 0;
}

// =====================================================================
// Client connection handler
// =====================================================================

static void handleClient(int fd, const char* dbPath) {
    // --- Startup phase ---
    // Read startup message (no type byte): [4:length][4:version][params...]
    int32_t msgLen = readInt32(fd);
    if (msgLen < 8 || msgLen > 10000) { close(fd); return; }

    std::vector<uint8_t> buf(msgLen - 4);
    if (!readFull(fd, buf.data(), buf.size())) { close(fd); return; }

    // Check protocol version
    uint32_t version;
    memcpy(&version, buf.data(), 4);
    version = ntohl(version);

    // Handle SSL request (80877103 = 1234.5679)
    if (version == 80877103) {
        // Reject SSL: send 'N'
        char n = 'N';
        write(fd, &n, 1);
        // Client will retry with normal startup
        msgLen = readInt32(fd);
        if (msgLen < 8 || msgLen > 10000) { close(fd); return; }
        buf.resize(msgLen - 4);
        if (!readFull(fd, buf.data(), buf.size())) { close(fd); return; }
        memcpy(&version, buf.data(), 4);
        version = ntohl(version);
    }

    if ((version >> 16) != 3) {
        sendErrorResponse(fd, "FATAL", "08004", "unsupported protocol version");
        close(fd);
        return;
    }

    // Parse startup parameters (just log them)
    // params start at offset 4, null-terminated key-value pairs
    std::string user = "unknown";
    size_t pos = 4;
    while (pos < buf.size() && buf[pos] != 0) {
        const char* key = (const char*)&buf[pos];
        pos += strlen(key) + 1;
        if (pos >= buf.size()) break;
        const char* val = (const char*)&buf[pos];
        pos += strlen(val) + 1;
        if (strcmp(key, "user") == 0) user = val;
    }

    fprintf(stderr, "[oro-server] client connected: user=%s\n", user.c_str());

    // Auth: if a users file was loaded at startup, require MD5 password.
    // Otherwise (no --users flag), accept the connection unconditionally.
    if (g_users.enabled) {
        uint8_t salt[4];
        // Mix in /dev/urandom for the salt. If reading fails we fall back to
        // a timestamp-based seed — neither is a substitute for proper SCRAM
        // (see GAPS.md #5), but it's enough to defeat replay attacks.
        FILE* rnd = fopen("/dev/urandom", "rb");
        if (!rnd || fread(salt, 1, 4, rnd) != 4) {
            uint32_t t = (uint32_t)time(nullptr);
            memcpy(salt, &t, 4);
        }
        if (rnd) fclose(rnd);
        sendAuthMd5(fd, salt);

        // Read the client's PasswordMessage ('p').
        uint8_t mt;
        if (!readFull(fd, &mt, 1) || mt != 'p') {
            sendErrorResponse(fd, "FATAL", "28000",
                              "expected password message");
            close(fd); return;
        }
        int32_t bl = readInt32(fd);
        if (bl < 4 || bl > 10000) { close(fd); return; }
        std::vector<char> pw(bl - 4 + 1);
        if (bl > 4 && !readFull(fd, pw.data(), bl - 4)) { close(fd); return; }
        pw[bl - 4] = '\0';

        auto it = g_users.md5_hashes.find(user);
        bool ok = false;
        if (it != g_users.md5_hashes.end()) {
            std::string expect = oroAuthExpectedResponse(it->second, salt);
            ok = (!expect.empty() && expect == std::string(pw.data()));
        }
        if (!ok) {
            sendErrorResponse(fd, "FATAL", "28P01",
                              "password authentication failed");
            close(fd);
            return;
        }
    }

    // Send auth OK + startup parameters
    sendAuthOk(fd);
    sendParameterStatus(fd, "server_version", "15.0 (oro-db MOT)");
    sendParameterStatus(fd, "server_encoding", "UTF8");
    sendParameterStatus(fd, "client_encoding", "UTF8");
    sendParameterStatus(fd, "DateStyle", "ISO, MDY");
    sendParameterStatus(fd, "integer_datetimes", "on");
    sendBackendKeyData(fd, getpid(), 0);
    sendReadyForQuery(fd, 'I');

    // --- Open a per-connection SQLite database ---
    sqlite3* db = nullptr;
    int rc = sqlite3_open(dbPath, &db);
    if (rc != SQLITE_OK) {
        sendErrorResponse(fd, "FATAL", "08001", "failed to open database");
        close(fd);
        return;
    }

    // Enable FK enforcement
    sqlite3_exec(db, "PRAGMA foreign_keys = ON", nullptr, nullptr, nullptr);

    // Concurrency: SQLite WAL mode lets readers and one writer coexist,
    // dramatically improving throughput under multiple clients. busy_timeout
    // makes writers wait briefly instead of returning SQLITE_BUSY immediately.
    if (strcmp(dbPath, ":memory:") != 0) {
        sqlite3_exec(db, "PRAGMA journal_mode = WAL", nullptr, nullptr, nullptr);
    }
    sqlite3_exec(db, "PRAGMA busy_timeout = 5000", nullptr, nullptr, nullptr);

    // Enable MOT WAL persistence for file-backed databases
    if (strcmp(dbPath, ":memory:") != 0) {
        oroMotWalEnable(db);
        oroMotWalRecover(db);
    }

    // Set up PostgreSQL catalog compatibility (so psql \dt etc. work)
    oroPgCatalogInit(db);

    // Per-connection extended-protocol state.
    ServerConn sc;
    sc.db = db;

    // --- Query loop ---
    while (true) {
        // Read message: [1:type][4:length][payload]
        uint8_t msgType;
        if (!readFull(fd, &msgType, 1)) break;

        int32_t bodyLen = readInt32(fd);
        if (bodyLen < 4) break;

        int32_t payloadLen = bodyLen - 4;
        std::vector<char> payload(payloadLen + 1);
        if (payloadLen > 0) {
            if (!readFull(fd, payload.data(), payloadLen)) break;
        }
        payload[payloadLen] = '\0';

        switch (msgType) {
            case 'Q':
                // Simple query
                handleQuery(fd, db, payload.data());
                break;

            case 'P':
                handleParse(sc, fd, payload.data(), payloadLen);
                break;
            case 'B':
                handleBind(sc, fd, payload.data(), payloadLen);
                break;
            case 'D':
                handleDescribe(sc, fd, payload.data(), payloadLen);
                break;
            case 'E':
                handleExecute(sc, fd, payload.data(), payloadLen);
                break;
            case 'C':
                handleClose(sc, fd, payload.data(), payloadLen);
                break;
            case 'S':
                // Sync — clear extended-protocol error state, emit ReadyForQuery.
                extClearError(sc);
                sendReadyForQuery(fd, 'I');
                break;
            case 'H':
                // Flush — no-op for us (writes are unbuffered).
                break;

            case 'X':
                // Terminate
                goto done;

            case 'p':
                // Password message (we don't require auth)
                sendAuthOk(fd);
                sendReadyForQuery(fd, 'I');
                break;

            default:
                // Unknown message — send error but keep connection
                sendErrorResponse(fd, "ERROR", "0A000",
                    "unsupported message type");
                sendReadyForQuery(fd, 'I');
                break;
        }
    }

done:
    fprintf(stderr, "[oro-server] client disconnected: user=%s\n", user.c_str());
    // Tear down extended-protocol state.
    for (auto& kv : sc.portals) closePortal(kv.second);
    for (auto& kv : sc.prepared) closePrepared(kv.second);
    sqlite3_close(db);
    close(fd);
}

// =====================================================================
// Main
// =====================================================================

static volatile sig_atomic_t g_running = 1;

static void sigHandler(int) { g_running = 0; }

int main(int argc, char* argv[]) {
    int port = 5432;
    const char* dbPath = ":memory:";
    const char* motConfig = nullptr;
    const char* usersFile = nullptr;

    // Parse arguments
    for (int i = 1; i < argc; i++) {
        if (strcmp(argv[i], "--port") == 0 && i + 1 < argc) {
            port = atoi(argv[++i]);
        } else if (strcmp(argv[i], "--db") == 0 && i + 1 < argc) {
            dbPath = argv[++i];
        } else if (strcmp(argv[i], "--mot-config") == 0 && i + 1 < argc) {
            motConfig = argv[++i];
        } else if (strcmp(argv[i], "--users") == 0 && i + 1 < argc) {
            usersFile = argv[++i];
        } else if (strcmp(argv[i], "--help") == 0 || strcmp(argv[i], "-h") == 0) {
            fprintf(stderr,
                "Usage: %s [options]\n"
                "  --port N          Listen port (default: 5432)\n"
                "  --db PATH         SQLite database file (default: :memory:)\n"
                "  --mot-config PATH MOT engine config file\n"
                "  --users PATH      Users file (user/password lines).\n"
                "                    Enables MD5 password authentication.\n"
                "                    Without this flag, all clients are\n"
                "                    accepted (trust mode).\n"
                "\nConnect with: psql -h localhost -p N\n", argv[0]);
            return 0;
        }
    }

    if (usersFile) {
        if (!oroAuthLoadUsers(usersFile, g_users)) {
            fprintf(stderr, "FATAL: failed to load users file: %s\n", usersFile);
            return 1;
        }
        fprintf(stderr, "[oro-server] auth: MD5 with %zu users\n",
                g_users.md5_hashes.size());
    } else {
        fprintf(stderr, "[oro-server] auth: trust mode (no --users)\n");
    }

    // Auto-detect mot.conf
    char cfgBuf[PATH_MAX];
    if (!motConfig) {
        char exePath[PATH_MAX];
        ssize_t len = readlink("/proc/self/exe", exePath, sizeof(exePath) - 1);
        if (len > 0) {
            exePath[len] = '\0';
            snprintf(cfgBuf, sizeof(cfgBuf), "%s/mot.conf", dirname(exePath));
            motConfig = cfgBuf;
        }
    }

    // Initialize MOT engine
    if (oroMotInit(motConfig) != 0) {
        fprintf(stderr, "FATAL: failed to initialize MOT engine\n");
        return 1;
    }

    // Create listening socket
    int listenFd = socket(AF_INET, SOCK_STREAM, 0);
    if (listenFd < 0) { perror("socket"); return 1; }

    int opt = 1;
    setsockopt(listenFd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    struct sockaddr_in addr = {};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = INADDR_ANY;
    addr.sin_port = htons(port);

    if (bind(listenFd, (struct sockaddr*)&addr, sizeof(addr)) < 0) {
        perror("bind");
        close(listenFd);
        return 1;
    }

    if (listen(listenFd, 8) < 0) {
        perror("listen");
        close(listenFd);
        return 1;
    }

    signal(SIGINT, sigHandler);
    signal(SIGTERM, sigHandler);
    signal(SIGPIPE, SIG_IGN);

    fprintf(stderr, "oro-db server listening on port %d\n", port);
    fprintf(stderr, "Connect with: psql -h localhost -p %d\n\n", port);

    while (g_running) {
        struct sockaddr_in clientAddr;
        socklen_t clientLen = sizeof(clientAddr);
        int clientFd = accept(listenFd, (struct sockaddr*)&clientAddr, &clientLen);
        if (clientFd < 0) {
            if (g_running) perror("accept");
            continue;
        }

        // Disable Nagle for snappy responses
        int flag = 1;
        setsockopt(clientFd, IPPROTO_TCP, TCP_NODELAY, &flag, sizeof(flag));

        // One thread per connection
        std::thread([clientFd, dbPath]() {
            handleClient(clientFd, dbPath);
        }).detach();
    }

    fprintf(stderr, "\nShutting down...\n");
    close(listenFd);
    oroMotShutdown();
    return 0;
}
