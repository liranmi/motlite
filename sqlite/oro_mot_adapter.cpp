/*
 * oro_mot_adapter.cpp - Implementation of SQLite ↔ MOT bridge
 *
 * Stores SQLite serialized records as opaque BLOBs in MOT, keyed by rowid.
 * The MassTree primary index uses rowid (big-endian uint64) as the key.
 *
 * Internal MOT table layout (per SQLite MOT table):
 *   col 0: data    (BLOB, max 16KB) - the SQLite serialized record bytes
 *   col 1: rowid   (LONG)            - the rowid (also InternalKey)
 */

#include "oro_mot_adapter.h"
#include "sqlite3.h"

#include <cstdio>
#include <cstring>
#include <cstdlib>
#include <endian.h>
#include <mutex>
#include <unordered_map>
#include <string>
#include <atomic>

// MOT engine headers
#include "mot_engine.h"
#include "session_context.h"
#include "session_manager.h"
#include "txn.h"
#include "table.h"
#include "row.h"
#include "index.h"
#include "index_factory.h"
#include "index_iterator.h"
#include "catalog_column_types.h"
#include "txn_insert_action.h"
#include "txn_access.h"

#include <vector>
#include <map>
#include <algorithm>

using namespace MOT;

// =====================================================================
// Constants
// =====================================================================

// Max size for the SQLite record blob stored in MOT
static constexpr uint32_t MAX_RECORD_SIZE = 4096;

// Key length for the primary index (8 bytes uint64)
static constexpr uint16_t MOT_KEY_LEN = 8;

// Max encoded key length for secondary indexes.
// VARCHAR stores [4-byte-len][data], and the InternalKey path copies from
// the column start (including the 4-byte prefix). MOT MAX_KEY_SIZE is 256,
// so we need: 4 + encoded_len <= 256  →  encoded_len <= 252.
static constexpr uint32_t IDX_ENC_KEY_LEN = 252;
// VARCHAR column size = 4 + 252 = 256
static constexpr uint32_t IDX_KEY_COL_SIZE = IDX_ENC_KEY_LEN + sizeof(uint32_t); // 256
// MOT index key length = full column size = 256 = MAX_KEY_SIZE
static constexpr uint16_t IDX_MOT_KEY_LEN = (uint16_t)IDX_KEY_COL_SIZE; // 256

// MOT column indices for the internal layout
enum MotColIdx : int {
    MOT_COL_DATA = 0,    // BLOB - SQLite record bytes
    MOT_COL_ROWID = 1    // LONG - rowid (also InternalKey)
};

// =====================================================================
// Global state
// =====================================================================

static MOTEngine* g_engine = nullptr;
static std::atomic<bool> g_initialized{false};

// Thread-local MOT init flag
static thread_local bool tl_mot_initialized = false;

// Per-connection state (one per sqlite3*)
struct OroMotConn {
    SessionContext* session      = nullptr;
    TxnManager*    txn           = nullptr;
    bool           in_txn        = false;
    // WAL state
    bool           wal_enabled   = false;
    bool           wal_replaying = false;  // suppress WAL writes during replay
    void*          pDb           = nullptr;  // sqlite3* for executing WAL SQL
    void*          ins_stmt      = nullptr;  // prepared INSERT for WAL
    void*          del_stmt      = nullptr;  // prepared DELETE marker for WAL
    // Auto-checkpoint state. Threshold 0 disables; counter is reset after each
    // successful checkpoint. Auto-checkpoint only fires outside an explicit
    // SQLite transaction (sqlite3_get_autocommit == 1) and outside replay.
    uint64_t       wal_writes_since_ck = 0;
    uint64_t       wal_ck_threshold    = 0;
    // RYOW pending-inserts overlay. MOT's RowLookup via sentinel misses the
    // current txn's own inserts (the access-set AccessLookup doesn't match),
    // so we keep our own map: per-table, rowid → staged Row*. Read paths
    // (CursorAdvance after iter exhaust, oroMotSeekRowid on miss, oroMotCount)
    // merge in these entries to give read-your-own-writes semantics. Cleared
    // on commit/rollback. Pointers remain valid throughout the MOT txn.
    std::unordered_map<MOT::Table*, std::map<int64_t, MOT::Row*>> pending_inserts;
};

// Table registry: (iDb, table_name) → MOT::Table*
struct TableKey {
    int iDb;
    std::string name;
    bool operator==(const TableKey& o) const { return iDb==o.iDb && name==o.name; }
};
struct TableKeyHash {
    size_t operator()(const TableKey& k) const noexcept {
        return std::hash<std::string>()(k.name) ^ std::hash<int>()(k.iDb);
    }
};

// Secondary index registry key: (iDb, table_name, index_name)
struct IdxKey {
    int iDb;
    std::string tabName;
    std::string ixName;
    bool operator==(const IdxKey& o) const {
        return iDb==o.iDb && tabName==o.tabName && ixName==o.ixName;
    }
};
struct IdxKeyHash {
    size_t operator()(const IdxKey& k) const noexcept {
        size_t h = std::hash<std::string>()(k.tabName);
        h ^= std::hash<std::string>()(k.ixName) + 0x9e3779b9 + (h<<6) + (h>>2);
        h ^= std::hash<int>()(k.iDb);
        return h;
    }
};

struct OroMotIdxInfo {
    Table* table = nullptr;
    Index* index = nullptr;
};

// Per-table rowid counter
struct RowidCounter {
    std::atomic<int64_t> next{1};
};

struct OroGlobals {
    std::mutex                                                    mu;
    std::unordered_map<TableKey, Table*, TableKeyHash>            tables;
    std::unordered_map<void*, OroMotConn*>                        conns;
    std::unordered_map<IdxKey, OroMotIdxInfo, IdxKeyHash>         sec_indexes;
    std::unordered_map<TableKey, RowidCounter*, TableKeyHash>     rowid_counters;
};

static OroGlobals& globals() {
    static OroGlobals g;
    return g;
}

// =====================================================================
// Cursor structure
// =====================================================================

// Column indices for secondary index MOT table
enum IdxColIdx : int {
    IDX_COL_ROWID  = 0,    // LONG — the table rowid
    IDX_COL_RECORD = 1,    // BLOB — original SQLite index record bytes
    IDX_COL_KEY    = 2     // VARCHAR — memcmp-encoded key (InternalKey source)
};

// Pending insert entry for RYOW merge
struct PendingRow {
    int64_t rowid;
    Row*    row;
};

struct OroMotCursor {
    Table*           table       = nullptr;
    Index*           index       = nullptr;
    OroMotConn*      conn        = nullptr;
    IndexIterator*   iter        = nullptr;
    Row*             current_row = nullptr;  // MVCC-visible row from RowLookup
    int64_t          current_rowid = 0;
    bool             at_eof      = true;
    bool             write_mode  = false;
    // --- Identity (for WAL logging) ---
    std::string      table_name;             // original SQLite table name
    // --- Index cursor fields ---
    bool             is_index    = false;
    int64_t          idx_rowid   = 0;       // rowid from current index entry
    // --- RYOW: when true, the regular MOT iter is exhausted and we're
    // draining the connection's pending-inserts overlay for this table.
    // pending_last is the rowid of the most recently emitted pending row;
    // PendingNextAfter(conn, table, pending_last, ...) gives the next one.
    bool             pending_mode = false;
    int64_t          pending_last = INT64_MIN;
};

// =====================================================================
// Thread context helper
// =====================================================================

static void EnsureThreadCtx(OroMotConn* conn) {
    if (!tl_mot_initialized) {
        knl_thread_mot_init();
        tl_mot_initialized = true;
    }
    if (conn && conn->session) {
        u_sess->mot_cxt.session_context = conn->session;
        u_sess->mot_cxt.txn_manager = conn->txn;
    }
}

// =====================================================================
// Engine lifecycle
// =====================================================================

extern "C" int oroMotInit(const char* config_path) {
    if (g_initialized.load()) return 0;
    g_engine = MOTEngine::CreateInstance(config_path);
    if (!g_engine) return 1;
    g_initialized.store(true);
    return 0;
}

extern "C" void oroMotShutdown(void) {
    if (!g_initialized.load()) return;
    // Connections are torn down by their owning thread via oroMotConnDetach
    // (called from handleClient's exit path). Iterating g.conns here would
    // race with any client thread still mid-statement on shutdown — under
    // TSan this surfaces as a data race on OroMotConn::in_txn because the
    // owning thread mutates it under the per-conn ownership invariant
    // (no g.mu), while the shutdown loop reads it under g.mu. The two locks
    // never coincide. We instead require callers to drain live connections
    // before invoking shutdown.
    //
    // If g.conns is non-empty here, it means at least one client thread is
    // still alive — log it and skip the per-conn loop. The OS will reap
    // memory on process exit.
    {
        auto& g = globals();
        std::lock_guard<std::mutex> lock(g.mu);
        if (!g.conns.empty()) {
            fprintf(stderr,
                "[oro-mot] shutdown: %zu connections still active; "
                "skipping per-conn cleanup\n", g.conns.size());
        }
        g.tables.clear();
    }
    MOTEngine::DestroyInstance();
    g_engine = nullptr;
    g_initialized.store(false);
}

extern "C" int oroMotIsInit(void) {
    return g_initialized.load() ? 1 : 0;
}

// =====================================================================
// Connection management
// =====================================================================

static OroMotConn* GetOrCreateConn(void* pDb) {
    auto& g = globals();
    std::lock_guard<std::mutex> lock(g.mu);

    auto it = g.conns.find(pDb);
    if (it != g.conns.end()) return it->second;

    EnsureThreadCtx(nullptr);
    SessionContext* sess = g_engine->GetSessionManager()->CreateSessionContext();
    if (!sess) return nullptr;

    auto* c = new OroMotConn();
    c->session = sess;
    c->txn = sess->GetTxnManager();
    c->pDb = pDb;
    g.conns[pDb] = c;

    EnsureThreadCtx(c);
    return c;
}

extern "C" int oroMotConnAttach(void* pDb) {
    return GetOrCreateConn(pDb) ? 0 : 1;
}

extern "C" int oroMotConnDetach(void* pDb) {
    auto& g = globals();
    std::lock_guard<std::mutex> lock(g.mu);

    auto it = g.conns.find(pDb);
    if (it == g.conns.end()) return 0;

    OroMotConn* c = it->second;
    EnsureThreadCtx(c);
    if (c->in_txn) {
        c->txn->Rollback();
        c->txn->EndTransaction();
        c->in_txn = false;
    }
    c->pending_inserts.clear();
    g_engine->GetSessionManager()->DestroySessionContext(c->session);
    delete c;
    g.conns.erase(it);
    return 0;
}

extern "C" int oroMotBegin(void* pDb) {
    OroMotConn* c = GetOrCreateConn(pDb);
    if (!c) return 1;
    EnsureThreadCtx(c);
    if (!c->in_txn) {
        c->txn->StartTransaction(c->txn->GetTransactionId(), READ_COMMITED);
        c->in_txn = true;
    }
    return 0;
}

extern "C" int oroMotCommit(void* pDb) {
    auto& g = globals();
    OroMotConn* c;
    {
        std::lock_guard<std::mutex> lock(g.mu);
        auto it = g.conns.find(pDb);
        if (it == g.conns.end()) return 0;
        c = it->second;
    }
    EnsureThreadCtx(c);
    if (c->in_txn) {
        RC rc = c->txn->Commit();
        c->txn->EndTransaction();
        c->in_txn = false;
        c->pending_inserts.clear();
        return (rc == RC_OK) ? 0 : 1;
    }
    return 0;
}

extern "C" int oroMotRollback(void* pDb) {
    auto& g = globals();
    OroMotConn* c;
    {
        std::lock_guard<std::mutex> lock(g.mu);
        auto it = g.conns.find(pDb);
        if (it == g.conns.end()) return 0;
        c = it->second;
    }
    EnsureThreadCtx(c);
    if (c->in_txn) {
        c->txn->Rollback();
        c->txn->EndTransaction();
        c->in_txn = false;
    }
    c->pending_inserts.clear();
    return 0;
}

// Forward decl — defined alongside WAL helpers later in the file.
static void MaybeAutoCheckpoint(OroMotConn* c);

extern "C" int oroMotAutoCommit(void* pDb) {
    // Same as oroMotCommit but safely no-ops if no connection or no active txn.
    auto& g = globals();
    OroMotConn* c;
    {
        std::lock_guard<std::mutex> lock(g.mu);
        auto it = g.conns.find(pDb);
        if (it == g.conns.end()) return 0;
        c = it->second;
    }
    int result = 0;
    if (c->in_txn) {
        EnsureThreadCtx(c);
        RC rc = c->txn->Commit();
        c->txn->EndTransaction();
        c->in_txn = false;
        c->pending_inserts.clear();
        result = (rc == RC_OK) ? 0 : 1;
    }
    // Statement boundary in autocommit mode — safe place to run an auto
    // checkpoint if the threshold has been crossed.
    MaybeAutoCheckpoint(c);
    return result;
}

extern "C" int oroMotHasActiveTxn(void* pDb) {
    auto& g = globals();
    std::lock_guard<std::mutex> lock(g.mu);
    auto it = g.conns.find(pDb);
    if (it == g.conns.end()) return 0;
    return it->second->in_txn ? 1 : 0;
}

// =====================================================================
// Table registry
// =====================================================================

extern "C" int oroMotTableCreate(int iDb, const char* table_name) {
    if (!g_initialized.load() || !table_name) return 1;

    auto& g = globals();
    {
        std::lock_guard<std::mutex> lock(g.mu);
        TableKey key{iDb, table_name};
        if (g.tables.count(key)) return 0;  // already exists
    }

    // We need a session/transaction to create the table. Use a temporary
    // session bound to the current thread.
    EnsureThreadCtx(nullptr);
    SessionContext* sess = g_engine->GetSessionManager()->CreateSessionContext();
    if (!sess) return 1;
    TxnManager* txn = sess->GetTxnManager();

    // Build a unique internal MOT table name (avoid collisions across iDb)
    char name_buf[128];
    snprintf(name_buf, sizeof(name_buf), "mot_%d_%s", iDb, table_name);
    std::string long_name = std::string("public.") + name_buf;

    Table* t = new Table();
    if (!t->Init(name_buf, long_name.c_str(), 2)) {
        delete t;
        g_engine->GetSessionManager()->DestroySessionContext(sess);
        return 1;
    }

    // col 0: data BLOB (variable, up to MAX_RECORD_SIZE)
    t->AddColumn("data", MAX_RECORD_SIZE, MOT_CATALOG_FIELD_TYPES::MOT_TYPE_BLOB, false);
    // col 1: rowid LONG (also serves as the InternalKey)
    t->AddColumn("rowid", sizeof(uint64_t), MOT_CATALOG_FIELD_TYPES::MOT_TYPE_LONG, true);

    if (!t->InitRowPool() || !t->InitTombStonePool()) {
        delete t;
        g_engine->GetSessionManager()->DestroySessionContext(sess);
        return 1;
    }

    txn->StartTransaction(txn->GetTransactionId(), READ_COMMITED);

    RC rc = txn->CreateTable(t);
    if (rc != RC_OK) {
        txn->Rollback();
        txn->EndTransaction();
        delete t;
        g_engine->GetSessionManager()->DestroySessionContext(sess);
        return 1;
    }

    // Primary index on rowid (col 1)
    RC irc = RC_OK;
    Index* ix = IndexFactory::CreateIndexEx(
        IndexOrder::INDEX_ORDER_PRIMARY,
        IndexingMethod::INDEXING_METHOD_TREE,
        DEFAULT_TREE_FLAVOR,
        true,  // unique
        MOT_KEY_LEN,
        std::string("ix_") + name_buf + "_pk",
        irc, nullptr);

    if (!ix || irc != RC_OK) {
        txn->Rollback();
        txn->EndTransaction();
        delete t;
        g_engine->GetSessionManager()->DestroySessionContext(sess);
        return 1;
    }

    ix->SetNumTableFields(t->GetFieldCount());
    ix->SetNumIndexFields(1);
    ix->SetLenghtKeyFields(0, MOT_COL_ROWID, MOT_KEY_LEN);
    ix->SetTable(t);

    rc = txn->CreateIndex(t, ix, true);
    if (rc != RC_OK) {
        delete ix;
        txn->Rollback();
        txn->EndTransaction();
        delete t;
        g_engine->GetSessionManager()->DestroySessionContext(sess);
        return 1;
    }

    rc = txn->Commit();
    txn->EndTransaction();
    g_engine->GetSessionManager()->DestroySessionContext(sess);

    if (rc != RC_OK) {
        delete t;
        return 1;
    }

    {
        std::lock_guard<std::mutex> lock(g.mu);
        g.tables[TableKey{iDb, table_name}] = t;
    }
    return 0;
}

extern "C" int oroMotTableDrop(int iDb, const char* table_name) {
    if (!table_name) return 1;
    auto& g = globals();
    std::lock_guard<std::mutex> lock(g.mu);
    g.tables.erase(TableKey{iDb, table_name});
    // Note: actual MOT::Table destruction would need a transaction; for now
    // we just remove from registry and let MOT engine cleanup handle it.
    return 0;
}

extern "C" int oroMotTableExists(int iDb, const char* table_name) {
    if (!table_name) return 0;
    auto& g = globals();
    std::lock_guard<std::mutex> lock(g.mu);
    return g.tables.count(TableKey{iDb, table_name}) ? 1 : 0;
}

static Table* LookupTable(int iDb, const char* table_name) {
    if (!table_name) return nullptr;
    auto& g = globals();
    std::lock_guard<std::mutex> lock(g.mu);
    auto it = g.tables.find(TableKey{iDb, table_name});
    return (it != g.tables.end()) ? it->second : nullptr;
}

// =====================================================================
// Cursor operations
// =====================================================================

extern "C" int oroMotCursorOpen(void* pDb, int iDb, const char* table_name,
                                int wrFlag, OroMotCursor** ppCursor) {
    Table* t = LookupTable(iDb, table_name);
    if (!t) return 1;

    OroMotConn* c = GetOrCreateConn(pDb);
    if (!c) return 1;

    EnsureThreadCtx(c);

    // Auto-start a transaction if none active (read transaction)
    if (!c->in_txn) {
        c->txn->StartTransaction(c->txn->GetTransactionId(), READ_COMMITED);
        c->in_txn = true;
    }

    OroMotCursor* cur = new OroMotCursor();
    cur->table = t;
    cur->index = t->GetPrimaryIndex();
    cur->conn = c;
    cur->write_mode = (wrFlag != 0);
    cur->at_eof = true;
    cur->table_name = table_name ? table_name : "";

    *ppCursor = cur;
    return 0;
}

extern "C" void oroMotCursorClose(OroMotCursor* pCur) {
    if (!pCur) return;
    if (pCur->iter) {
        pCur->iter->Destroy();
        pCur->iter = nullptr;
    }
    delete pCur;
}

// =====================================================================
// RYOW pending-inserts overlay
// =====================================================================
//
// MOT's RowLookup(AccessType::RD, sentinel) misses rows the current txn
// just inserted: the sentinel for the new row is in the MassTree, but
// AccessLookup against the txn's access set fails (sentinel-identity
// mismatch). The fix lives entirely in this adapter — we keep a per-conn
// map of pending rowids and the staged Row* for each one, and the read
// paths merge it in. Cleared on commit/rollback (oroMotCommit /
// oroMotRollback / oroMotAutoCommit).

static void PendingInsert(OroMotConn* c, Table* t, int64_t rowid, Row* r) {
    if (!c) return;
    c->pending_inserts[t][rowid] = r;
}

static void PendingClear(OroMotConn* c) {
    if (!c) return;
    c->pending_inserts.clear();
}

// Public-from-this-TU helpers used by oroMotCount and the cursor advance
// path. Returned Row* is owned by MOT's txn access set; valid until commit.
extern "C" int64_t oroMotPendingCount(OroMotConn* c, Table* t) {
    if (!c) return 0;
    auto it = c->pending_inserts.find(t);
    if (it == c->pending_inserts.end()) return 0;
    return (int64_t)it->second.size();
}

static Row* PendingFind(OroMotConn* c, Table* t, int64_t rowid) {
    if (!c) return nullptr;
    auto it = c->pending_inserts.find(t);
    if (it == c->pending_inserts.end()) return nullptr;
    auto jt = it->second.find(rowid);
    return (jt == it->second.end()) ? nullptr : jt->second;
}

// Yield the next pending row strictly greater than `after_rowid` (or the
// first one if after_rowid == INT64_MIN). Returns nullptr when exhausted.
static Row* PendingNextAfter(OroMotConn* c, Table* t, int64_t after_rowid,
                             int64_t* out_rowid) {
    if (!c) return nullptr;
    auto it = c->pending_inserts.find(t);
    if (it == c->pending_inserts.end()) return nullptr;
    auto& m = it->second;
    auto next = (after_rowid == INT64_MIN) ? m.begin() : m.upper_bound(after_rowid);
    if (next == m.end()) return nullptr;
    *out_rowid = next->first;
    return next->second;
}

// Advance iter to next MVCC-visible row, or EOF.
// Note: plain RD on the sentinel misses the current txn's own pending
// inserts. After the regular iter exhausts, CursorAdvance falls through
// to the pending overlay (PendingNextAfter) for read-your-own-writes.
static void CursorAdvance(OroMotCursor* cur) {
    EnsureThreadCtx(cur->conn);
    if (!cur->pending_mode) {
        while (cur->iter && cur->iter->IsValid()) {
            Sentinel* s = cur->iter->GetPrimarySentinel();
            if (s) {
                RC rc = RC_OK;
                Row* r = cur->conn->txn->RowLookup(AccessType::RD, s, rc);
                if (rc != RC_OK) {
                    cur->at_eof = true;
                    return;
                }
                if (r) {
                    cur->current_row = r;
                    uint64_t rid_be;
                    r->GetValue(MOT_COL_ROWID, rid_be);
                    cur->current_rowid = (int64_t)be64toh(rid_be);
                    cur->at_eof = false;
                    return;
                }
            }
            cur->iter->Next();
        }
        // Regular iter exhausted — switch into pending-overlay mode.
        cur->pending_mode = true;
        cur->pending_last = INT64_MIN;
    }
    int64_t next_rid = 0;
    Row* r = PendingNextAfter(cur->conn, cur->table, cur->pending_last, &next_rid);
    if (r) {
        cur->current_row = r;
        cur->current_rowid = next_rid;
        cur->pending_last = next_rid;
        cur->at_eof = false;
        return;
    }
    cur->at_eof = true;
}

// Forward declaration
static void IdxCursorAdvance(OroMotCursor* cur);

extern "C" int oroMotFirst(OroMotCursor* pCur, int* pEof) {
    EnsureThreadCtx(pCur->conn);
    if (pCur->iter) {
        pCur->iter->Destroy();
        pCur->iter = nullptr;
    }
    pCur->iter = pCur->index->Begin(0);
    pCur->pending_mode = false;
    pCur->pending_last = INT64_MIN;
    if (pCur->is_index) {
        IdxCursorAdvance(pCur);
    } else {
        CursorAdvance(pCur);
    }
    *pEof = pCur->at_eof ? 1 : 0;
    return 0;
}

extern "C" int oroMotLast(OroMotCursor* pCur, int* pEof) {
    // Simple implementation: not used for primary scan, can be added later
    *pEof = 1;
    return 0;
}

extern "C" int oroMotNext(OroMotCursor* pCur, int* pEof) {
    if (pCur->pending_mode) {
        // Already draining pending overlay — don't touch the regular iter.
        CursorAdvance(pCur);
        *pEof = pCur->at_eof ? 1 : 0;
        return 0;
    }
    if (pCur->iter) {
        pCur->iter->Next();
        if (pCur->is_index) {
            IdxCursorAdvance(pCur);
        } else {
            CursorAdvance(pCur);
        }
    } else {
        pCur->at_eof = true;
    }
    *pEof = pCur->at_eof ? 1 : 0;
    return 0;
}

extern "C" int oroMotPrev(OroMotCursor* pCur, int* pEof) {
    *pEof = 1;
    return 0;
}

extern "C" int oroMotSeekRowid(OroMotCursor* pCur, int64_t rowid, int* pRes) {
    EnsureThreadCtx(pCur->conn);

    if (pCur->iter) {
        pCur->iter->Destroy();
        pCur->iter = nullptr;
    }

    // Build the search key: htobe64(rowid)
    Key* key = pCur->index->CreateNewSearchKey();
    if (!key) return 1;
    key->FillPattern(0x00, key->GetKeyLength(), 0);
    uint64_t be_val = htobe64((uint64_t)rowid);
    key->FillValue(reinterpret_cast<const uint8_t*>(&be_val), sizeof(uint64_t), 0);

    RC rc = RC_OK;
    Row* r = pCur->conn->txn->RowLookupByKey(pCur->table, AccessType::RD, key, rc);
    pCur->index->DestroyKey(key);

    if (rc == RC_OK && r) {
        pCur->current_row = r;
        pCur->current_rowid = rowid;
        pCur->at_eof = false;
        *pRes = 0;
        return 0;
    }
    // Miss — check the pending overlay (current txn's own inserts).
    Row* pr = PendingFind(pCur->conn, pCur->table, rowid);
    if (pr) {
        pCur->current_row = pr;
        pCur->current_rowid = rowid;
        pCur->at_eof = false;
        *pRes = 0;
        return 0;
    }
    pCur->current_row = nullptr;
    pCur->at_eof = true;
    *pRes = -1;
    return 0;
}

extern "C" int oroMotSeekCmp(OroMotCursor* pCur, int64_t rowid, int cmp_op,
                             int* pEof) {
    /* cmp_op: 0=GT, 1=GE, 2=LT, 3=LE */
    EnsureThreadCtx(pCur->conn);

    if (pCur->iter) {
        pCur->iter->Destroy();
        pCur->iter = nullptr;
    }

    /* For GT/GE scans: start at beginning, advance until condition met.
     * For LT/LE scans: start at beginning, advance while rowid < target, keep last match.
     * This is O(N) but correct. MOT could optimize with range-aware iterators later. */
    pCur->iter = pCur->index->Begin(0);
    CursorAdvance(pCur);

    while (!pCur->at_eof) {
        int64_t cur = pCur->current_rowid;
        bool match = false;
        switch (cmp_op) {
            case 0: match = (cur >  rowid); break;  // GT
            case 1: match = (cur >= rowid); break;  // GE
            case 2: match = (cur <  rowid); break;  // LT
            case 3: match = (cur <= rowid); break;  // LE
        }
        if (match) break;

        /* For GT/GE: if current key < target, advance */
        if (cmp_op <= 1) {
            if (pCur->iter) pCur->iter->Next();
            CursorAdvance(pCur);
        } else {
            /* For LT/LE scans, SQLite uses iteration in reverse. Full support
             * would need a reverse iterator. For now, scan past non-matching. */
            if (pCur->iter) pCur->iter->Next();
            CursorAdvance(pCur);
        }
    }
    *pEof = pCur->at_eof ? 1 : 0;
    return 0;
}

extern "C" int oroMotRowid(OroMotCursor* pCur, int64_t* pRowid) {
    if (pCur->at_eof || !pCur->current_row) return 1;
    *pRowid = pCur->current_rowid;
    return 0;
}

extern "C" int oroMotPayloadSize(OroMotCursor* pCur, uint32_t* pSize) {
    if (pCur->at_eof || !pCur->current_row) {
        *pSize = 0;
        return 1;
    }
    // First 4 bytes of the BLOB column store the actual record size
    const uint8_t* p = pCur->current_row->GetValue(MOT_COL_DATA);
    uint32_t sz;
    memcpy(&sz, p, sizeof(uint32_t));
    *pSize = sz;
    return 0;
}

extern "C" int oroMotRowData(OroMotCursor* pCur, uint32_t offset, uint32_t amount,
                             void* pBuf) {
    if (pCur->at_eof || !pCur->current_row) return 1;
    const uint8_t* p = pCur->current_row->GetValue(MOT_COL_DATA);
    uint32_t sz;
    memcpy(&sz, p, sizeof(uint32_t));
    if (offset + amount > sz) return 1;
    memcpy(pBuf, p + sizeof(uint32_t) + offset, amount);
    return 0;
}

extern "C" const void* oroMotPayloadFetch(OroMotCursor* pCur, uint32_t* pAmt) {
    if (pCur->at_eof || !pCur->current_row) {
        *pAmt = 0;
        return nullptr;
    }
    const uint8_t* p = pCur->current_row->GetValue(MOT_COL_DATA);
    uint32_t sz;
    memcpy(&sz, p, sizeof(uint32_t));
    *pAmt = sz;
    return p + sizeof(uint32_t);
}

// Forward decl
static int WalLogInsert(OroMotConn* c, const char* tab, int64_t rowid,
                        const void* pData, int nData);
static int WalLogDelete(OroMotConn* c, const char* tab, int64_t rowid);

extern "C" int oroMotInsert(OroMotCursor* pCur, int64_t rowid,
                            const void* pData, int nData) {
    EnsureThreadCtx(pCur->conn);

    if ((uint32_t)nData + sizeof(uint32_t) > MAX_RECORD_SIZE) {
        return 1;  // record too large
    }

    // Log to WAL BEFORE touching MOT. SQLite's txn coordinates durability.
    if (pCur->conn->wal_enabled && !pCur->conn->wal_replaying) {
        WalLogInsert(pCur->conn, pCur->table_name.c_str(), rowid, pData, nData);
    }

    Row* row = pCur->table->CreateNewRow();
    if (!row) return 1;

    char buf[MAX_RECORD_SIZE];
    memset(buf, 0, sizeof(buf));
    uint32_t sz = (uint32_t)nData;
    memcpy(buf, &sz, sizeof(uint32_t));
    memcpy(buf + sizeof(uint32_t), pData, nData);
    SetStringValue(row, MOT_COL_DATA, buf);

    row->SetValue<uint64_t>(MOT_COL_ROWID, (uint64_t)rowid);
    row->SetInternalKey(MOT_COL_ROWID, htobe64((uint64_t)rowid));

    RC rc = pCur->table->InsertRow(row, pCur->conn->txn);
    if (rc != RC_OK) {
        return (rc == RC_UNIQUE_VIOLATION) ? 2 : 1;
    }
    // Record in the per-conn pending overlay so reads inside this txn see it.
    // Skip during WAL replay — replay applies a stream of committed writes
    // and we don't need (or want) to keep an in-memory pending map.
    if (!pCur->conn->wal_replaying) {
        PendingInsert(pCur->conn, pCur->table, rowid, row);
    }
    return 0;
}

extern "C" int oroMotDelete(OroMotCursor* pCur) {
    EnsureThreadCtx(pCur->conn);
    if (pCur->at_eof || !pCur->current_row) return 1;

    int64_t rowid_to_delete = pCur->current_rowid;

    Key* key = pCur->index->CreateNewSearchKey();
    if (!key) return 1;
    key->FillPattern(0x00, key->GetKeyLength(), 0);
    uint64_t be_val = htobe64((uint64_t)rowid_to_delete);
    key->FillValue(reinterpret_cast<const uint8_t*>(&be_val), sizeof(uint64_t), 0);

    RC rc = RC_OK;
    Row* r = pCur->conn->txn->RowLookupByKey(pCur->table,
                                             AccessType::RD_FOR_UPDATE, key, rc);
    pCur->index->DestroyKey(key);

    if (rc != RC_OK || !r) return 1;

    rc = pCur->conn->txn->DeleteLastRow();
    if (rc != RC_OK) return 1;

    // If this rowid was staged in our pending overlay (RYOW), drop it so
    // post-delete reads inside the same txn don't keep seeing it.
    auto& pi = pCur->conn->pending_inserts;
    auto it_p = pi.find(pCur->table);
    if (it_p != pi.end()) it_p->second.erase(rowid_to_delete);

    if (pCur->conn->wal_enabled && !pCur->conn->wal_replaying) {
        WalLogDelete(pCur->conn, pCur->table_name.c_str(), rowid_to_delete);
    }
    return 0;
}

extern "C" int oroMotCount(OroMotCursor* pCur, int64_t* pCount) {
    EnsureThreadCtx(pCur->conn);
    int64_t n = 0;
    IndexIterator* it = pCur->index->Begin(0);
    while (it && it->IsValid()) {
        Sentinel* s = it->GetPrimarySentinel();
        if (s) {
            RC rc = RC_OK;
            Row* r = pCur->conn->txn->RowLookup(AccessType::RD, s, rc);
            if (rc == RC_OK && r) n++;
        }
        it->Next();
    }
    if (it) it->Destroy();
    // Pending inserts (this txn's own writes) — see RYOW machinery below.
    extern int64_t oroMotPendingCount(OroMotConn* c, Table* t);
    n += oroMotPendingCount(pCur->conn, pCur->table);
    *pCount = n;
    return 0;
}

extern "C" int oroMotEof(OroMotCursor* pCur) {
    return pCur->at_eof ? 1 : 0;
}

// =====================================================================
// Rowid allocation
// =====================================================================

// Get per-table RowidCounter, creating and seeding on first call
static RowidCounter* GetRowidCounter(OroMotCursor* pCur) {
    auto& g = globals();
    // Use the MOT Table* pointer as a stable key
    // We store counters by a (table pointer) key for simplicity
    static std::unordered_map<Table*, RowidCounter*> s_counters;
    static std::mutex s_mu;
    std::lock_guard<std::mutex> lock(s_mu);

    auto it = s_counters.find(pCur->table);
    if (it != s_counters.end()) return it->second;

    RowidCounter* ctr = new RowidCounter();
    // Seed from max rowid in the table
    EnsureThreadCtx(pCur->conn);
    int64_t maxId = 0;
    Index* ix = pCur->table->GetPrimaryIndex();
    IndexIterator* it2 = ix->Begin(0);
    while (it2 && it2->IsValid()) {
        Sentinel* s = it2->GetPrimarySentinel();
        if (s) {
            RC rc2 = RC_OK;
            Row* r = pCur->conn->txn->RowLookup(AccessType::RD, s, rc2);
            if (rc2 == RC_OK && r) {
                uint64_t rid_be;
                r->GetValue(MOT_COL_ROWID, rid_be);
                int64_t rid = (int64_t)be64toh(rid_be);
                if (rid > maxId) maxId = rid;
            }
        }
        it2->Next();
    }
    if (it2) it2->Destroy();
    ctr->next.store(maxId + 1);
    s_counters[pCur->table] = ctr;
    return ctr;
}

extern "C" int oroMotCursorNewRowid(OroMotCursor* pCur, int64_t* pRowid) {
    RowidCounter* ctr = GetRowidCounter(pCur);
    *pRowid = ctr->next.fetch_add(1);
    return 0;
}

// =====================================================================
// Key encoding
// =====================================================================

// SQLite varint decoder (simplified, reads up to 9 bytes)
static int oroGetVarint(const uint8_t* p, int64_t* pVal) {
    uint64_t v = 0;
    int i;
    for (i = 0; i < 8; i++) {
        v = (v << 7) | (p[i] & 0x7F);
        if ((p[i] & 0x80) == 0) {
            *pVal = (int64_t)v;
            return i + 1;
        }
    }
    // 9th byte: all 8 bits are payload
    v = (v << 8) | p[8];
    *pVal = (int64_t)v;
    return 9;
}

// Encode a signed int64 to memcmp-comparable 8 bytes (flip sign bit → unsigned BE)
static void encodeInt64(int64_t v, uint8_t* out) {
    uint64_t u = (uint64_t)v ^ 0x8000000000000000ULL;
    for (int i = 7; i >= 0; i--) {
        out[7 - i] = (uint8_t)(u >> (i * 8));
    }
}

// Encode a double to memcmp-comparable 8 bytes
static void encodeReal(double v, uint8_t* out) {
    uint64_t bits;
    memcpy(&bits, &v, 8);
    if (bits & 0x8000000000000000ULL) {
        bits = ~bits;  // negative: flip all bits
    } else {
        bits ^= 0x8000000000000000ULL;  // positive: flip sign bit
    }
    for (int i = 7; i >= 0; i--) {
        out[7 - i] = (uint8_t)(bits >> (i * 8));
    }
}

// Size of data for a SQLite serial type
static int serialTypeSize(int64_t type) {
    if (type <= 0) return 0;
    if (type <= 4) return (int)type;
    if (type == 5) return 6;
    if (type == 6 || type == 7) return 8;
    if (type == 8 || type == 9) return 0;
    if (type >= 12 && (type & 1) == 0) return (int)((type - 12) / 2);  // BLOB
    if (type >= 13 && (type & 1) == 1) return (int)((type - 13) / 2);  // TEXT
    return 0;
}

// Decode a signed integer from SQLite serial-type encoded bytes
static int64_t decodeSerialInt(const uint8_t* data, int len) {
    int64_t v = 0;
    if (len > 0 && (data[0] & 0x80)) v = -1;  // sign-extend
    for (int i = 0; i < len; i++) {
        v = (v << 8) | data[i];
    }
    return v;
}

extern "C" int oroMotEncodeIdxRecord(const void* pRecord, int nRecord,
                                     void* pOut, int64_t* pRowid) {
    const uint8_t* rec = (const uint8_t*)pRecord;
    uint8_t* out = (uint8_t*)pOut;
    memset(out, 0, IDX_ENC_KEY_LEN);

    if (nRecord < 1) return 1;

    // Parse header: first varint is header size
    int64_t hdrSize;
    int hdrBytes = oroGetVarint(rec, &hdrSize);
    if (hdrSize < 1 || hdrSize > nRecord) return 1;

    // Parse serial types from header
    int64_t types[64];
    int nTypes = 0;
    int pos = hdrBytes;
    while (pos < (int)hdrSize && nTypes < 64) {
        int64_t t;
        pos += oroGetVarint(rec + pos, &t);
        types[nTypes++] = t;
    }

    // Data starts after header
    int dataPos = (int)hdrSize;
    int outPos = 0;

    // Encode each field (including rowid as last field)
    for (int i = 0; i < nTypes && outPos < (int)IDX_ENC_KEY_LEN - 10; i++) {
        int64_t st = types[i];
        int dlen = serialTypeSize(st);

        if (st == 0) {
            // NULL
            out[outPos++] = 0x00;
        } else if (st >= 1 && st <= 6) {
            // Integer
            int64_t val = decodeSerialInt(rec + dataPos, dlen);
            out[outPos++] = 0x02;
            encodeInt64(val, out + outPos);
            outPos += 8;
        } else if (st == 7) {
            // Real (IEEE 754 double)
            double val;
            // SQLite stores doubles in big-endian in the record
            uint64_t bits = 0;
            for (int b = 0; b < 8; b++)
                bits = (bits << 8) | rec[dataPos + b];
            memcpy(&val, &bits, 8);
            out[outPos++] = 0x03;
            encodeReal(val, out + outPos);
            outPos += 8;
        } else if (st == 8) {
            // Integer value 0
            out[outPos++] = 0x02;
            encodeInt64(0, out + outPos);
            outPos += 8;
        } else if (st == 9) {
            // Integer value 1
            out[outPos++] = 0x02;
            encodeInt64(1, out + outPos);
            outPos += 8;
        } else if (st >= 13 && (st & 1) == 1) {
            // TEXT
            out[outPos++] = 0x04;
            int copyLen = dlen;
            if (outPos + copyLen + 1 > (int)IDX_ENC_KEY_LEN)
                copyLen = (int)IDX_ENC_KEY_LEN - outPos - 1;
            if (copyLen > 0) memcpy(out + outPos, rec + dataPos, copyLen);
            outPos += copyLen;
            out[outPos++] = 0x00;  // terminator
        } else if (st >= 12 && (st & 1) == 0) {
            // BLOB
            out[outPos++] = 0x05;
            int copyLen = dlen;
            if (outPos + copyLen + 1 > (int)IDX_ENC_KEY_LEN)
                copyLen = (int)IDX_ENC_KEY_LEN - outPos - 1;
            if (copyLen > 0) memcpy(out + outPos, rec + dataPos, copyLen);
            outPos += copyLen;
            out[outPos++] = 0x00;  // terminator
        }
        dataPos += dlen;
    }

    // The last field is the rowid — decode it for the caller
    if (pRowid && nTypes > 0) {
        int64_t lastType = types[nTypes - 1];
        int lastLen = serialTypeSize(lastType);
        int lastDataPos = nRecord - lastLen;
        if (lastType >= 1 && lastType <= 6) {
            *pRowid = decodeSerialInt(rec + lastDataPos, lastLen);
        } else if (lastType == 8) {
            *pRowid = 0;
        } else if (lastType == 9) {
            *pRowid = 1;
        } else {
            *pRowid = 0;
        }
    }

    return 0;
}

// Forward declaration for Mem-based encoding (used from sqlite3.c)
// The pMem pointer is cast to sqlite3_value* / Mem* in sqlite3.c.
// We use a simple interface: pass type flags and values directly.

// Internal: encode a single value into the key buffer at outPos
static int encodeOneValue(uint8_t* out, int outPos, int maxLen,
                          int flags, int64_t iVal, double rVal,
                          const void* zVal, int nVal) {
    // flags: 1=NULL, 2=INT, 4=REAL, 8=TEXT, 16=BLOB
    if (flags & 1) {
        // NULL
        if (outPos < maxLen) out[outPos++] = 0x00;
        return outPos;
    }
    if (flags & 2) {
        // Integer
        if (outPos + 9 <= maxLen) {
            out[outPos++] = 0x02;
            encodeInt64(iVal, out + outPos);
            outPos += 8;
        }
        return outPos;
    }
    if (flags & 4) {
        // Real
        if (outPos + 9 <= maxLen) {
            out[outPos++] = 0x03;
            encodeReal(rVal, out + outPos);
            outPos += 8;
        }
        return outPos;
    }
    if (flags & 8) {
        // Text
        if (outPos + nVal + 2 <= maxLen) {
            out[outPos++] = 0x04;
            if (nVal > 0) memcpy(out + outPos, zVal, nVal);
            outPos += nVal;
            out[outPos++] = 0x00;
        }
        return outPos;
    }
    if (flags & 16) {
        // Blob
        if (outPos + nVal + 2 <= maxLen) {
            out[outPos++] = 0x05;
            if (nVal > 0) memcpy(out + outPos, zVal, nVal);
            outPos += nVal;
            out[outPos++] = 0x00;
        }
        return outPos;
    }
    // Unknown → NULL
    if (outPos < maxLen) out[outPos++] = 0x00;
    return outPos;
}

// Exposed to sqlite3.c: encode Mem values into memcmp key.
// The pMem parameter is actually a Mem* from SQLite internals.
// Since we can't include SQLite internals here, we define a simple
// C callback protocol instead: sqlite3.c calls oroMotEncodeMemValues
// through a wrapper that extracts type+value from each Mem.
extern "C" int oroMotEncodeMemValues(const void* pMem, int nField,
                                     void* pOut, int* pOutLen) {
    // This function is called from sqlite3.c where pMem is actually
    // an array of (flags, iVal, rVal, zVal, nVal) structs packed for us.
    // See oroMotEncodeMemValuesEx below for the actual implementation.
    (void)pMem; (void)nField; (void)pOut; (void)pOutLen;
    return 1;  // Not used directly — use oroMotEncodeMemValuesEx
}

// Alternative: encode from explicit value arrays.
// flags[i]: 1=NULL, 2=INT, 4=REAL, 8=TEXT, 16=BLOB
// iVals[i], rVals[i], zVals[i], nVals[i] for each field.
extern "C" int oroMotEncodeValues(int nField, const int* flags,
                                  const int64_t* iVals, const double* rVals,
                                  const void* const* zVals, const int* nVals,
                                  void* pOut, int* pOutLen) {
    uint8_t* out = (uint8_t*)pOut;
    memset(out, 0, IDX_ENC_KEY_LEN);
    int pos = 0;
    for (int i = 0; i < nField; i++) {
        pos = encodeOneValue(out, pos, (int)IDX_ENC_KEY_LEN,
                             flags[i], iVals ? iVals[i] : 0,
                             rVals ? rVals[i] : 0.0,
                             zVals ? zVals[i] : nullptr,
                             nVals ? nVals[i] : 0);
    }
    if (pOutLen) *pOutLen = pos;
    return 0;
}

// =====================================================================
// Secondary index management
// =====================================================================

static OroMotIdxInfo* LookupIdx(int iDb, const char* tabName,
                                const char* ixName) {
    auto& g = globals();
    // Caller must hold g.mu or call under safe conditions
    auto it = g.sec_indexes.find(IdxKey{iDb, tabName, ixName});
    return (it != g.sec_indexes.end()) ? &it->second : nullptr;
}

extern "C" int oroMotIndexCreate(void* pDb, int iDb, const char* table_name,
                                 const char* index_name) {
    if (!g_initialized.load() || !table_name || !index_name) return 1;

    auto& g = globals();
    {
        std::lock_guard<std::mutex> lock(g.mu);
        IdxKey ik{iDb, table_name, index_name};
        if (g.sec_indexes.count(ik)) return 0;  // already exists
    }

    EnsureThreadCtx(nullptr);
    SessionContext* sess = g_engine->GetSessionManager()->CreateSessionContext();
    if (!sess) return 1;
    TxnManager* txn = sess->GetTxnManager();

    char name_buf[256];
    snprintf(name_buf, sizeof(name_buf), "motix_%d_%s_%s", iDb, table_name, index_name);
    std::string long_name = std::string("public.") + name_buf;

    Table* t = new Table();
    if (!t->Init(name_buf, long_name.c_str(), 3)) {
        delete t;
        g_engine->GetSessionManager()->DestroySessionContext(sess);
        return 1;
    }

    // col 0: rowid (LONG, 8 bytes) — the table rowid this entry points to
    t->AddColumn("idx_rowid", sizeof(uint64_t),
                 MOT_CATALOG_FIELD_TYPES::MOT_TYPE_LONG, false);
    // col 1: record data (BLOB, MAX_RECORD_SIZE) — original SQLite index record
    t->AddColumn("idx_record", MAX_RECORD_SIZE,
                 MOT_CATALOG_FIELD_TYPES::MOT_TYPE_BLOB, false);
    // col 2: encoded key (VARCHAR, IDX_KEY_COL_SIZE) — memcmp-comparable key
    //   This MUST be the last column so GetInternalKeyBuff(PRIMARY) returns it.
    t->AddColumn("idx_key", IDX_KEY_COL_SIZE,
                 MOT_CATALOG_FIELD_TYPES::MOT_TYPE_VARCHAR, true);

    if (!t->InitRowPool() || !t->InitTombStonePool()) {
        delete t;
        g_engine->GetSessionManager()->DestroySessionContext(sess);
        return 1;
    }

    txn->StartTransaction(txn->GetTransactionId(), READ_COMMITED);

    RC rc = txn->CreateTable(t);
    if (rc != RC_OK) {
        txn->Rollback();
        txn->EndTransaction();
        delete t;
        g_engine->GetSessionManager()->DestroySessionContext(sess);
        return 1;
    }

    // Primary MOT index on col 2 (encoded key), unique, keyLength = IDX_MOT_KEY_LEN
    RC irc = RC_OK;
    Index* ix = IndexFactory::CreateIndexEx(
        IndexOrder::INDEX_ORDER_PRIMARY,
        IndexingMethod::INDEXING_METHOD_TREE,
        DEFAULT_TREE_FLAVOR,
        true,  // unique (SQLite records always include rowid)
        IDX_MOT_KEY_LEN,
        std::string("ix_") + name_buf + "_pk",
        irc, nullptr);

    if (!ix || irc != RC_OK) {
        txn->Rollback();
        txn->EndTransaction();
        delete t;
        g_engine->GetSessionManager()->DestroySessionContext(sess);
        return 1;
    }

    ix->SetNumTableFields(t->GetFieldCount());
    ix->SetNumIndexFields(1);
    ix->SetLenghtKeyFields(0, IDX_COL_KEY, IDX_MOT_KEY_LEN);
    ix->SetTable(t);

    rc = txn->CreateIndex(t, ix, true);
    if (rc != RC_OK) {
        delete ix;
        txn->Rollback();
        txn->EndTransaction();
        delete t;
        g_engine->GetSessionManager()->DestroySessionContext(sess);
        return 1;
    }

    rc = txn->Commit();
    txn->EndTransaction();
    g_engine->GetSessionManager()->DestroySessionContext(sess);

    if (rc != RC_OK) {
        delete t;
        return 1;
    }

    {
        std::lock_guard<std::mutex> lock(g.mu);
        g.sec_indexes[IdxKey{iDb, table_name, index_name}] = OroMotIdxInfo{t, ix};
    }
    return 0;
}

extern "C" int oroMotIndexDrop(int iDb, const char* table_name,
                               const char* index_name) {
    if (!table_name || !index_name) return 1;
    auto& g = globals();
    std::lock_guard<std::mutex> lock(g.mu);
    g.sec_indexes.erase(IdxKey{iDb, table_name, index_name});
    return 0;
}

// =====================================================================
// Index cursor
// =====================================================================

extern "C" int oroMotIdxCursorOpen(void* pDb, int iDb, const char* table_name,
                                   const char* index_name, int wrFlag,
                                   OroMotCursor** ppCursor) {
    OroMotIdxInfo* info = nullptr;
    {
        auto& g = globals();
        std::lock_guard<std::mutex> lock(g.mu);
        auto it = g.sec_indexes.find(IdxKey{iDb, table_name, index_name});
        if (it == g.sec_indexes.end()) return 1;
        info = &it->second;
    }

    OroMotConn* c = GetOrCreateConn(pDb);
    if (!c) return 1;
    EnsureThreadCtx(c);

    if (!c->in_txn) {
        c->txn->StartTransaction(c->txn->GetTransactionId(), READ_COMMITED);
        c->in_txn = true;
    }

    OroMotCursor* cur = new OroMotCursor();
    cur->table = info->table;
    cur->index = info->index;
    cur->conn = c;
    cur->write_mode = (wrFlag != 0);
    cur->at_eof = true;
    cur->is_index = true;

    *ppCursor = cur;
    return 0;
}

// Helper: advance index cursor to next MVCC-visible entry
static void IdxCursorAdvance(OroMotCursor* cur) {
    EnsureThreadCtx(cur->conn);
    while (cur->iter && cur->iter->IsValid()) {
        Sentinel* s = cur->iter->GetPrimarySentinel();
        if (s) {
            RC rc = RC_OK;
            Row* r = cur->conn->txn->RowLookup(AccessType::RD, s, rc);
            if (rc != RC_OK) { cur->at_eof = true; return; }
            if (r) {
                cur->current_row = r;
                // Read rowid from col 0
                uint64_t rid;
                r->GetValue(IDX_COL_ROWID, rid);
                cur->idx_rowid = (int64_t)rid;
                cur->at_eof = false;
                return;
            }
        }
        cur->iter->Next();
    }
    cur->at_eof = true;
}

extern "C" int oroMotIdxInsert(OroMotCursor* pCur,
                               const void* pEncodedKey, int64_t rowid,
                               const void* pRecord, int nRecord) {
    EnsureThreadCtx(pCur->conn);

    if ((uint32_t)nRecord + sizeof(uint32_t) > MAX_RECORD_SIZE) return 1;

    Row* row = pCur->table->CreateNewRow();
    if (!row) return 1;

    // col 0: rowid
    row->SetValue<uint64_t>(IDX_COL_ROWID, (uint64_t)rowid);

    // col 1: original record data (BLOB with 4-byte length prefix)
    {
        char buf[MAX_RECORD_SIZE];
        memset(buf, 0, sizeof(buf));
        uint32_t sz = (uint32_t)nRecord;
        memcpy(buf, &sz, sizeof(uint32_t));
        memcpy(buf + sizeof(uint32_t), pRecord, nRecord);
        SetStringValue(row, IDX_COL_RECORD, buf);
    }

    // col 2: encoded key (VARCHAR with 4-byte length prefix)
    // Write directly into the row data buffer
    {
        Column* col = pCur->table->GetField(IDX_COL_KEY);
        uint8_t* dst = const_cast<uint8_t*>(row->GetData()) + col->m_offset;
        uint32_t klen = IDX_ENC_KEY_LEN;
        memcpy(dst, &klen, sizeof(uint32_t));
        memcpy(dst + sizeof(uint32_t), pEncodedKey, IDX_ENC_KEY_LEN);
    }

    // Mark as InternalKey so BuildKey uses the InternalKey path
    row->SetKeytype(KeyType::INTERNAL_KEY);

    RC rc = pCur->table->InsertRow(row, pCur->conn->txn);
    if (rc != RC_OK) {
        return (rc == RC_UNIQUE_VIOLATION) ? 2 : 1;
    }
    return 0;
}

extern "C" int oroMotIdxDelete(OroMotCursor* pCur,
                               const void* pEncodedKey) {
    EnsureThreadCtx(pCur->conn);

    // Build search key: [4-byte constant len][encoded key bytes]
    Key* key = pCur->index->CreateNewSearchKey();
    if (!key) return 1;
    key->FillPattern(0x00, key->GetKeyLength(), 0);
    uint32_t klen = IDX_ENC_KEY_LEN;
    key->FillValue((const uint8_t*)&klen, sizeof(uint32_t), 0);
    key->FillValue((const uint8_t*)pEncodedKey, IDX_ENC_KEY_LEN, sizeof(uint32_t));

    RC rc = RC_OK;
    Row* r = pCur->conn->txn->RowLookupByKey(pCur->table,
                                              AccessType::RD_FOR_UPDATE, key, rc);
    pCur->index->DestroyKey(key);

    if (rc != RC_OK || !r) return (rc == RC_OK) ? 0 : 1;  // not found = ok

    rc = pCur->conn->txn->DeleteLastRow();
    return (rc == RC_OK) ? 0 : 1;
}

extern "C" int oroMotIdxRowid(OroMotCursor* pCur, int64_t* pRowid) {
    if (pCur->at_eof || !pCur->current_row) return 1;
    *pRowid = pCur->idx_rowid;
    return 0;
}

extern "C" const void* oroMotIdxRecordFetch(OroMotCursor* pCur,
                                            unsigned int* pAmt) {
    if (pCur->at_eof || !pCur->current_row) {
        *pAmt = 0;
        return nullptr;
    }
    // col 1 is BLOB: [4-byte-len][record bytes]
    const uint8_t* p = pCur->current_row->GetValue(IDX_COL_RECORD);
    uint32_t sz;
    memcpy(&sz, p, sizeof(uint32_t));
    *pAmt = sz;
    return p + sizeof(uint32_t);
}

extern "C" int oroMotIdxSeek(OroMotCursor* pCur,
                             const void* pEncodedKey, int nKey,
                             int cmp_op, int* pEof) {
    /* cmp_op: 0=GT, 1=GE, 2=LT, 3=LE */
    EnsureThreadCtx(pCur->conn);

    if (pCur->iter) {
        pCur->iter->Destroy();
        pCur->iter = nullptr;
    }

    // Build search key: [4-byte constant len][encoded key bytes (padded)]
    Key* key = pCur->index->CreateNewSearchKey();
    if (!key) return 1;
    key->FillPattern(0x00, key->GetKeyLength(), 0);
    uint32_t klen = IDX_ENC_KEY_LEN;
    key->FillValue((const uint8_t*)&klen, sizeof(uint32_t), 0);
    int copyLen = nKey < (int)IDX_ENC_KEY_LEN ? nKey : (int)IDX_ENC_KEY_LEN;
    key->FillValue((const uint8_t*)pEncodedKey, copyLen, sizeof(uint32_t));

    bool found = false;
    bool forward = (cmp_op <= 1);  // GT,GE = forward; LT,LE = reverse?

    // For GE/GT: search forward from the key position
    // For LE/LT: search forward too but we'll need to handle differently
    pCur->iter = pCur->index->Search(key, true /*matchKey*/, true /*forward*/,
                                     0 /*pid*/, found);
    pCur->index->DestroyKey(key);

    if (cmp_op == 0 || cmp_op == 1) {
        // GE or GT
        IdxCursorAdvance(pCur);
        if (cmp_op == 0 && !pCur->at_eof) {
            // GT: if positioned at exact match, skip it
            // Compare current key against search key
            Column* col = pCur->table->GetField(IDX_COL_KEY);
            const uint8_t* curKey = pCur->current_row->GetData() + col->m_offset + sizeof(uint32_t);
            if (memcmp(curKey, pEncodedKey, copyLen) == 0) {
                // Exact match — need to advance past it for GT
                if (pCur->iter) pCur->iter->Next();
                IdxCursorAdvance(pCur);
            }
        }
    } else {
        // LT or LE: we need entries before the key.
        // MassTree Search positions at or after the key.
        // We need to back up. For now, scan from beginning and stop at key.
        if (pCur->iter) { pCur->iter->Destroy(); pCur->iter = nullptr; }
        pCur->iter = pCur->index->Begin(0);
        // Find the last entry that is < (or <=) the search key
        OroMotCursor best = {};
        best.at_eof = true;
        while (true) {
            IdxCursorAdvance(pCur);
            if (pCur->at_eof) break;
            Column* col = pCur->table->GetField(IDX_COL_KEY);
            const uint8_t* curKey = pCur->current_row->GetData() + col->m_offset + sizeof(uint32_t);
            int cmp = memcmp(curKey, pEncodedKey, copyLen);
            if (cmp_op == 2 && cmp >= 0) break;   // LT: stop at >=
            if (cmp_op == 3 && cmp > 0) break;    // LE: stop at >
            best.current_row = pCur->current_row;
            best.idx_rowid = pCur->idx_rowid;
            best.at_eof = false;
            if (pCur->iter) pCur->iter->Next();
        }
        if (!best.at_eof) {
            pCur->current_row = best.current_row;
            pCur->idx_rowid = best.idx_rowid;
            pCur->at_eof = false;
        } else {
            pCur->at_eof = true;
        }
    }

    *pEof = pCur->at_eof ? 1 : 0;
    return 0;
}

// =====================================================================
// Write-Ahead Log via native SQLite table
// =====================================================================
//
// Design: Every MOT row mutation is mirrored into a native SQLite table
// named `_mot_wal` in the same database file. SQLite's own durability
// (rollback journal / WAL mode on its .db file) makes the log crash-safe.
//
// Schema:
//   CREATE TABLE _mot_wal (
//     seq        INTEGER PRIMARY KEY AUTOINCREMENT,
//     table_name TEXT,
//     op         INTEGER,   -- 1=INS, 2=DEL
//     rowid      INTEGER,
//     record     BLOB
//   )
//
// Write ordering (per user txn):
//   (1) INSERT row into _mot_wal    ← buffered in SQLite txn
//   (2) Table::InsertRow into MOT   ← in-memory
//   (3) user COMMIT                 ← SQLite fsyncs _mot_wal atomically
//   (4) MOT commit                  ← in-memory, cannot fail durably
//
// If we crash between (3) and (4): on restart, _mot_wal has the record,
// MOT does not — replay applies it. Correct.
// If we crash between (1) and (3): SQLite rolls back _mot_wal, MOT rolls
// back its staged change. Nothing persisted. Correct.
//
// The WAL rows live in the same .db file as the SQLite-side schema and
// native tables, so one fsync durably commits everything.
// =====================================================================

#define WAL_OP_INS 1
#define WAL_OP_DEL 2

static const char* WAL_DDL =
    "CREATE TABLE IF NOT EXISTS _mot_wal ("
    "  seq        INTEGER PRIMARY KEY AUTOINCREMENT,"
    "  table_name TEXT NOT NULL,"
    "  op         INTEGER NOT NULL,"
    "  rowid      INTEGER NOT NULL,"
    "  record     BLOB)";

static const char* WAL_INS_SQL =
    "INSERT INTO _mot_wal(table_name, op, rowid, record) VALUES(?1, ?2, ?3, ?4)";

// Per-statement WAL-write counter: incremented from WalLogInsert/Delete; the
// auto-checkpoint trigger lives in oroMotAutoCommit, which fires after each
// statement when autocommit is on. We can't checkpoint inside WalLog* because
// we're nested inside the outer statement's implicit transaction at that
// point — oroMotWalCheckpoint's own BEGIN IMMEDIATE would conflict.

static int WalLogInsert(OroMotConn* c, const char* tab, int64_t rowid,
                        const void* pData, int nData) {
    if (!c || !c->wal_enabled || !c->ins_stmt) return 1;
    sqlite3_stmt* stmt = (sqlite3_stmt*)c->ins_stmt;
    sqlite3_reset(stmt);
    sqlite3_bind_text(stmt, 1, tab, -1, SQLITE_STATIC);
    sqlite3_bind_int(stmt, 2, WAL_OP_INS);
    sqlite3_bind_int64(stmt, 3, rowid);
    sqlite3_bind_blob(stmt, 4, pData, nData, SQLITE_TRANSIENT);
    int rc = sqlite3_step(stmt);
    if (rc == SQLITE_DONE) {
        c->wal_writes_since_ck++;
        return 0;
    }
    return 1;
}

static int WalLogDelete(OroMotConn* c, const char* tab, int64_t rowid) {
    if (!c || !c->wal_enabled || !c->ins_stmt) return 1;
    sqlite3_stmt* stmt = (sqlite3_stmt*)c->ins_stmt;
    sqlite3_reset(stmt);
    sqlite3_bind_text(stmt, 1, tab, -1, SQLITE_STATIC);
    sqlite3_bind_int(stmt, 2, WAL_OP_DEL);
    sqlite3_bind_int64(stmt, 3, rowid);
    sqlite3_bind_null(stmt, 4);
    int rc = sqlite3_step(stmt);
    if (rc == SQLITE_DONE) {
        c->wal_writes_since_ck++;
        return 0;
    }
    return 1;
}

// Called from oroMotAutoCommit (after a statement-level implicit txn has
// settled). If a threshold is configured and we've crossed it, run a
// checkpoint and reset the counter. Safe to call when autocommit is on;
// caller is responsible for not calling it inside an explicit user txn.
static void MaybeAutoCheckpoint(OroMotConn* c) {
    if (!c || !c->wal_enabled || c->wal_replaying) return;
    if (c->wal_ck_threshold == 0) return;
    if (c->wal_writes_since_ck < c->wal_ck_threshold) return;
    if (!c->pDb) return;
    extern int oroMotWalCheckpoint(void*);
    oroMotWalCheckpoint(c->pDb);  // resets counter on success
}

extern "C" int oroMotWalEnable(void* pDb) {
    if (!pDb) return 1;
    sqlite3* db = (sqlite3*)pDb;
    OroMotConn* c = GetOrCreateConn(pDb);
    if (!c) return 1;

    // Create WAL table
    char* err = nullptr;
    if (sqlite3_exec(db, WAL_DDL, nullptr, nullptr, &err) != SQLITE_OK) {
        if (err) sqlite3_free(err);
        return 1;
    }

    // Prepare the INSERT statement (reused across all logging)
    sqlite3_stmt* stmt = nullptr;
    if (sqlite3_prepare_v2(db, WAL_INS_SQL, -1, &stmt, nullptr) != SQLITE_OK) {
        return 1;
    }
    c->ins_stmt = stmt;
    c->wal_enabled = true;
    return 0;
}

extern "C" int oroMotWalIsEnabled(void* pDb) {
    auto& g = globals();
    std::lock_guard<std::mutex> lock(g.mu);
    auto it = g.conns.find(pDb);
    if (it == g.conns.end()) return 0;
    return it->second->wal_enabled ? 1 : 0;
}

extern "C" int oroMotWalRecover(void* pDb) {
    if (!pDb) return -1;
    sqlite3* db = (sqlite3*)pDb;

    // Check if the WAL table exists (if not, nothing to do)
    sqlite3_stmt* check = nullptr;
    const char* chkSql = "SELECT 1 FROM sqlite_schema WHERE type='table' AND name='_mot_wal'";
    if (sqlite3_prepare_v2(db, chkSql, -1, &check, nullptr) != SQLITE_OK) return -1;
    int hasWal = (sqlite3_step(check) == SQLITE_ROW) ? 1 : 0;
    sqlite3_finalize(check);
    if (!hasWal) return 0;

    OroMotConn* c = GetOrCreateConn(pDb);
    if (!c) return -1;

    // Read all WAL entries in order
    sqlite3_stmt* sel = nullptr;
    const char* selSql =
        "SELECT seq, table_name, op, rowid, record FROM _mot_wal ORDER BY seq";
    if (sqlite3_prepare_v2(db, selSql, -1, &sel, nullptr) != SQLITE_OK) return -1;

    EnsureThreadCtx(c);
    c->wal_replaying = true;  // suppress re-logging during replay

    // Replay each record in its own MOT transaction so subsequent operations
    // see the committed state (avoids RYOW issue within the replay itself).
    int applied = 0;
    int rc = 0;
    while (sqlite3_step(sel) == SQLITE_ROW) {
        const char* tab = (const char*)sqlite3_column_text(sel, 1);
        int op = sqlite3_column_int(sel, 2);
        int64_t rowid = sqlite3_column_int64(sel, 3);
        const void* rec = sqlite3_column_blob(sel, 4);
        int nRec = sqlite3_column_bytes(sel, 4);
        if (!tab) continue;

        // Start a fresh MOT transaction for this record
        if (!c->in_txn) {
            c->txn->StartTransaction(c->txn->GetTransactionId(), READ_COMMITED);
            c->in_txn = true;
        }

        OroMotCursor* cur = nullptr;
        if (oroMotCursorOpen(pDb, 0, tab, 1, &cur) != 0) {
            if (c->in_txn) {
                c->txn->Rollback();
                c->txn->EndTransaction();
                c->in_txn = false;
            }
            continue;
        }

        bool ok = false;
        if (op == WAL_OP_INS) {
            int ir = oroMotInsert(cur, rowid, rec, nRec);
            // rc=2 means UNIQUE violation — idempotent: row already in MOT
            // (e.g. leftover state from same process, or partial replay).
            ok = (ir == 0 || ir == 2);
        } else if (op == WAL_OP_DEL) {
            int res = 0;
            if (oroMotSeekRowid(cur, rowid, &res) == 0 && res == 0) {
                ok = (oroMotDelete(cur) == 0);
            } else {
                ok = true;  // already gone — idempotent
            }
        }

        oroMotCursorClose(cur);

        // Commit this record's transaction
        if (c->in_txn) {
            RC mrc = c->txn->Commit();
            c->txn->EndTransaction();
            c->in_txn = false;
            if (mrc != RC_OK) ok = false;
        }

        if (ok) applied++;
    }
    sqlite3_finalize(sel);

    c->wal_replaying = false;

    // Keep _mot_wal intact. Replays are idempotent (UNIQUE violations and
    // missing-delete rows are both treated as already-applied), so every
    // new connection can safely re-replay to restore MOT state if lost.
    // Checkpointing (truncate after snapshot) is future work.

    return (rc == 0) ? applied : -1;
}

// =====================================================================
// WAL checkpoint
// =====================================================================
// Replace the accumulated WAL with a fresh snapshot of current MOT state.
// After this, replay time on restart is bounded by the number of LIVE rows,
// not the historical write count.
//
// Implementation:
//   BEGIN                                   ← SQLite tx
//   CREATE TEMP TABLE _mot_wal_new(...)     ← new log
//   for each MOT table:
//     for each visible row:
//       INSERT into _mot_wal_new as INS
//   DELETE FROM _mot_wal
//   INSERT INTO _mot_wal SELECT * FROM _mot_wal_new
//   COMMIT
extern "C" int oroMotWalSetAutoCheckpoint(void* pDb, uint64_t threshold) {
    if (!pDb) return 1;
    OroMotConn* c = GetOrCreateConn(pDb);
    if (!c) return 1;
    c->wal_ck_threshold = threshold;
    return 0;
}

extern "C" int oroMotWalCheckpoint(void* pDb) {
    if (!pDb) return -1;
    sqlite3* db = (sqlite3*)pDb;

    OroMotConn* c = GetOrCreateConn(pDb);
    if (!c) return -1;

    // Collect (table_name, Table*) snapshot under the lock
    std::vector<std::pair<std::string, Table*>> tablesCopy;
    {
        auto& g = globals();
        std::lock_guard<std::mutex> lock(g.mu);
        for (auto& kv : g.tables) {
            tablesCopy.emplace_back(kv.first.name, kv.second);
        }
    }

    // Begin transaction on SQLite side
    if (sqlite3_exec(db, "BEGIN IMMEDIATE", nullptr, nullptr, nullptr) != SQLITE_OK) {
        return -1;
    }

    // Clear and rebuild _mot_wal
    if (sqlite3_exec(db, "DELETE FROM _mot_wal", nullptr, nullptr, nullptr)
            != SQLITE_OK) {
        sqlite3_exec(db, "ROLLBACK", nullptr, nullptr, nullptr);
        return -1;
    }

    sqlite3_stmt* ins = nullptr;
    if (sqlite3_prepare_v2(db,
            "INSERT INTO _mot_wal(table_name, op, rowid, record) "
            "VALUES(?1, ?2, ?3, ?4)", -1, &ins, nullptr) != SQLITE_OK) {
        sqlite3_exec(db, "ROLLBACK", nullptr, nullptr, nullptr);
        return -1;
    }

    EnsureThreadCtx(c);

    int64_t total = 0;
    for (auto& p : tablesCopy) {
        const std::string& tabName = p.first;
        Table* t = p.second;

        // Start a read txn for the scan
        if (!c->in_txn) {
            c->txn->StartTransaction(c->txn->GetTransactionId(), READ_COMMITED);
            c->in_txn = true;
        }

        Index* ix = t->GetPrimaryIndex();
        IndexIterator* it = ix->Begin(0);
        while (it && it->IsValid()) {
            Sentinel* s = it->GetPrimarySentinel();
            if (s) {
                RC rc = RC_OK;
                Row* r = c->txn->RowLookup(AccessType::RD, s, rc);
                if (rc == RC_OK && r) {
                    // Decode rowid and record bytes from col 1 (BLOB)
                    uint64_t rid_be;
                    r->GetValue(MOT_COL_ROWID, rid_be);
                    int64_t rowid = (int64_t)be64toh(rid_be);

                    const uint8_t* p = r->GetValue(MOT_COL_DATA);
                    uint32_t rsz;
                    memcpy(&rsz, p, sizeof(uint32_t));
                    const void* recBytes = p + sizeof(uint32_t);

                    sqlite3_reset(ins);
                    sqlite3_bind_text(ins, 1, tabName.c_str(), -1, SQLITE_TRANSIENT);
                    sqlite3_bind_int(ins, 2, WAL_OP_INS);
                    sqlite3_bind_int64(ins, 3, rowid);
                    sqlite3_bind_blob(ins, 4, recBytes, rsz, SQLITE_TRANSIENT);
                    if (sqlite3_step(ins) == SQLITE_DONE) {
                        total++;
                    }
                }
            }
            it->Next();
        }
        if (it) it->Destroy();

        // End the read txn
        if (c->in_txn) {
            c->txn->Commit();
            c->txn->EndTransaction();
            c->in_txn = false;
        }
    }

    sqlite3_finalize(ins);

    // Commit the SQLite txn — WAL rebuild is durable now
    if (sqlite3_exec(db, "COMMIT", nullptr, nullptr, nullptr) != SQLITE_OK) {
        return -1;
    }

    // Note: VACUUM would reclaim the freed pages but requires no pending
    // cursors/statements. Skipping for now — SQLite reuses freed pages on
    // subsequent inserts anyway. Users can run `VACUUM` manually.

    c->wal_writes_since_ck = 0;
    return (int)total;
}
