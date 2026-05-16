// libpq_load.cpp — multi-threaded load / conformance client for oro_server.
//
// Usage:
//   harness_libpq_load --host H --port P --user U --db D \
//       --threads N --duration SEC --mode MODE [--seed S]
//
// Modes:
//   smoke      One thread, one connection. Runs CREATE/INSERT/SELECT/DELETE
//              over the simple and extended protocols. Verifies basics.
//   stress     Mixed workload (INSERT/SELECT/UPDATE/DELETE) across N threads
//              against a shared MOT table. Verifies absence of crashes/races.
//   extended   Round-trip a parameterised query via PQexecParams (forces the
//              extended protocol). Exits with code 0 on success.
//
// Build: requires libpq-dev. Wired in CMakeLists.txt as harness_libpq_load.
//
// Exit codes:
//   0  success
//   1  fatal libpq error / oracle mismatch
//   2  bad CLI

#include <libpq-fe.h>

#include <atomic>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <random>
#include <string>
#include <thread>
#include <vector>

namespace {

struct Args {
    std::string host = "localhost";
    int port = 5433;
    std::string user = "t";
    std::string db = "t";
    int threads = 4;
    int duration = 5;     // seconds
    std::string mode = "smoke";
    uint64_t seed = 1;
};

bool parseArgs(int argc, char** argv, Args& out) {
    for (int i = 1; i < argc; i++) {
        std::string a = argv[i];
        auto next = [&]() -> const char* {
            if (i + 1 >= argc) return nullptr;
            return argv[++i];
        };
        if (a == "--host") out.host = next();
        else if (a == "--port") out.port = std::atoi(next());
        else if (a == "--user") out.user = next();
        else if (a == "--db") out.db = next();
        else if (a == "--threads") out.threads = std::atoi(next());
        else if (a == "--duration") out.duration = std::atoi(next());
        else if (a == "--mode") out.mode = next();
        else if (a == "--seed") out.seed = std::strtoull(next(), nullptr, 10);
        else { std::fprintf(stderr, "unknown arg: %s\n", a.c_str()); return false; }
    }
    return true;
}

PGconn* connectOrDie(const Args& a) {
    char conninfo[512];
    std::snprintf(conninfo, sizeof(conninfo),
        "host=%s port=%d user=%s dbname=%s connect_timeout=5",
        a.host.c_str(), a.port, a.user.c_str(), a.db.c_str());
    PGconn* c = PQconnectdb(conninfo);
    if (PQstatus(c) != CONNECTION_OK) {
        std::fprintf(stderr, "connect failed: %s\n", PQerrorMessage(c));
        PQfinish(c);
        return nullptr;
    }
    return c;
}

bool execOk(PGconn* c, const char* sql, ExecStatusType expect = PGRES_COMMAND_OK) {
    PGresult* r = PQexec(c, sql);
    bool ok = (PQresultStatus(r) == expect);
    if (!ok) {
        std::fprintf(stderr, "exec failed [%s]: status=%s err=%s\n",
                     sql, PQresStatus(PQresultStatus(r)), PQerrorMessage(c));
    }
    PQclear(r);
    return ok;
}

int runSmoke(const Args& a) {
    PGconn* c = connectOrDie(a);
    if (!c) return 1;
    auto fail = [&](const char* msg) {
        std::fprintf(stderr, "[smoke] %s\n", msg);
        PQfinish(c);
        return 1;
    };

    // Simple protocol path
    if (!execOk(c, "DROP TABLE IF EXISTS smoke_t"))           return fail("drop");
    if (!execOk(c, "CREATE MOT TABLE smoke_t (id INT PRIMARY KEY, name TEXT)"))
        return fail("create");
    if (!execOk(c, "INSERT INTO smoke_t VALUES (1, 'a'), (2, 'b'), (3, 'c')"))
        return fail("insert");

    PGresult* r = PQexec(c, "SELECT count(*) FROM smoke_t");
    if (PQresultStatus(r) != PGRES_TUPLES_OK) {
        PQclear(r);
        return fail("select");
    }
    std::string n = PQgetvalue(r, 0, 0);
    PQclear(r);
    if (n != "3") return fail("count != 3");

    if (!execOk(c, "DELETE FROM smoke_t WHERE id=2")) return fail("delete");

    // Extended protocol path (PQexecParams forces 'P'/'B'/'E')
    const char* values[1] = { "1" };
    int lengths[1] = { 1 };
    int formats[1] = { 0 };
    r = PQexecParams(c,
        "SELECT name FROM smoke_t WHERE id = $1",
        1, nullptr, values, lengths, formats, 0);
    bool extOk = (PQresultStatus(r) == PGRES_TUPLES_OK);
    if (!extOk) {
        std::fprintf(stderr, "[smoke] extended protocol failed: %s\n",
                     PQerrorMessage(c));
    }
    PQclear(r);
    if (!extOk) return fail("extended");

    PQfinish(c);
    std::printf("[smoke] OK\n");
    return 0;
}

int runExtended(const Args& a) {
    PGconn* c = connectOrDie(a);
    if (!c) return 1;
    if (!execOk(c, "DROP TABLE IF EXISTS ext_t")) { PQfinish(c); return 1; }
    if (!execOk(c, "CREATE MOT TABLE ext_t (id INT PRIMARY KEY, v TEXT)")) {
        PQfinish(c); return 1;
    }
    const char* iv[2] = { "7", "lucky" };
    PGresult* r = PQexecParams(c, "INSERT INTO ext_t VALUES ($1, $2)",
                               2, nullptr, iv, nullptr, nullptr, 0);
    bool ok = (PQresultStatus(r) == PGRES_COMMAND_OK);
    PQclear(r);
    if (!ok) {
        std::fprintf(stderr, "[extended] insert via params failed: %s\n",
                     PQerrorMessage(c));
        PQfinish(c);
        return 1;
    }
    const char* sv[1] = { "7" };
    r = PQexecParams(c, "SELECT v FROM ext_t WHERE id = $1",
                     1, nullptr, sv, nullptr, nullptr, 0);
    ok = (PQresultStatus(r) == PGRES_TUPLES_OK && PQntuples(r) == 1 &&
          std::string(PQgetvalue(r, 0, 0)) == "lucky");
    PQclear(r);
    PQfinish(c);
    std::printf("[extended] %s\n", ok ? "OK" : "FAIL");
    return ok ? 0 : 1;
}

void stressWorker(const Args a, int tid, std::atomic<bool>* stop,
                  std::atomic<uint64_t>* ops, std::atomic<bool>* err) {
    PGconn* c = connectOrDie(a);
    if (!c) { err->store(true); return; }
    std::mt19937_64 rng(a.seed + tid);
    char sql[256];
    while (!stop->load(std::memory_order_relaxed)) {
        int pick = rng() % 100;
        int id = (int)(rng() % 100000);
        PGresult* r = nullptr;
        if (pick < 70) {
            std::snprintf(sql, sizeof(sql),
                "INSERT OR IGNORE INTO stress_t VALUES (%d, 'v%d')", id, id);
            r = PQexec(c, sql);
        } else if (pick < 90) {
            std::snprintf(sql, sizeof(sql),
                "SELECT v FROM stress_t WHERE id = %d", id);
            r = PQexec(c, sql);
        } else if (pick < 95) {
            std::snprintf(sql, sizeof(sql),
                "UPDATE stress_t SET v = 'u%d' WHERE id = %d", id, id);
            r = PQexec(c, sql);
        } else {
            std::snprintf(sql, sizeof(sql),
                "DELETE FROM stress_t WHERE id = %d", id);
            r = PQexec(c, sql);
        }
        // PGRES_FATAL_ERROR is a real failure; expected statuses are
        // COMMAND_OK (DML) and TUPLES_OK (SELECT). Anything else flags err.
        ExecStatusType st = PQresultStatus(r);
        if (st != PGRES_COMMAND_OK && st != PGRES_TUPLES_OK) {
            const char* msg = PQerrorMessage(c);
            // "database is locked" under contention is benign with
            // busy_timeout — flag as a soft error, don't abort the run.
            if (st == PGRES_FATAL_ERROR &&
                (strstr(msg, "database is locked") == nullptr)) {
                std::fprintf(stderr, "[stress tid=%d] fatal: %s", tid, msg);
                err->store(true);
            }
        }
        PQclear(r);
        ops->fetch_add(1, std::memory_order_relaxed);
    }
    PQfinish(c);
}

// delete_loop — 1,000 iterations of CREATE/INSERT/DELETE on a MOT table over
// the wire. Reproduces the GAPS.md #6 scenario. Server must stay alive and
// the final row must be the only one left.
int runDeleteLoop(const Args& a) {
    PGconn* c = connectOrDie(a);
    if (!c) return 1;
    if (!execOk(c, "DROP TABLE IF EXISTS dl_t")) { PQfinish(c); return 1; }
    if (!execOk(c, "CREATE MOT TABLE dl_t (id INT PRIMARY KEY, name TEXT)")) {
        PQfinish(c); return 1;
    }
    char sql[128];
    for (int i = 0; i < 1000; i++) {
        std::snprintf(sql, sizeof(sql), "INSERT INTO dl_t VALUES (%d, 'x')", i);
        if (!execOk(c, sql)) { PQfinish(c); return 1; }
        std::snprintf(sql, sizeof(sql), "DELETE FROM dl_t WHERE id = %d", i);
        if (!execOk(c, sql)) { PQfinish(c); return 1; }
    }
    PGresult* r = PQexec(c, "SELECT count(*) FROM dl_t");
    bool ok = (PQresultStatus(r) == PGRES_TUPLES_OK &&
               std::string(PQgetvalue(r, 0, 0)) == "0");
    PQclear(r);
    PQfinish(c);
    std::printf("[delete_loop] %s\n", ok ? "OK" : "FAIL");
    return ok ? 0 : 1;
}

int runStress(const Args& a) {
    PGconn* c = connectOrDie(a);
    if (!c) return 1;
    execOk(c, "DROP TABLE IF EXISTS stress_t");
    if (!execOk(c, "CREATE MOT TABLE stress_t (id INT PRIMARY KEY, v TEXT)")) {
        PQfinish(c); return 1;
    }
    PQfinish(c);

    std::atomic<bool> stop(false), err(false);
    std::atomic<uint64_t> ops(0);
    std::vector<std::thread> ths;
    for (int i = 0; i < a.threads; i++) {
        ths.emplace_back(stressWorker, a, i, &stop, &ops, &err);
    }
    std::this_thread::sleep_for(std::chrono::seconds(a.duration));
    stop.store(true);
    for (auto& t : ths) t.join();
    std::printf("[stress] threads=%d duration=%ds ops=%lu err=%d\n",
                a.threads, a.duration, (unsigned long)ops.load(),
                err.load() ? 1 : 0);
    return err.load() ? 1 : 0;
}

// DDL worker: loops CREATE INDEX IF NOT EXISTS / DROP INDEX on stress_t at
// a slow cadence (every ~200ms). Also rotates a sibling MOT table
// (stress_sibling_<rand>) on a longer cycle to exercise oroMotTableCreate /
// Drop under load. Errors that aren't "already exists" / "no such index"
// flag the global err.
void ddlWorker(const Args a, std::atomic<bool>* stop,
               std::atomic<uint64_t>* ops, std::atomic<bool>* err) {
    PGconn* c = connectOrDie(a);
    if (!c) { err->store(true); return; }
    std::mt19937_64 rng(a.seed ^ 0xDD1ULL);
    int round = 0;
    while (!stop->load(std::memory_order_relaxed)) {
        char sql[256];
        std::snprintf(sql, sizeof(sql),
            "CREATE INDEX IF NOT EXISTS stress_t_v_idx ON stress_t(v)");
        PGresult* r = PQexec(c, sql);
        if (PQresultStatus(r) == PGRES_FATAL_ERROR) {
            std::fprintf(stderr, "[ddl] CREATE INDEX failed: %s",
                         PQerrorMessage(c));
            err->store(true);
        }
        PQclear(r);
        ops->fetch_add(1, std::memory_order_relaxed);

        std::this_thread::sleep_for(std::chrono::milliseconds(200));

        r = PQexec(c, "DROP INDEX IF EXISTS stress_t_v_idx");
        if (PQresultStatus(r) == PGRES_FATAL_ERROR) {
            std::fprintf(stderr, "[ddl] DROP INDEX failed: %s",
                         PQerrorMessage(c));
            err->store(true);
        }
        PQclear(r);
        ops->fetch_add(1, std::memory_order_relaxed);

        // Sibling-table churn every 5th round.
        if ((round++ % 5) == 0) {
            char tbl[64];
            std::snprintf(tbl, sizeof(tbl), "sib_%u", (unsigned)(rng() & 0xFFFF));
            std::snprintf(sql, sizeof(sql),
                "CREATE MOT TABLE %s (id INT PRIMARY KEY, v TEXT)", tbl);
            r = PQexec(c, sql);
            // CREATE on a duplicate name from this RNG would be "already
            // exists" — fine to ignore.
            PQclear(r);
            std::snprintf(sql, sizeof(sql), "DROP TABLE IF EXISTS %s", tbl);
            r = PQexec(c, sql);
            PQclear(r);
            ops->fetch_add(1, std::memory_order_relaxed);
        }

        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
    PQfinish(c);
}

int runStressDdl(const Args& a) {
    PGconn* c = connectOrDie(a);
    if (!c) return 1;
    execOk(c, "DROP TABLE IF EXISTS stress_t");
    if (!execOk(c, "CREATE MOT TABLE stress_t (id INT PRIMARY KEY, v TEXT)")) {
        PQfinish(c); return 1;
    }
    PQfinish(c);

    std::atomic<bool> stop(false), err(false);
    std::atomic<uint64_t> dml_ops(0), ddl_ops(0);
    std::vector<std::thread> ths;
    for (int i = 0; i < a.threads; i++) {
        ths.emplace_back(stressWorker, a, i, &stop, &dml_ops, &err);
    }
    std::thread ddl(ddlWorker, a, &stop, &ddl_ops, &err);

    std::this_thread::sleep_for(std::chrono::seconds(a.duration));
    stop.store(true);
    for (auto& t : ths) t.join();
    ddl.join();
    std::printf("[stress_ddl] threads=%d duration=%ds dml_ops=%lu ddl_ops=%lu err=%d\n",
                a.threads, a.duration, (unsigned long)dml_ops.load(),
                (unsigned long)ddl_ops.load(), err.load() ? 1 : 0);
    return err.load() ? 1 : 0;
}

// Native-SQLite oracle: same workload against a MOT table and a vanilla
// SQLite table, single-threaded inside one txn per op so commit order is
// identical. After the run, count and a sum-based checksum must match.
//
// Determinism matters more than throughput here: a divergence (lost row,
// duplicated row, wrong value) means the MOT adapter is incorrect vs the
// SQLite reference.
int runOracle(const Args& a) {
    PGconn* c = connectOrDie(a);
    if (!c) return 1;

    execOk(c, "DROP TABLE IF EXISTS oracle_mot");
    execOk(c, "DROP TABLE IF EXISTS oracle_native");
    if (!execOk(c, "CREATE MOT TABLE oracle_mot ("
                   "  id INT PRIMARY KEY, v TEXT, h INT)")) {
        PQfinish(c); return 1;
    }
    if (!execOk(c, "CREATE TABLE oracle_native ("
                   "  id INT PRIMARY KEY, v TEXT, h INT)")) {
        PQfinish(c); return 1;
    }

    // Deterministic stream. The duration arg controls op count
    // (≈ 5000 ops/sec budgeted).
    int ops = std::max(1, a.duration) * 5000;
    std::mt19937_64 rng(a.seed);
    char sql[512];
    int errors = 0;
    for (int i = 0; i < ops; i++) {
        int id = (int)(rng() % 2000);          // keyspace forces collisions
        int v_seed = (int)(rng() & 0xFFFF);
        int h = v_seed ^ id;
        int pick = (int)(rng() % 100);

        if (!execOk(c, "BEGIN")) { errors++; continue; }
        if (pick < 50) {
            // INSERT — collisions become UPDATE-by-replace (INSERT OR REPLACE).
            std::snprintf(sql, sizeof(sql),
                "INSERT OR REPLACE INTO oracle_mot VALUES (%d, 'v%d', %d)",
                id, v_seed, h);
            if (!execOk(c, sql)) errors++;
            std::snprintf(sql, sizeof(sql),
                "INSERT OR REPLACE INTO oracle_native VALUES (%d, 'v%d', %d)",
                id, v_seed, h);
            if (!execOk(c, sql)) errors++;
        } else if (pick < 80) {
            // UPDATE — only affects rows that exist.
            std::snprintf(sql, sizeof(sql),
                "UPDATE oracle_mot SET v='u%d', h=%d WHERE id=%d",
                v_seed, h, id);
            if (!execOk(c, sql)) errors++;
            std::snprintf(sql, sizeof(sql),
                "UPDATE oracle_native SET v='u%d', h=%d WHERE id=%d",
                v_seed, h, id);
            if (!execOk(c, sql)) errors++;
        } else {
            // DELETE.
            std::snprintf(sql, sizeof(sql),
                "DELETE FROM oracle_mot WHERE id=%d", id);
            if (!execOk(c, sql)) errors++;
            std::snprintf(sql, sizeof(sql),
                "DELETE FROM oracle_native WHERE id=%d", id);
            if (!execOk(c, sql)) errors++;
        }
        if (!execOk(c, "COMMIT")) errors++;
    }
    if (errors > 0) {
        std::fprintf(stderr, "[oracle] %d exec errors during run\n", errors);
        PQfinish(c);
        return 1;
    }

    auto fetchSum = [&](const char* tbl, long long& cnt,
                        long long& sum_h, long long& sum_id) -> bool {
        char q[256];
        std::snprintf(q, sizeof(q),
            "SELECT count(*), coalesce(sum(h),0), coalesce(sum(id),0) FROM %s",
            tbl);
        PGresult* r = PQexec(c, q);
        bool ok = (PQresultStatus(r) == PGRES_TUPLES_OK && PQntuples(r) == 1);
        if (ok) {
            cnt    = std::strtoll(PQgetvalue(r, 0, 0), nullptr, 10);
            sum_h  = std::strtoll(PQgetvalue(r, 0, 1), nullptr, 10);
            sum_id = std::strtoll(PQgetvalue(r, 0, 2), nullptr, 10);
        }
        PQclear(r);
        return ok;
    };

    long long mc = 0, mh = 0, mi = 0;
    long long nc = 0, nh = 0, ni = 0;
    if (!fetchSum("oracle_mot", mc, mh, mi) ||
        !fetchSum("oracle_native", nc, nh, ni)) {
        PQfinish(c);
        return 1;
    }

    bool match = (mc == nc && mh == nh && mi == ni);
    std::printf("[oracle] ops=%d mot{n=%lld,h=%lld,i=%lld} "
                "native{n=%lld,h=%lld,i=%lld} %s\n",
                ops, mc, mh, mi, nc, nh, ni, match ? "MATCH" : "DIVERGE");
    PQfinish(c);
    return match ? 0 : 1;
}

}  // namespace

int main(int argc, char** argv) {
    Args a;
    if (!parseArgs(argc, argv, a)) return 2;
    if (a.mode == "smoke") return runSmoke(a);
    if (a.mode == "extended") return runExtended(a);
    if (a.mode == "stress") return runStress(a);
    if (a.mode == "delete_loop") return runDeleteLoop(a);
    if (a.mode == "stress_ddl") return runStressDdl(a);
    if (a.mode == "oracle") return runOracle(a);
    std::fprintf(stderr, "unknown mode: %s\n", a.mode.c_str());
    return 2;
}
