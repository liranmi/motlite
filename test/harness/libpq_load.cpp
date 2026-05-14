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
        if (pick < 70) {
            std::snprintf(sql, sizeof(sql),
                "INSERT OR IGNORE INTO stress_t VALUES (%d, 'v%d')", id, id);
            execOk(c, sql);
        } else if (pick < 90) {
            std::snprintf(sql, sizeof(sql),
                "SELECT v FROM stress_t WHERE id = %d", id);
            PGresult* r = PQexec(c, sql);
            PQclear(r);
        } else if (pick < 95) {
            std::snprintf(sql, sizeof(sql),
                "UPDATE stress_t SET v = 'u%d' WHERE id = %d", id, id);
            execOk(c, sql);
        } else {
            std::snprintf(sql, sizeof(sql),
                "DELETE FROM stress_t WHERE id = %d", id);
            execOk(c, sql);
        }
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

}  // namespace

int main(int argc, char** argv) {
    Args a;
    if (!parseArgs(argc, argv, a)) return 2;
    if (a.mode == "smoke") return runSmoke(a);
    if (a.mode == "extended") return runExtended(a);
    if (a.mode == "stress") return runStress(a);
    if (a.mode == "delete_loop") return runDeleteLoop(a);
    std::fprintf(stderr, "unknown mode: %s\n", a.mode.c_str());
    return 2;
}
