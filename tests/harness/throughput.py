#!/usr/bin/env python3
"""Throughput/scaling harness for kaidb.

Unlike concurrency_stress.py (a CORRECTNESS gate), this measures how throughput scales with
concurrency, and separates read scaling from write scaling. That distinction is the whole point:

  * Reads take db.rw_lock SHARED  -> they SHOULD scale with worker count today.
  * Writes take db.rw_lock EXCLUSIVE -> serialized by the global write lock.

So:
  reads scale + writes flat  => the global write lock is the write ceiling (removing it is the fix)
  reads ALSO flat            => the bottleneck is the HTTP/server layer, NOT the lock

It uses **keep-alive** connections (one per worker) so per-request TCP setup (~5ms) does not
dominate and mask the engine. Start the server yourself in a scratch dir, then:

    python3 tests/harness/throughput.py            # default sweep 1,2,4,8,16
    python3 tests/harness/throughput.py 1,8,32     # custom worker counts
    python3 tests/harness/throughput.py 1,8 --secs 5
"""
import http.client
import json
import multiprocessing as mp
import sys
import threading
import time

HOST, PORT = "127.0.0.1", 3008
ROWS = 2000  # preloaded rows for the read benchmark


class Conn:
    """One keep-alive HTTP connection, reused across requests (schnell supports persistent
    connections), so per-request TCP setup does not mask the engine.

    Reconnects transparently: the server closes a connection after max_requests_per_connection
    (sending `Connection: close`), exactly as a real client must handle.
    """

    def __init__(self):
        self.c = None
        self._connect()

    def _connect(self):
        if self.c is not None:
            try:
                self.c.close()
            except Exception:
                pass
        self.c = http.client.HTTPConnection(HOST, PORT, timeout=30)

    def q(self, sql):
        body = json.dumps({"sql": sql, "session_token": ""})
        for attempt in (0, 1):
            try:
                self.c.request("POST", "/query", body=body, headers={"Content-Type": "application/json"})
                return self.c.getresponse().read()
            except Exception:
                if attempt == 1:
                    raise
                self._connect()  # server hung up (keep-alive cap) -> reconnect once

    def close(self):
        try:
            self.c.close()
        except Exception:
            pass


def _proc_main(idx, secs, kind, wbase, out_q):
    """Body of one client PROCESS: hammer the server for `secs`, report ops done.

    Processes (not threads) because at >10k ops/s the Python GIL makes a threaded client the
    bottleneck and flattens the curve regardless of what the server does.
    """
    c = Conn()
    if kind == "read":
        sql = lambda k: f"SELECT v FROM bench WHERE id={(idx * 7 + k) % ROWS}"
    else:
        sql = lambda k: f"INSERT INTO bench (id,v) VALUES ({wbase + idx * 1000000 + k},'x')"
    try:
        c.q(sql(0))  # warm
    except Exception:
        pass
    n = 0
    end = time.time() + secs
    while time.time() < end:
        try:
            c.q(sql(n))
            n += 1
        except Exception:
            break
    c.close()
    out_q.put(n)


def run_procs(workers, secs, kind, wbase):
    """Run `workers` client processes; return (total_ops, elapsed)."""
    q = mp.Queue()
    procs = [mp.Process(target=_proc_main, args=(i, secs, kind, wbase, q)) for i in range(workers)]
    t0 = time.time()
    for p in procs:
        p.start()
    total = sum(q.get() for _ in procs)
    for p in procs:
        p.join(timeout=30)
    return total, time.time() - t0


def run_pool(workers, secs, make_sql):
    """Run `workers` keep-alive clients for `secs`; return total ops completed."""
    counts = [0] * workers
    errors = []
    stop = threading.Event()
    barrier = threading.Barrier(workers + 1)

    def work(idx):
        c = Conn()
        try:
            c.q("SELECT v FROM bench WHERE id=1")  # warm the connection
        except Exception:
            pass
        barrier.wait()
        n = 0
        while not stop.is_set():
            try:
                c.q(make_sql(idx, n))
                n += 1
            except Exception as e:
                errors.append(f"w{idx}@{n}: {type(e).__name__}: {e}")
                break
        counts[idx] = n
        c.close()

    threads = [threading.Thread(target=work, args=(i,), daemon=True) for i in range(workers)]
    for t in threads:
        t.start()
    barrier.wait()
    t0 = time.time()
    time.sleep(secs)
    stop.set()
    for t in threads:
        t.join(timeout=30)
    return sum(counts), time.time() - t0, errors


def main():
    sweep = [1, 2, 4, 8, 16]
    secs = 3.0
    args = [a for a in sys.argv[1:]]
    if args and not args[0].startswith("--"):
        sweep = [int(x) for x in args[0].split(",")]
    if "--secs" in args:
        secs = float(args[args.index("--secs") + 1])

    setup = Conn()
    setup.q("CREATE TABLE bench (id INT PRIMARY KEY, v TEXT)")
    print(f"preloading {ROWS} rows ...", flush=True)
    for i in range(ROWS):
        setup.q(f"INSERT INTO bench (id,v) VALUES ({i},'v{i}')")

    print(f"\n{'workers':>7} | {'READ ops/s':>11} {'scale':>6} | {'WRITE ops/s':>11} {'scale':>6}")
    print("-" * 52)
    base_r = base_w = None
    wbase = 10_000_000  # write keyspace offset, keeps PKs unique across runs
    for w in sweep:
        n, dt = run_procs(w, secs, "read", 0)
        rps = n / dt
        base_r = base_r or rps
        n, dt = run_procs(w, secs, "write", wbase)
        wps = n / dt
        wbase += 100_000_000
        base_w = base_w or wps
        print(f"{w:>7} | {rps:>11.0f} {rps/base_r:>5.1f}x | {wps:>11.0f} {wps/base_w:>5.1f}x")

    setup.close()
    print(
        "\nreads scale + writes flat -> global write lock is the ceiling"
        "\nreads flat too            -> HTTP/server layer is the ceiling (lock removal buys nothing)"
    )


if __name__ == "__main__":
    main()
