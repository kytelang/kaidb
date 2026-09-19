#!/usr/bin/env python3
"""kaidb concurrency stress + correctness harness.

Drives the running server (HTTP :3008) with many parallel writers doing
insert/update/delete over disjoint key ranges, then verifies end-to-end that the
B+tree stayed consistent under concurrency:

  - every surviving key's point-lookup (WHERE id=k) matches the full-scan view
    (catches internal-node routing corruption from unsafe structure modifications)
  - COUNT(*) equals the surviving-key count
  - no lost or duplicated keys

This is the acceptance gate for any change to the engine's locking. It must PASS
under the current global write-lock (baseline); a failure after a lock change means
concurrent structure modifications corrupted the tree.

  Usage: concurrency_stress.py [WORKERS] [KEYS_PER_WORKER]
         (defaults: 16 workers x 300 keys)

Requires the server running on http://127.0.0.1:3008 (start it first).
Exit: 0 = PASS, 1 = FAIL.
"""
import json, sys, urllib.request, concurrent.futures as cf

URL = "http://127.0.0.1:3008/query"

def q(sql, timeout=20):
    d = json.dumps({"sql": sql, "session_token": ""}).encode()
    r = urllib.request.urlopen(urllib.request.Request(URL, data=d), timeout=timeout)
    return json.loads(r.read())

WORKERS = int(sys.argv[1]) if len(sys.argv) > 1 else 16
PER = int(sys.argv[2]) if len(sys.argv) > 2 else 300

def setup():
    try: q("CREATE TABLE stress (id INT PRIMARY KEY, v TEXT)")
    except Exception: pass

def worker(w):
    base = w * 100000
    for i in range(PER):
        q(f"INSERT INTO stress (id,v) VALUES ({base+i},'w{w}_{i}')")
    for i in range(0, PER, 2):          # update half
        q(f"UPDATE stress SET v = 'upd{w}_{i}' WHERE id = {base+i}")
    for i in range(0, PER, 4):          # delete a quarter
        q(f"DELETE FROM stress WHERE id = {base+i}")
    return w

def expected_survivors():
    s = set()
    for w in range(WORKERS):
        base = w * 100000
        for i in range(PER):
            if i % 4 == 0:  # deleted
                continue
            s.add(base + i)
    return s

def main():
    setup()
    print(f"stress: {WORKERS} workers x {PER} keys ...", flush=True)
    with cf.ThreadPoolExecutor(max_workers=WORKERS) as ex:
        list(ex.map(worker, range(WORKERS)))

    exp = expected_survivors()
    scan_ids = set(int(r[0]) for r in q("SELECT id FROM stress")["rows"])
    cnt = int(q("SELECT COUNT(*) FROM stress")["rows"][0][0])

    ok = True
    if scan_ids != exp:
        ok = False
        missing = list(exp - scan_ids)[:10]; extra = list(scan_ids - exp)[:10]
        print(f"  SCAN MISMATCH: have {len(scan_ids)} expected {len(exp)}; missing {missing}; extra {extra}")
    if cnt != len(exp):
        ok = False
        print(f"  COUNT MISMATCH: COUNT(*)={cnt} expected {len(exp)}")

    sample = list(exp)[::max(1, len(exp)//200)][:200]
    pl_fail = sum(1 for k in sample
                  if not (lambda rows: rows and int(rows[0][0]) == k)(q(f"SELECT id FROM stress WHERE id = {k}")["rows"]))
    if pl_fail:
        ok = False
        print(f"  POINT-LOOKUP FAIL: {pl_fail}/{len(sample)} survivors not found by WHERE id=k (routing corruption)")

    print("RESULT:", "PASS" if ok else "FAIL", f"(survivors={len(exp)}, scan={len(scan_ids)}, count={cnt})")
    sys.exit(0 if ok else 1)

if __name__ == "__main__":
    main()
