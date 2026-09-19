#!/usr/bin/env python3
"""kaidb primary-key ordering / range regression harness.

Exercises numeric AND text primary keys across insert / point-lookup / range / update /
delete / ORDER BY, checking that:
  - point lookups find every key (WHERE id = k)
  - range predicates return the correct set (id > k, id BETWEEN a AND b)
  - numeric PKs come back in NUMERIC order on `SELECT ... ORDER BY id` (1,2,10,20 not 1,10,2,20)
  - update + delete by PK work

Guards the order-preserving-key encoding: run before (numeric ranges full-scan, key order
lexicographic) and after (numeric-ordered, index range). Requires the server on :3008.
Exit: 0 = PASS, 1 = FAIL.
"""
import json, sys, urllib.request

URL = "http://127.0.0.1:3008/query"
def q(sql):
    d = json.dumps({"sql": sql, "session_token": ""}).encode()
    return json.loads(urllib.request.urlopen(urllib.request.Request(URL, data=d), timeout=15).read())

def ids(res): return [int(r[0]) for r in res["rows"]]
def strs(res): return [r[0] for r in res["rows"]]

fails = []
def check(name, cond, detail=""):
    print(f"  [{'ok' if cond else 'FAIL'}] {name}{'' if cond else '  '+detail}")
    if not cond: fails.append(name)

def main():
    # ---- numeric PK ----
    try: q("DROP TABLE nk")
    except Exception: pass
    q("CREATE TABLE nk (id INT PRIMARY KEY, v TEXT)")
    keys = [1, 2, 3, 10, 20, 100, 7, 55]
    for k in keys:
        q(f"INSERT INTO nk (id,v) VALUES ({k},'v{k}')")

    # point lookups
    miss = [k for k in keys if not ids(q(f"SELECT id FROM nk WHERE id = {k}")) == [k]]
    check("numeric point-lookup finds all keys", not miss, f"missing {miss}")

    # numeric ordering
    ordered = ids(q("SELECT id FROM nk ORDER BY id"))
    check("numeric ORDER BY id is numeric", ordered == sorted(keys), f"got {ordered}")

    # range
    gt5 = sorted(ids(q("SELECT id FROM nk WHERE id > 5")))
    check("range id>5", gt5 == sorted(k for k in keys if k > 5), f"got {gt5}")
    btw = sorted(ids(q("SELECT id FROM nk WHERE id BETWEEN 3 AND 20")))
    check("range id BETWEEN 3 AND 20", btw == sorted(k for k in keys if 3 <= k <= 20), f"got {btw}")

    # update + delete by PK
    q("UPDATE nk SET v = 'updated' WHERE id = 10")
    check("update by PK", strs(q("SELECT v FROM nk WHERE id = 10")) == ["updated"])
    q("DELETE FROM nk WHERE id = 20")
    check("delete by PK", not ids(q("SELECT id FROM nk WHERE id = 20")))
    check("count after delete", ids(q("SELECT COUNT(*) FROM nk")) == [len(keys) - 1])

    # ---- text PK (must still be correct) ----
    try: q("DROP TABLE tk")
    except Exception: pass
    q("CREATE TABLE tk (name TEXT PRIMARY KEY, n INT)")
    for nm in ["banana", "apple", "cherry", "date"]:
        q(f"INSERT INTO tk (name,n) VALUES ('{nm}',1)")
    check("text point-lookup", strs(q("SELECT name FROM tk WHERE name = 'cherry'")) == ["cherry"])
    check("text ORDER BY", strs(q("SELECT name FROM tk ORDER BY name")) == ["apple", "banana", "cherry", "date"])

    print("RESULT:", "PASS" if not fails else f"FAIL ({len(fails)}: {fails})")
    sys.exit(0 if not fails else 1)

if __name__ == "__main__":
    main()
