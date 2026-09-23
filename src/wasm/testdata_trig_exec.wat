;; A BEFORE INSERT trigger guest that does DML through kaidb_exec (embed-wasm.md section 11). When
;; it fires (mid-INSERT into table t, so t's GroupLock is held by the outer statement), it issues an
;; UPDATE on the SAME table t through kaidb_exec, which re-enters the executor and borrows the outer
;; lock (per-(thread,txn) GroupLock re-entrancy). It then returns 1 (non-zero) to allow the INSERT.
;; This proves triggers can do in-process DML and that a nested statement can write the very table
;; whose DML is in flight.
;;
;; The request is a pre-built 'Q' frame for "UPDATE t SET flag = 9 WHERE id = 1":
;;   'Q' | u32 BE len = 40 (0x00000028) | u16 BE sqllen = 34 (0x0022) | the SQL (34 bytes)
;; total frame = 41 bytes, at offset 0; response scratch at 256 (cap 256).
;;
;; Rebuild:  wat2wasm src/wasm/testdata_trig_exec.wat -o src/wasm/testdata_trig_exec.wasm
(module
  (import "kaidb" "kaidb_exec" (func $exec (param i32 i32 i32 i32) (result i32)))
  (memory (export "memory") 1)
  (data (i32.const 0) "\51\00\00\00\28\00\22UPDATE t SET flag = 9 WHERE id = 1")
  (func (export "kaidb_udf") (result i64)
    (drop (call $exec (i32.const 0) (i32.const 41) (i32.const 256) (i32.const 256)))
    (i64.const 1)))
