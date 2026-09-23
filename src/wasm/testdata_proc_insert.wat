;; An in-process stored-procedure guest (embed-wasm.md section 11). When CALLed, it issues an
;; INSERT through the kaidb_exec host import, which re-enters the executor under the caller's
;; transaction. This proves the guest -> kaidb_exec -> executeNested -> storage path end to end.
;;
;; The request is a pre-built simple-query ('Q') wire frame held in a data segment:
;;   byte 0      : 'Q' (0x51)
;;   bytes 1..5  : u32 BE frame length = 43 (0x0000002b) (counts these 4 bytes + the payload)
;;   bytes 5..7  : u16 BE SQL length  = 37 (0x0025)
;;   bytes 7..44 : the SQL text
;; The response is written by the host into the scratch region at offset 256 (cap 256); the
;; procedure returns the host's i32 result (response length, or negative) sign-extended to i64,
;; which CALL surfaces as the "status" column. The INSERT itself runs regardless of whether the
;; response fits, because kaidb_exec executes the frame before encoding the reply.
;;
;; Rebuild:  wat2wasm src/wasm/testdata_proc_insert.wat -o src/wasm/testdata_proc_insert.wasm
(module
  (import "kaidb" "kaidb_exec" (func $exec (param i32 i32 i32 i32) (result i32)))
  (memory (export "memory") 1)
  (data (i32.const 0) "\51\00\00\00\2b\00\25INSERT INTO t (id, v) VALUES (7, 700)")
  (func (export "kaidb_udf") (result i64)
    (i64.extend_i32_s
      (call $exec (i32.const 0) (i32.const 44) (i32.const 256) (i32.const 256)))))
