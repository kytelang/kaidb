# Custom TCP/TLS Wire Protocol

This document details the database wire protocol, design choices, message structures, and guidelines for developing custom client driver connectors.

---

## 1. Protocol Architecture: Length-Prefixed Framing

The database communicates over raw TCP/TLS (default port `3009` or the port defined in `db.json`) using a simple **Length-Prefixed Framing** pattern:

```
+---------------------------+-----------------------------------+
| Length Header (4 Bytes)   | Payload bytes (Variable Length)   |
| u32 little-endian         | JSON/UTF-8 string                 |
+---------------------------+-----------------------------------+
```

### Framing Specifications:
1. **Length Header (4 bytes)**: A 32-bit unsigned integer encoded in **little-endian** byte order. This integer represents the size of the payload in bytes (excluding the 4 header bytes).
2. **Payload (N bytes)**: A serialized JSON string representing the operations and query structures.

---

## 2. Message Structs & Schema (JSON Payload)

The database payload utilizes a single unified tagged union structure called `Packet` mapping to one of the following operations defined in `src/common/proto.zig`:

### A. Operations Types

#### 1. Query Request (`Query`)
Sent by the client to execute SQL queries.
```json
{
  "Query": {
    "sql": "SELECT * FROM users",
    "session_token": "a1b2c3d4..."
  }
}
```
* `sql`: The raw SQL query string to run.
* `session_token`: Optional string parameter containing the session token.

#### 2. Query Reply (`Reply`)
Sent by the server back to the client.
```json
{
  "Reply": {
    "status": "Ok",
    "data": "{\"columns\":[\"id\",\"name\"],\"rows\":[[\"1\",\"Alice\"]],\"rows_affected\":1}"
  }
}
```
* `status`: `"Ok"` or `"Error"`.
* `data`: A nested serialized JSON string representing the tabular output results:
  * For success: A serialized JSON string of the `QueryResponse` structure (`columns: []string`, `rows: [][]string`, `rows_affected: int`).
  * For errors: The raw string of the execution error.

#### 3. Replica WAL Synchronization (`ShipWal`)
Used inside replication loops for streaming write-ahead log updates.
```json
{
  "ShipWal": {
    "lsn": 105,
    "tx_id": 4,
    "timestamp": 1783775923112,
    "kind": 3,
    "table_name": "sys.objects",
    "key": "test_table",
    "value": "..."
  }
}
```

### B. Authentication & Session Flow Example

To authenticate a client driver session, the client initiates the handshake by querying a `LOGIN` statement, then extracts and attaches the returned session token to all future queries.

#### Step 1: Send Login Query Packet
The client sends a standard `Query` request containing the login SQL command and a `null` session token:
```json
{
  "Query": {
    "sql": "LOGIN admin 'admin'",
    "session_token": null
  }
}
```

#### Step 2: Receive Login Response Packet
If validation succeeds, the server replies with a `Reply` packet containing the session token:
```json
{
  "Reply": {
    "status": "Ok",
    "data": "{\"columns\":[\"session_token\"],\"rows\":[[\"a1b2c3d4e5f67890a1b2c3d4e5f67890\"]],\"rows_affected\":1}"
  }
}
```

#### Step 3: Run Future Queries with the Session Token
For all subsequent database queries on that connection, the client includes the session token hex string:
```json
{
  "Query": {
    "sql": "SELECT * FROM secure_table",
    "session_token": "a1b2c3d4e5f67890a1b2c3d4e5f67890"
  }
}
```

---

## 3. Developing Custom Driver Connectors

Because of this simple design, developers can write client connectors in any programming language in minutes. 

### Implementation Guide for Drivers (e.g. Python, Node.js, Go)

To build a connector in your language of choice, implement these core steps:

#### Step 1: Connect
Establish a raw TCP socket connection. If TLS is enabled in the configuration, wrap the socket using the standard library TLS library of the driver language (skipping certificate verification if using self-signed development keys).

#### Step 2: Write a Frame
1. Serialize your request payload to a JSON string:
   `payload = json.dumps({"Query": {"sql": sql_string, "session_token": token}})`
2. Convert the payload string to UTF-8 bytes.
3. Compute the payload length and pack it as a 4-byte little-endian integer.
4. Write both the 4-byte header and the payload bytes to the socket stream and flush.

#### Step 3: Read a Frame
1. Read exactly 4 bytes from the socket.
2. Unpack the bytes as a little-endian 32-bit unsigned integer to get the payload length `L`.
3. Read exactly `L` bytes from the socket to retrieve the complete payload data.
4. Parse the `L` bytes as a JSON string to extract the `Reply` response.

---

### Python Code Example (Minimal Driver)
```python
import socket
import ssl
import struct
import json

def execute_query(sql: str, host="127.0.0.1", port=3009, use_tls=False) -> dict:
    # 1. Connect
    sock = socket.create_connection((host, port))
    if use_tls:
        context = ssl.create_default_context()
        context.check_hostname = False
        context.verify_mode = ssl.CERT_NONE
        sock = context.wrap_socket(sock)
        
    try:
        # 2. Write Frame
        query_packet = {"Query": {"sql": sql, "session_token": None}}
        payload = json.dumps(query_packet).encode("utf-8")
        header = struct.pack("<I", len(payload))
        sock.sendall(header + payload)
        
        # 3. Read Frame
        resp_header = sock.recv(4)
        if len(resp_header) < 4:
            raise ConnectionError("Server disconnected")
        payload_len = struct.unpack("<I", resp_header)[0]
        
        resp_payload = b""
        while len(resp_payload) < payload_len:
            chunk = sock.recv(payload_len - len(resp_payload))
            if not chunk:
                raise ConnectionError("Truncated response")
            resp_payload += chunk
            
        # 4. Parse Response
        resp_packet = json.loads(resp_payload.decode("utf-8"))
        reply = resp_packet.get("Reply", {})
        if reply.get("status") == "Error":
            raise Exception(f"Database Error: {reply.get('data')}")
            
        return json.loads(reply.get("data", "{}"))
        
    finally:
        sock.close()
```

---

## 4. Evaluation and Design Trade-Offs

### Design Advantages
1. **Extreme Simplicity**: Eliminates the need to construct complex low-level binary state machines for handshakes, column layouts, or metadata.
2. **Debuggability**: Raw packets are easily logged as readable JSON text objects.
3. **Cross-Language Ease**: Eliminates custom serialization modules, relying on standard, ubiquitously supported HTTP/JSON parser toolsets.

### Scaling & Future-Proofing
If JSON parsing ever becomes a CPU bottleneck for heavy database traffic, the **4-byte length prefix framing is already built to scale**. 

The serializer/deserializer implementation inside `Packet` can be swapped to use a binary serializer (e.g. **MessagePack** or **BSON**) without changing a single line of TCP socket routing, buffering, or client connection code.
