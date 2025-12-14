# FR-086: PostgreSQL Wire Protocol & Ecosystem Connectivity

> **Status**: Planning | **Priority**: Critical | **Tier**: Open Source + Enterprise

## Overview

PostgreSQL wire protocol support is an **adoption-critical feature** that enables Velostream to integrate with the entire PostgreSQL ecosystem: psql, dbt, Metabase, Grafana, DataGrip, JDBC, psycopg2, and hundreds of other tools.

This is NOT a differentiator - it's table stakes. [Materialize](https://materialize.com/blog/postgres-compatibility/) and [RisingWave](https://risingwave.com/glossary/postgresql-compatibility/) both have it. Without it, adoption is severely limited.

## The Viral Demo Moment

```bash
# The "holy shit" moment for data engineers:
curl -sSL https://velostream.dev/install | bash

# Connect with your existing psql client
psql -h localhost -p 5433 -d velostream

# Run streaming SQL from psql!
velostream=# SELECT symbol, AVG(price)
             FROM kafka://trades
             GROUP BY symbol
             EMIT CHANGES;
 symbol | avg_price
--------+-----------
 AAPL   | 189.45
 TSLA   | 242.33
 NVDA   | 487.22
(rows updating in real-time... Ctrl+C to stop)
```

**Why this is viral:**

1. Every data engineer knows psql
2. "Wait, I can run streaming SQL from psql?!"
3. No new tools to learn
4. Works with dbt, Metabase, DataGrip immediately
5. GIF-worthy streaming output

**Disruption Score**: 94/100 (based on FR-085 ranking formula)

## Commercial Tiering

### Open Source (Apache 2.0)

Core connectivity that drives adoption:

| Feature              | Description                        | Why Open Source      |
| -------------------- | ---------------------------------- | -------------------- |
| PgWire Server        | TCP listener, protocol handling    | Core connectivity    |
| SimpleQueryHandler   | psql, basic clients                | Minimum viable       |
| Password Auth (MD5)  | Basic authentication               | Minimum security     |
| Type Mapping         | FieldValue to PostgreSQL           | Core functionality   |
| Streaming Results    | EMIT CHANGES to continuous rows    | THIS IS THE PRODUCT  |
| Basic pg_catalog     | Tables, columns, types             | Tool compatibility   |
| Error Responses      | SqlError to PG error codes         | Usability            |

### Enterprise License

Production features for scale, security, compliance:

| Feature                | Description                  | Why Enterprise             |
| ---------------------- | ---------------------------- | -------------------------- |
| ExtendedQueryHandler   | Prepared statements, JDBC    | Performance optimization   |
| SCRAM-SHA-256          | Modern auth standard         | Security compliance        |
| SSL/TLS Encryption     | In-transit encryption        | Security compliance        |
| Connection Pooling     | Efficient resource usage     | Scale                      |
| Rate Limiting          | Multi-tenant protection      | Scale                      |
| Connection Audit Logs  | Who connected, when, what    | Compliance (SOC2/HIPAA)    |
| Full pg_catalog        | All system views             | Advanced tool support      |

## Implementation Phases (Viral-First Ordering)

### Phase 1: The 30-Second Demo (LoE: 2-3 weeks)

**Goal**: psql connects and runs a streaming query.

| Task  | Description                                        | LoE       |
| ----- | -------------------------------------------------- | --------- |
| 1.1   | Add pgwire dependency (v0.25+)                     | 2 hours   |
| 1.2   | TCP listener with configurable port (default 5433) | 4 hours   |
| 1.3   | StartupMessage handling (protocol v3.0)            | 8 hours   |
| 1.4   | Password authentication (MD5)                      | 8 hours   |
| 1.5   | SimpleQueryHandler skeleton                        | 8 hours   |
| 1.6   | Query routing to StreamExecutionEngine             | 16 hours  |
| 1.7   | RowDescription generation from schema              | 8 hours   |
| 1.8   | DataRow encoding for FieldValue types              | 16 hours  |
| 1.9   | Streaming result handling (EMIT CHANGES)           | 16 hours  |
| 1.10  | CommandComplete and ReadyForQuery                  | 4 hours   |
| 1.11  | Basic error response mapping                       | 8 hours   |
| 1.12  | Integration tests with psql                        | 16 hours  |

**Deliverable**: Run `psql -h localhost -p 5433 -c "SELECT * FROM kafka://topic LIMIT 10"`

**Files to Create**:

```
src/velostream/pgwire/
├── mod.rs              # Module exports
├── server.rs           # TCP listener, connection accept
├── connection.rs       # Per-connection state machine
├── startup.rs          # StartupMessage, auth handshake
├── simple_query.rs     # SimpleQueryHandler impl
├── type_mapping.rs     # FieldValue to PG types
├── result_encoder.rs   # RowDescription, DataRow
└── error_mapping.rs    # SqlError to PG error codes
```

### Phase 2: Tool Compatibility (LoE: 2-3 weeks)

**Goal**: dbt, Metabase, DataGrip work out of the box.

| Task  | Description                            | LoE       |
| ----- | -------------------------------------- | --------- |
| 2.1   | pg_catalog.pg_type (type definitions)  | 8 hours   |
| 2.2   | pg_catalog.pg_class (tables/streams)   | 8 hours   |
| 2.3   | pg_catalog.pg_attribute (columns)      | 8 hours   |
| 2.4   | pg_catalog.pg_namespace (schemas)      | 4 hours   |
| 2.5   | information_schema.tables              | 4 hours   |
| 2.6   | information_schema.columns             | 4 hours   |
| 2.7   | SHOW commands (TABLES, STREAMS, JOBS)  | 8 hours   |
| 2.8   | Backslash meta-command support         | 8 hours   |
| 2.9   | current_database(), version() funcs    | 4 hours   |
| 2.10  | Metabase integration testing           | 16 hours  |
| 2.11  | dbt adapter skeleton                   | 16 hours  |
| 2.12  | DataGrip/DBeaver testing               | 8 hours   |

**Deliverable**: Add Velostream as PostgreSQL data source in Metabase

**pg_catalog Implementation Strategy** (from Materialize's approach):

- Create views over internal velo_catalog tables
- Add shims incrementally as tools require them
- Focus on most-used tables first (pg_type, pg_class, pg_attribute)

### Phase 3: Prepared Statements (LoE: 2-3 weeks) - Enterprise

**Goal**: JDBC, psycopg2 with prepared statements work efficiently.

| Task  | Description                         | LoE       |
| ----- | ----------------------------------- | --------- |
| 3.1   | ExtendedQueryHandler skeleton       | 8 hours   |
| 3.2   | Parse message handling              | 8 hours   |
| 3.3   | Bind message handling               | 8 hours   |
| 3.4   | Execute message handling            | 8 hours   |
| 3.5   | Describe message handling           | 8 hours   |
| 3.6   | Prepared statement cache            | 16 hours  |
| 3.7   | Portal management                   | 8 hours   |
| 3.8   | Parameter binding ($1, $2, ...)     | 16 hours  |
| 3.9   | Binary format encoding              | 16 hours  |
| 3.10  | JDBC driver testing                 | 16 hours  |
| 3.11  | psycopg2 testing                    | 8 hours   |
| 3.12  | Connection pooler testing           | 8 hours   |

**Deliverable**: Java app with JDBC connects and runs prepared queries

### Phase 4: Production Hardening (LoE: 2-3 weeks) - Enterprise

**Goal**: Production-ready security and performance.

| Task  | Description                    | LoE       |
| ----- | ------------------------------ | --------- |
| 4.1   | SCRAM-SHA-256 authentication   | 16 hours  |
| 4.2   | SSL/TLS with rustls            | 16 hours  |
| 4.3   | Direct SSL negotiation (PG17+) | 8 hours   |
| 4.4   | Connection limits              | 8 hours   |
| 4.5   | Connection timeout handling    | 4 hours   |
| 4.6   | Graceful shutdown              | 8 hours   |
| 4.7   | Connection audit logging       | 8 hours   |
| 4.8   | Rate limiting per IP/user      | 8 hours   |
| 4.9   | Memory profiling               | 8 hours   |
| 4.10  | Performance benchmarks         | 16 hours  |
| 4.11  | Connection pool integration    | 8 hours   |
| 4.12  | Documentation                  | 16 hours  |

**Deliverable**: Production deployment with SSL, audit logs, connection limits

## Type Mapping Specification

### FieldValue to PostgreSQL

| FieldValue Variant         | PostgreSQL Type | OID  | Binary Format            |
| -------------------------- | --------------- | ---- | ------------------------ |
| Integer(i64)               | INT8/BIGINT     | 20   | 8-byte big-endian        |
| Float(f64)                 | FLOAT8/DOUBLE   | 701  | IEEE 754                 |
| String(String)             | TEXT            | 25   | UTF-8 bytes              |
| Boolean(bool)              | BOOL            | 16   | 1 byte (0/1)             |
| Null                       | NULL            | -    | -1 length                |
| Date(NaiveDate)            | DATE            | 1082 | Days since 2000-01-01    |
| Timestamp(NaiveDateTime)   | TIMESTAMP       | 1114 | Microseconds since 2000  |
| Decimal(Decimal)           | NUMERIC         | 1700 | PG numeric format        |
| ScaledInteger(i64, u8)     | NUMERIC         | 1700 | PG numeric format        |
| Array(Vec)                 | ARRAY           | -    | Array header + elements  |
| Map                        | JSONB           | 3802 | JSONB binary             |
| Struct                     | JSONB           | 3802 | JSONB binary             |

### Special Handling

**ScaledInteger (Financial Precision)**:

- `ScaledInteger(12345, 2)` represents `123.45`
- Encode as PostgreSQL NUMERIC to preserve precision
- Critical for financial analytics use case

**Streaming Results (EMIT CHANGES)**:

- Send rows continuously as they arrive
- Client sees updating results in psql
- Use PostgreSQL's row-by-row protocol (no buffering)

## Error Mapping Specification

| SqlError Variant | SQLSTATE                       | Severity |
| ---------------- | ------------------------------ | -------- |
| ParseError       | 42601 (syntax_error)           | ERROR    |
| SchemaError      | 42P01 (undefined_table)        | ERROR    |
| TypeError        | 42804 (datatype_mismatch)      | ERROR    |
| ExecutionError   | XX000 (internal_error)         | ERROR    |
| StreamError      | 58030 (io_error)               | ERROR    |
| WindowError      | 22023 (invalid_parameter)      | ERROR    |
| ResourceError    | 53000 (insufficient_resources) | ERROR    |

Include position information when available for IDE integration.

## pg_catalog Implementation

### Minimum Viable (Phase 2)

```sql
-- pg_catalog.pg_type
CREATE VIEW pg_catalog.pg_type AS
SELECT oid, typname, typnamespace, typlen, typtype
FROM velo_catalog.types;

-- pg_catalog.pg_class
CREATE VIEW pg_catalog.pg_class AS
SELECT oid, relname, relnamespace, relkind
FROM velo_catalog.streams
UNION ALL
SELECT oid, relname, relnamespace, relkind
FROM velo_catalog.tables;

-- pg_catalog.pg_attribute
CREATE VIEW pg_catalog.pg_attribute AS
SELECT attrelid, attname, atttypid, attnum
FROM velo_catalog.columns;
```

### Priority Order (based on tool requirements)

1. **pg_type** - Required by all clients for type resolution
2. **pg_class** - Required for table listing
3. **pg_attribute** - Required for column info
4. **pg_namespace** - Required for schema support
5. **pg_proc** - Required for function listing
6. **pg_index** - Required for some tools (can return empty)

## Integration with Existing Architecture

```
+---------------------------------------------------------------------+
|                    PostgreSQL Wire Protocol                          |
+---------------------------------------------------------------------+
|                                                                      |
|  +--------------+    +--------------+    +--------------+           |
|  | PgWireServer |--->| QueryHandler |--->| ResultEncoder|           |
|  |   (NEW)      |    |    (NEW)     |    |    (NEW)     |           |
|  +--------------+    +--------------+    +--------------+           |
|         |                   |                   |                    |
|         v                   v                   v                    |
|  +--------------+    +--------------+    +--------------+           |
|  | TCP/TLS      |    | Streaming    |    | Type         |           |
|  | Connections  |    | SqlParser    |    | Mapping      |           |
|  +--------------+    | (EXISTING)   |    +--------------+           |
|                      +--------------+                                |
|                             |                                        |
|                             v                                        |
|                      +--------------+                                |
|                      | Execution    |                                |
|                      | Engine       |                                |
|                      | (EXISTING)   |                                |
|                      +--------------+                                |
+---------------------------------------------------------------------+
```

### Key Integration Points

| Existing Component      | Integration                                     |
| ----------------------- | ----------------------------------------------- |
| StreamExecutionEngine   | Call execute_with_record() for each query       |
| StreamingSqlParser      | Reuse for parsing (may need dialect tweaks)     |
| ProcessorResult         | Convert Option to DataRow                       |
| FieldValue              | Map to PostgreSQL OIDs and binary format        |
| SqlError                | Map to PostgreSQL error codes                   |
| output_sender channel   | Use for streaming EMIT CHANGES results          |

## Gaps to Address

### Gap 1: SQL Dialect Differences

**Issue**: PostgreSQL SQL has features Velostream parser doesn't support.

**Solution**:

- Phase 1: Reject unsupported features with clear error
- Phase 2: Add compatibility mode that rewrites common patterns
- Examples: `::type` casting becomes `CAST()`

### Gap 2: pg_catalog Coverage

**Issue**: Tools query pg_catalog extensively.

**Solution**:

- Start with minimum viable (pg_type, pg_class, pg_attribute)
- Add shims based on tool testing
- Return empty results for unsupported tables rather than error

### Gap 3: Transaction Semantics

**Issue**: PostgreSQL has BEGIN/COMMIT/ROLLBACK. Streaming is different.

**Solution**:

- Phase 1: Single-statement auto-commit mode
- Return success for BEGIN/COMMIT (no-op)
- Reject multi-statement transactions with clear error

### Gap 4: Binary Protocol Encoding

**Issue**: Extended query protocol uses binary format.

**Solution**:

- Phase 1: Text format only (sufficient for psql)
- Phase 3: Add binary format for JDBC performance

### Gap 5: Connection State Management

**Issue**: Prepared statements require per-connection state.

**Solution**:

- Each connection gets ConnectionContext with prepared statement cache
- Use pgwire's PortalStore helper

## LoE Summary

| Phase       | Scope                | LoE          | Tier        |
| ----------- | -------------------- | ------------ | ----------- |
| Phase 1     | 30-Second Demo       | 2-3 weeks    | Open Source |
| Phase 2     | Tool Compatibility   | 2-3 weeks    | Open Source |
| Phase 3     | Prepared Statements  | 2-3 weeks    | Enterprise  |
| Phase 4     | Production Hardening | 2-3 weeks    | Enterprise  |
| **Total**   | Complete PgWire      | **8-12 wks** | Mixed       |

**Open Source Scope**: 4-6 weeks (Phase 1 + Phase 2)

**Enterprise Scope**: 4-6 weeks (Phase 3 + Phase 4)

## Dependencies

### Cargo.toml Additions

```toml
[dependencies]
pgwire = "0.25"

# TLS support (for Phase 4)
rustls = { version = "0.23", optional = true }
tokio-rustls = { version = "0.26", optional = true }

[features]
default = []
pgwire-tls = ["rustls", "tokio-rustls"]
```

### pgwire Crate Capabilities

The [pgwire crate](https://github.com/sunng87/pgwire) provides:

- Protocol message encoding/decoding
- StartupMessage handling
- Simple Query protocol
- Extended Query protocol
- Multiple auth methods (cleartext, MD5, SCRAM-SHA-256)
- SSL/TLS support
- COPY protocol

Used by: GreptimeDB, CeresDB, PeerDB, dozer, SpacetimeDB

## Success Criteria

### Phase 1 Complete

- `psql -h localhost -p 5433` connects successfully
- `SELECT * FROM kafka://topic LIMIT 10` returns rows
- `EMIT CHANGES` streams continuously until Ctrl+C
- Errors show meaningful messages with position

### Phase 2 Complete

- Metabase: Add as PostgreSQL data source, run queries
- dbt: `dbt run` executes models
- DataGrip: Schema browser shows streams/tables

### Phase 3 Complete

- JDBC: PreparedStatement works with parameters
- psycopg2: Parameterized queries work
- PgBouncer: Connection pooling works

### Phase 4 Complete

- SSL/TLS: Encrypted connections
- SCRAM-SHA-256: Secure auth
- Audit log: All connections logged
- Benchmarks: Protocol overhead under 10ms

## Files to Create

```
src/velostream/pgwire/
├── mod.rs                 # Module exports, feature gates
├── server.rs              # PgWireServer, TCP listener
├── connection.rs          # ConnectionHandler, state machine
├── startup.rs             # StartupMessage, auth negotiation
├── auth.rs                # AuthSource implementations
├── simple_query.rs        # SimpleQueryHandler
├── extended_query.rs      # ExtendedQueryHandler [Enterprise]
├── type_mapping.rs        # FieldValue to PG types
├── result_encoder.rs      # RowDescription, DataRow
├── error_mapping.rs       # SqlError to ErrorResponse
├── catalog/
│   ├── mod.rs             # Catalog module
│   ├── pg_type.rs         # pg_catalog.pg_type
│   ├── pg_class.rs        # pg_catalog.pg_class
│   ├── pg_attribute.rs    # pg_catalog.pg_attribute
│   └── pg_namespace.rs    # pg_catalog.pg_namespace
└── tests/
    ├── psql_test.rs       # psql integration tests
    ├── jdbc_test.rs       # JDBC tests [Enterprise]
    └── tool_compat_test.rs # Metabase, dbt tests

src/bin/
└── velo-pgwire.rs         # Standalone PgWire server binary
```

## References

- [pgwire crate](https://github.com/sunng87/pgwire) - PostgreSQL wire protocol in Rust
- [pgwire docs](https://docs.rs/pgwire) - API documentation
- [PostgreSQL Protocol](https://www.postgresql.org/docs/current/protocol.html) - Official protocol docs
- [Materialize pg_catalog](https://materialize.com/docs/sql/system-catalog/pg_catalog/) - Implementation approach
- [Materialize PostgreSQL Compatibility](https://materialize.com/blog/postgres-compatibility/) - Philosophy
- [RisingWave PostgreSQL Compatibility](https://risingwave.com/glossary/postgresql-compatibility/) - Feature list
