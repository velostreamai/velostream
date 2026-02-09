# Codebase Cleanup — TODO

## Completed

- [x] `src/main.rs` — deleted (redundant Kafka producer example; lib.rs is the proper entry point)
- [x] `bincode` dependency — removed from `Cargo.toml` (unused in entire `src/`)
- [x] `type_validator.rs` — deleted (`TypeValidator`, `TypeCategory` never used; removed from `validation/mod.rs`)
- [x] `providers.rs` — removed unused `JsonSchemaProvider` struct, `sample_size` fields from `FileSchemaProvider` and `S3SchemaProvider`
- [x] `circuit_breaker.rs` — audited; all items used in `phase_2_error_resource_test.rs` (false positives from cross-directory test analysis)
- [x] 4 disabled file datasource tests — deleted (`file_config_test.rs`, `file_datasource_test.rs`, `file_error_test.rs`, `file_watcher_test.rs`); APIs they tested have been removed/replaced
- [x] Stale commented-out test references — removed from `tests/unit/server/mod.rs` and `tests/unit/sql/execution/mod.rs`
- [x] `velo-test.rs:1904` `was_interrupted` — audited; value IS read after the for loop (lines 2071, 2087, 2093); false positive from `tokio::select!` control flow

## Medium Priority (future work)

- [ ] `once_cell` → `std::sync::LazyLock` — Rust 2024 edition has this in stdlib
- [ ] 720 clippy warnings in test code — top offenders: collapsible `if`, unnecessary `mut`, useless `vec!`

## Low Priority (long-term)

- [ ] Large files — `assertions.rs` (5.7K lines), `velo-test.rs` (5.2K), `parser.rs` (4.6K)
- [ ] 27 files with TODO/FIXME — audit and ticket
- [ ] 6 files with wildcard imports (`use *`)
- [ ] Duplicate transitive deps (axum 0.6/0.8, base64 0.21/0.22) — blocked on upstream
