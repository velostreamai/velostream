# Test Harness Examples (Reference)

This directory contains a few reference examples of velo-test spec files. The full test suite (8 tiers, 147 files) lives in the [velo-test](https://github.com/velostreamai/velo-test) repository.

## Examples

| Example | Description |
|---------|-------------|
| `tier1_basic/01_passthrough` | Simple SELECT * passthrough |
| `tier1_basic/03_filter` | WHERE clause filtering |
| `tier2_aggregations/12_tumbling_window` | Tumbling window aggregation with GROUP BY |

Each example has a `.sql` file (the streaming query) and a `.test.yaml` file (the test specification).

## Running

Install [velo-test](https://github.com/velostreamai/velo-test) and run:

```bash
velo-test run tier1_basic/01_passthrough.sql \
  --spec tier1_basic/01_passthrough.test.yaml \
  --use-testcontainers
```

## Full Test Suite

For the complete set of examples covering joins, window functions, serialization formats, fault injection, and more, see the [velo-test demo directory](https://github.com/velostreamai/velo-test/tree/main/demo/test_harness_examples).
