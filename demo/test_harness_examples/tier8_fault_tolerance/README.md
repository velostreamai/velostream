# Tier 8: Fault Tolerance & Error Handling

Examples demonstrating fault tolerance, error handling, and resilience testing.

## Overview

Production streaming systems must handle failures gracefully. This tier covers:

| Feature | Purpose |
|---------|---------|
| **Dead Letter Queue (DLQ)** | Capture failed records for debugging |
| **Fault Injection** | Test system resilience with injected failures |
| **Debug Mode** | Step-through analysis of pipeline stages |
| **Stress Testing** | Verify behavior under load and failure conditions |

## Examples

| File | Description |
|------|-------------|
| `70_dlq_basic.sql` | Basic DLQ configuration and usage |
| `72_fault_injection.sql` | Inject failures to test resilience |
| `73_debug_mode.sql` | Step-through debugging of pipelines |
| `74_stress_test.sql` | High-volume stress testing |

## Key Concepts

### Dead Letter Queue (DLQ)

DLQ captures records that fail processing:

```yaml
# Enable DLQ in test spec
fault_injection:
  enable_dlq: true
  dlq_max_size: 100

assertions:
  - type: dlq_count
    less_than: 5      # Expect few failures
```

### Fault Injection

Test system behavior under failure conditions:

```yaml
fault_injection:
  malformed_records:
    enabled: true
    rate: 0.05        # 5% malformed
    types: [missing_field, wrong_type, invalid_json]

  duplicates:
    enabled: true
    rate: 0.02        # 2% duplicates

  out_of_order:
    enabled: true
    rate: 0.10        # 10% out-of-order
```

### Error Recovery Patterns

Handle errors without crashing:

```sql
-- LogAndContinue: Skip bad records
-- @job_mode: simple
-- @enable_dlq: true
CREATE STREAM resilient AS
SELECT * FROM events EMIT CHANGES;
```

### Debug Mode

Debug mode provides an interactive REPL for step-by-step pipeline analysis.

**Starting a debug session:**

```bash
velo-test debug 73_debug_mode.sql
```

**Debug commands:**

| Command | Description |
|---------|-------------|
| `s`, `step` | Execute next statement |
| `c`, `continue` | Run until next breakpoint |
| `r`, `run` | Run all remaining statements |
| `b <name>` | Set breakpoint on statement |
| `u <name>` | Remove breakpoint from statement |
| `l`, `list` | List all statements with status |
| `i <name>` | Inspect output from statement |
| `st`, `status` | Show current execution state |
| `topics` | List all Kafka topics with offsets |
| `schema <topic>` | Show inferred schema for topic |
| `head <name>` | Show first N records from statement |
| `tail <name>` | Show last N records from statement |
| `filter <name> <expr>` | Filter records (e.g., `filter orders price>100`) |
| `export <name> <file>` | Export records to JSON/CSV file |
| `q`, `quit` | Exit debugger |

**Example session:**

```
$ velo-test debug 73_debug_mode.sql

🔧 Initializing debug infrastructure...
📋 Loaded 3 statements

🐛 Debug Commands:
   s, step       - Execute next statement
   l, list       - List all statements
   q, quit       - Exit debugger

(debug) l
   1. [ ] validated_orders
   2. [ ] enriched_orders
   3. [ ] final_orders

(debug) s
✅ Statement 1: validated_orders completed (5 records)

(debug) head validated_orders
┌──────────┬─────────────┬────────────┐
│ order_id │ customer_id │ line_total │
├──────────┼─────────────┼────────────┤
│ 1001     │ C100        │ 250.00     │
│ 1002     │ C101        │ 1500.00    │
└──────────┴─────────────┴────────────┘

(debug) b enriched_orders
Breakpoint set on 'enriched_orders'

(debug) c
✅ Statement 2: enriched_orders completed (5 records)
🔴 Hit breakpoint: enriched_orders

(debug) i enriched_orders
Records: 5
Fields: order_id, customer_id, line_total, order_tier, order_type
```

**Use cases:**

1. **Understanding pipeline behavior** - Step through each stage to see transformations
2. **Debugging complex logic** - Inspect CASE expressions and calculations
3. **Validating business rules** - Verify categorization and filtering
4. **Learning streaming concepts** - Interactive exploration of data flow

## Running Examples

```bash
# Basic DLQ
velo-test run 70_dlq_basic.sql

# With fault injection
velo-test run 72_fault_injection.sql

# Debug mode (interactive)
velo-test debug 73_debug_mode.sql

# Stress testing
velo-test stress 74_stress_test.sql --records 10000
```

## Best Practices

1. **Enable DLQ** - Always capture failed records in production
2. **Test with faults** - Inject failures during development
3. **Monitor DLQ size** - Alert when DLQ grows
4. **Set DLQ limits** - Prevent memory exhaustion
5. **Review DLQ regularly** - Investigate and fix root causes
