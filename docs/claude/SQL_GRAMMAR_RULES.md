# SQL Grammar Rules for Claude Code

**DO NOT GUESS - Follow These Rules Exactly**

This document is specifically for Claude Code to follow when writing or analyzing SQL for Velostream. These are non-negotiable rules extracted from the actual parser implementation.

---

## Rule 1: Always Verify Against Truth Sources (Do This First!)

When writing ANY SQL query:

1. **Check this file** for specific rules
2. **Check `docs/sql/COPY_PASTE_EXAMPLES.md`** for working examples
3. **Check `docs/sql/PARSER_GRAMMAR.md`** for formal grammar
4. **Search `tests/unit/sql/parser/`** for test cases with exact syntax
5. **ONLY THEN** write or modify SQL

**If you can't find the syntax in these sources, ask the user to clarify rather than guessing.**

---

## Rule 2: ROWS WINDOW Syntax (Critical - Most Common Mistake)

**ROWS WINDOW is COUNT-based, not TIME-based. It has different syntax than WINDOW clauses.**

### Correct ROWS WINDOW Syntax

```
ROWS WINDOW BUFFER N ROWS [PARTITION BY ...] [ORDER BY ...] [ROWS BETWEEN ...]
```

**MUST Include**:
- `ROWS WINDOW` keyword (not just `ROWS`)
- `BUFFER N ROWS` (not `BUFFER N` or `ROWS N`)
- Parentheses around entire OVER clause

### Examples - CORRECT ✅

```sql
-- Minimal ROWS WINDOW
LAG(price) OVER (ROWS WINDOW BUFFER 100 ROWS)

-- With PARTITION BY
AVG(price) OVER (
    ROWS WINDOW BUFFER 100 ROWS
    PARTITION BY symbol
)

-- With ORDER BY
SUM(qty) OVER (
    ROWS WINDOW BUFFER 1000 ROWS
    PARTITION BY symbol
    ORDER BY timestamp
)

-- With window frame
AVG(price) OVER (
    ROWS WINDOW BUFFER 100 ROWS
    PARTITION BY symbol
    ORDER BY timestamp
    ROWS BETWEEN 50 PRECEDING AND CURRENT ROW
)

-- With EMIT CHANGES
COUNT(*) OVER (
    ROWS WINDOW BUFFER 1000 ROWS
    PARTITION BY trader_id
    EMIT CHANGES
)
```

### Examples - WRONG ❌ (Never Do This)

```sql
❌ ROWS BUFFER 100 ROWS                        (Missing "WINDOW")
❌ ROWS WINDOW BUFFER 100                      (Missing "ROWS" after number)
❌ WINDOW ROWS 100                             (Wrong order, should be ROWS WINDOW)
❌ LAG(price) OVER ROWS WINDOW BUFFER 100      (Missing parentheses)
❌ OVER (ROWS BUFFER 100)                      (Missing "WINDOW")
❌ OVER (ROWS WINDOW BUFFER 100)               (Missing "ROWS" after number)
```

**If you write ANY of these, the query will FAIL.**

---

## Rule 3: Two Different Window Types - Know the Difference

### Type 1: Time-Based WINDOW (In SELECT statement)

**Location**: Top-level SELECT clause, after HAVING, before ORDER BY

**Used for**: Time-bucketed aggregations

**Syntax**:
```sql
WINDOW TUMBLING(INTERVAL 'N' MINUTE)
WINDOW SLIDING(INTERVAL 'N' MINUTE, INTERVAL 'N' MINUTE)
WINDOW SESSION(INTERVAL 'N' SECOND)
```

**Examples**:
```sql
SELECT symbol, COUNT(*)
FROM trades
GROUP BY symbol
WINDOW TUMBLING(INTERVAL '5' MINUTE)              -- ✅ Correct

SELECT symbol, AVG(price)
FROM market_data
GROUP BY symbol
WINDOW SLIDING(INTERVAL '10' MINUTE, INTERVAL '2' MINUTE)  -- ✅ Correct
```

**In AST**: `StreamingQuery::Select::window`

### Type 2: Row-Based ROWS WINDOW (In OVER clause)

**Location**: Inside OVER clause of window function

**Used for**: Row-count-based window functions (LAG, LEAD, moving averages)

**Syntax**:
```sql
ROWS WINDOW BUFFER N ROWS [PARTITION BY ...] [ORDER BY ...]
```

**Examples**:
```sql
SELECT symbol,
    LAG(price) OVER (
        ROWS WINDOW BUFFER 100 ROWS
        PARTITION BY symbol
        ORDER BY timestamp
    )
FROM trades                                        -- ✅ Correct

SELECT symbol,
    AVG(price) OVER (
        ROWS WINDOW BUFFER 100 ROWS
        PARTITION BY symbol
    )
FROM trades                                        -- ✅ Correct
```

**In AST**: `OverClause::window_spec` → `WindowSpec::Rows`

### Quick Decision Tree

```
Is it a time-based operation?
├─ YES → Use WINDOW TUMBLING(INTERVAL '...')
│        Located in SELECT statement
│        Used with GROUP BY
│        Supports watermarks
│
└─ NO (count-based)?
   └─ YES → Use ROWS WINDOW BUFFER N ROWS
            Located in OVER clause
            Used with window functions
            No watermarks
```

---

## Rule 4: Clause Order (Mandatory)

**Must appear in this exact order** (optional clauses shown in brackets):

```
SELECT ...
FROM ...
[FROM_ALIAS or AS alias]
[WHERE ...]
[GROUP BY ...]
[HAVING ...]
[WINDOW ...]           ← Time-based window, if used
[ORDER BY ...]
[LIMIT ...]
```

### Correct Order ✅

```sql
SELECT symbol, COUNT(*)
FROM trades
WHERE price > 100
GROUP BY symbol
HAVING COUNT(*) > 5
WINDOW TUMBLING(INTERVAL '5' MINUTE)
ORDER BY symbol
LIMIT 100
```

### Wrong Order ❌

```sql
❌ SELECT * FROM orders ORDER BY amount GROUP BY customer_id
❌ SELECT * FROM orders LIMIT 10 WHERE amount > 100
❌ SELECT * FROM orders GROUP BY symbol HAVING COUNT(*) > 5 WHERE price > 0
```

---

## Rule 5: PARTITION BY vs GROUP BY

**These are NOT the same.**

### GROUP BY (Aggregation)
- Reduces rows
- Combines multiple rows into one per group
- Located: After WHERE, before HAVING
- Example: `GROUP BY symbol` → One row per symbol

### PARTITION BY (Window Function)
- Keeps all rows
- Divides rows into groups for window calculation
- Located: Inside OVER clause
- Example: `PARTITION BY symbol` → Each row gets window calculation per its symbol

### Example Showing the Difference

```sql
-- GROUP BY: Reduces to 1 row per symbol
SELECT symbol, COUNT(*) FROM trades
GROUP BY symbol
-- Result: 1 row per symbol

-- PARTITION BY: Keeps all rows, adds window column
SELECT symbol, price,
    COUNT(*) OVER (ROWS WINDOW BUFFER 100 ROWS PARTITION BY symbol)
FROM trades
-- Result: All original rows, plus COUNT column calculated per symbol
```

---

## Rule 6: Aliases Must Use AS or Implicit Format

### Explicit AS (Recommended)

```sql
SELECT column_name AS alias_name
SELECT table_name AS t

-- Multiple aliases
SELECT
    order_id AS id,
    customer_id AS cust_id,
    amount AS total
FROM orders AS o
```

### Implicit Format (Also Valid)

```sql
SELECT column_name alias_name
SELECT table_name t

-- Multiple implicit aliases
SELECT
    order_id id,
    customer_id cust_id,
    amount total
FROM orders o
```

### Both Work, But Use Explicit AS for Clarity

When writing SQL for tests or documentation, use explicit `AS` for readability:

```sql
✅ SELECT order_id AS id FROM orders
❌ SELECT order_id id FROM orders        (Avoids ambiguity)
```

---

## Rule 7: Parentheses in OVER Clause

**ALWAYS required when ROWS WINDOW is used.**

### Correct ✅

```sql
LAG(price, 1) OVER (
    ROWS WINDOW BUFFER 100 ROWS
    PARTITION BY symbol
)

AVG(price) OVER (
    ROWS WINDOW BUFFER 100 ROWS
    PARTITION BY symbol
    ORDER BY timestamp
)
```

### Wrong ❌

```sql
❌ LAG(price) OVER ROWS WINDOW BUFFER 100
❌ AVG(price) OVER ROWS WINDOW BUFFER 100 ROWS PARTITION BY symbol
```

---

## Rule 8: FROM is Required

**Every SELECT must have FROM** (except edge cases like `SELECT 1`).

### Correct ✅

```sql
SELECT * FROM orders
SELECT order_id FROM orders WHERE amount > 100
SELECT COUNT(*) FROM trades
```

### Wrong ❌

```sql
❌ SELECT *
❌ SELECT order_id WHERE amount > 100
```

**Exception**: Only for simple constant expressions:
```sql
✅ SELECT 1
✅ SELECT 'test'
```

---

## Rule 9: Expression Precedence (Important for WHERE/HAVING)

When combining conditions, remember operator precedence:

```
1. Parentheses:        (expression)
2. NOT:                NOT condition
3. AND:                a AND b
4. OR:                 a OR b
```

**Without parentheses**:
```sql
-- Evaluated as: (a AND b) OR c
WHERE a AND b OR c

-- Evaluated as: a AND (b OR c)
WHERE a AND (b OR c)
```

**Always use parentheses for clarity**:
```sql
WHERE (price > 100 AND quantity >= 10)
WHERE (symbol = 'AAPL' AND trader_id = 'T1') OR (symbol = 'GOOGL')
```

---

## Rule 10: Keywords Are Case-Insensitive, But Use UPPERCASE

All valid:
```sql
select * from orders
SELECT * FROM orders
SeLeCt * FrOm OrDeRs
```

**Convention**: Write keywords in UPPERCASE, identifiers in lowercase:
```sql
SELECT symbol, price FROM market_data
WHERE price > 100
GROUP BY symbol
```

---

## Rule 11: Verify Syntax Before Writing

**Before generating ANY SQL:**

1. Check `docs/sql/COPY_PASTE_EXAMPLES.md` for working example
2. If no example exists, check `docs/sql/PARSER_GRAMMAR.md`
3. If uncertain after checking both, search: `grep -r "pattern" tests/unit/sql/parser/`
4. If still uncertain, **ask the user to clarify** rather than guessing

**Do not generate SQL from memory. Always reference documentation.**

---

## Rule 12: Common Mistakes I Must Never Make

### ❌ Mistake 1: ROWS vs ROWS WINDOW

```sql
❌ ROWS BUFFER 100
❌ ROWS WINDOW BUFFER 100        (Missing "ROWS")
✅ ROWS WINDOW BUFFER 100 ROWS
```

### ❌ Mistake 2: Confusing WINDOW Types

```sql
❌ ROWS WINDOW TUMBLING(1m)       (ROWS is count-based, not time)
❌ WINDOW BUFFER 100 ROWS         (WINDOW is for time, use ROWS WINDOW)
✅ WINDOW TUMBLING(INTERVAL '1' MINUTE)     (For time)
✅ ROWS WINDOW BUFFER 100 ROWS              (For count)
```

### ❌ Mistake 3: Missing Parentheses

```sql
❌ SELECT price OVER ROWS WINDOW BUFFER 100
✅ SELECT price OVER (ROWS WINDOW BUFFER 100 ROWS)
```

### ❌ Mistake 4: Wrong Clause Order

```sql
❌ WHERE first, then GROUP BY
✅ WHERE first, then GROUP BY
❌ ORDER BY before LIMIT
✅ LIMIT comes after ORDER BY
```

### ❌ Mistake 5: PARTITION BY in Time Window

```sql
❌ WINDOW TUMBLING(INTERVAL '1' MINUTE) PARTITION BY symbol
   (PARTITION BY goes in ROWS WINDOW, not time WINDOW)
✅ WINDOW TUMBLING(INTERVAL '1' MINUTE)
```

---

## Rule 13: How to Handle Uncertainty

If you're uncertain about SQL syntax:

1. **Search the examples**:
   ```bash
   grep -r "your_syntax" docs/sql/COPY_PASTE_EXAMPLES.md
   ```

2. **Check the grammar**:
   - Look in `docs/sql/PARSER_GRAMMAR.md`
   - Find the section that covers your query type
   - Copy the exact syntax

3. **Search tests**:
   ```bash
   grep -r "your_pattern" tests/unit/sql/parser/
   ```

4. **If still uncertain, ask the user**:
   - "I'm not certain about the exact syntax for [description]"
   - "Should this be [option1] or [option2]?"
   - Show what you found in the docs, and ask for clarification

**Never guess or make up syntax.**

---

## Rule 14: Validating SQL After Writing

After generating SQL:

1. ✅ Check against example in `COPY_PASTE_EXAMPLES.md`
2. ✅ Verify clause order matches the template
3. ✅ Verify keyword spelling (especially ROWS WINDOW, PARTITION BY, ORDER BY)
4. ✅ Verify parentheses placement in OVER clauses
5. ✅ Run against actual tests if possible: `grep -r "similar_pattern" tests/`

**If any check fails, revise the SQL.**

---

## Quick Reference: Window Syntax Comparison

| Aspect | Time WINDOW | ROWS WINDOW |
|--------|-------------|------------|
| **Full Syntax** | `WINDOW TUMBLING(INTERVAL '1' MINUTE)` | `ROWS WINDOW BUFFER 100 ROWS` |
| **Location** | SELECT statement (top-level) | OVER clause (function argument) |
| **Use Case** | Time bucketing with GROUP BY | Moving window functions (LAG, LEAD) |
| **PARTITION BY** | ❌ Not in WINDOW syntax | ✅ Yes, PARTITION BY expr |
| **ORDER BY** | ❌ Not in WINDOW syntax | ✅ Yes, ORDER BY expr |
| **Window Frame** | ❌ No | ✅ Yes, ROWS BETWEEN ... AND ... |
| **EMIT** | ❌ No | ✅ Yes, EMIT CHANGES/FINAL |
| **Requires parentheses** | ❌ No | ✅ Yes (OVER clause) |
| **Example** | `GROUP BY symbol WINDOW TUMBLING(...)` | `AVG(price) OVER (ROWS WINDOW ...)` |

---

## File Locations (Truth Sources)

When verifying syntax, check these files IN ORDER:

1. **`docs/sql/COPY_PASTE_EXAMPLES.md`** - Working examples (start here)
2. **`docs/sql/PARSER_GRAMMAR.md`** - Formal grammar and rules
3. **`tests/unit/sql/parser/*_test.rs`** - Unit tests (grep for patterns)
4. **`tests/integration/sql/*_test.rs`** - Integration tests
5. **`tests/performance/analysis/sql_operations/`** - Production examples

If you can't find your syntax in any of these, ask the user before proceeding.

---

## Rule 15: KEY Annotation for Kafka Message Keys (FR-089)

The `KEY` keyword marks a field as the Kafka message key. It goes **after** the field or alias.

### Syntax

```sql
SELECT column KEY, ...                     -- Single key
SELECT col1 KEY, col2 KEY, ...            -- Compound key
SELECT column AS alias KEY, ...           -- KEY with alias (alias becomes key name)
```

### Correct ✅

```sql
-- Single key
SELECT symbol KEY, price FROM trades

-- Compound key (produces JSON: {"region":"US","product":"Widget"})
SELECT region KEY, product KEY, SUM(qty) FROM orders GROUP BY region, product

-- KEY with alias (uses alias name as key)
SELECT stock_symbol AS sym KEY, price FROM market_data

-- KEY with GROUP BY
SELECT symbol KEY, COUNT(*) as trade_count
FROM trades
GROUP BY symbol
WINDOW TUMBLING(INTERVAL '1' MINUTE)
```

### Wrong ❌

```sql
❌ SELECT KEY symbol, price FROM trades    -- KEY must come AFTER field/alias
❌ SELECT symbol, KEY price FROM trades    -- KEY must come AFTER field/alias
❌ SELECT KEY, price FROM trades           -- KEY is not a column name
```

### Key Behavior

| Scenario | Kafka Key |
|----------|-----------|
| `symbol KEY` (single field) | Raw value: `"AAPL"` |
| `a KEY, b KEY` (compound) | JSON: `{"a":"X","b":"Y"}` |
| `col AS alias KEY` | Uses alias name |
| `GROUP BY symbol` (no KEY) | Auto-generates from GROUP BY columns |
| No KEY, no GROUP BY | Null key or use `sink.key_field` property |

---

## Final Checklist Before Submitting SQL

- [ ] Syntax matches `COPY_PASTE_EXAMPLES.md` or `PARSER_GRAMMAR.md`
- [ ] Clause order is correct (SELECT...FROM...WHERE...GROUP BY...HAVING...WINDOW...ORDER BY...LIMIT)
- [ ] Keywords spelled correctly (especially ROWS WINDOW, PARTITION BY, ORDER BY)
- [ ] Parentheses present in OVER clauses
- [ ] FROM clause is present
- [ ] Time WINDOW syntax uses INTERVAL format if applicable
- [ ] ROWS WINDOW includes "ROWS" after buffer size
- [ ] No confusion between GROUP BY and PARTITION BY
- [ ] ROWS WINDOW uses count-based buffers, WINDOW uses time-based intervals
- [ ] KEY annotation placed AFTER field/alias (not before)

**If any check fails, fix it before submitting.**

