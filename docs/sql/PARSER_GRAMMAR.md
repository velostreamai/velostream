# Velostream SQL Parser Grammar Reference

Complete formal grammar for the Velostream streaming SQL parser, with AST structure and syntax rules.

## Overview

The Velostream parser converts SQL text into an Abstract Syntax Tree (AST) for execution by the streaming engine. This document provides:
- EBNF-style grammar definitions
- AST structure for each construct
- Operator precedence rules
- Complete syntax examples

---

## Query Structure

### Main Query Types

```
STREAMING_QUERY = SELECT_STATEMENT
                | CREATE_STREAM_STATEMENT
                | CREATE_TABLE_STATEMENT
                | INSERT_STATEMENT
```

### SELECT Statement (Most Common)

```
SELECT_STATEMENT = SELECT select_list
                   FROM table_source
                   [FROM_ALIAS]
                   [JOIN_CLAUSES]
                   [WHERE_CLAUSE]
                   [GROUP_BY_CLAUSE]
                   [HAVING_CLAUSE]
                   [WINDOW_CLAUSE]
                   [ORDER_BY_CLAUSE]
                   [LIMIT_CLAUSE]

SELECT_LIST = select_item (',' select_item)*
select_item = '*'
            | expression [AS identifier]
            | identifier '.*'

TABLE_SOURCE = identifier

FROM_ALIAS = 'AS' identifier
           | identifier              # Implicit alias

WHERE_CLAUSE = 'WHERE' expression

GROUP_BY_CLAUSE = 'GROUP' 'BY' expression_list

HAVING_CLAUSE = 'HAVING' expression

ORDER_BY_CLAUSE = 'ORDER' 'BY' order_item (',' order_item)*
order_item = expression ['ASC' | 'DESC']

LIMIT_CLAUSE = 'LIMIT' number
```

**AST Structure**: `StreamingQuery::Select { fields, from, from_alias, joins, where_clause, group_by, having, window, order_by, limit }`

---

## Window Specifications

### ⭐ CRITICAL: Two Different Window Types

The Velostream parser has **TWO separate window mechanisms**:

1. **WINDOW clause** (on SELECT statement) - Time-based windows
2. **ROWS WINDOW** (inside OVER clause) - Row-count-based windows

They live in **different parts of the AST**.

### Time-Based Windows (WINDOW Clause)

Located in: `StreamingQuery::Select::window`

```
WINDOW_CLAUSE = 'WINDOW' window_spec

window_spec = TUMBLING_WINDOW
            | SLIDING_WINDOW
            | SESSION_WINDOW

TUMBLING_WINDOW = 'TUMBLING' '(' duration [ ',' identifier ] ')'

SLIDING_WINDOW = 'SLIDING' '(' duration ',' duration [ ',' identifier ] ')'

SESSION_WINDOW = 'SESSION' '(' duration [ ',' identifier ] ')'

duration = INTERVAL | simple_duration
INTERVAL = 'INTERVAL' string TIME_UNIT
simple_duration = number TIME_UNIT | number 'MS'

TIME_UNIT = 'SECOND' | 'MINUTE' | 'HOUR' | 'DAY'
```

**AST Structure**:
```rust
WindowSpec::Tumbling { size, time_column }
WindowSpec::Sliding { size, advance, time_column }
WindowSpec::Session { gap, time_column, partition_by }
```

**Examples**:
```sql
-- TUMBLING window: 5-minute fixed boundaries
WINDOW TUMBLING(INTERVAL '5' MINUTE)

-- SLIDING window: 10-minute window, advance every 2 minutes
WINDOW SLIDING(INTERVAL '10' MINUTE, INTERVAL '2' MINUTE)

-- SESSION window: 30-second inactivity gap
WINDOW SESSION(INTERVAL '30' SECOND)
```

### Row-Based Windows (ROWS WINDOW in OVER Clause)

Located in: `OverClause::window_spec` or function arguments

```
OVER_CLAUSE = 'OVER' '(' rows_window_spec | partition_order_spec ')'

rows_window_spec = 'ROWS' 'WINDOW'
                   'BUFFER' number 'ROWS'
                   [PARTITION_BY_CLAUSE]
                   [ORDER_BY_CLAUSE]
                   [WINDOW_FRAME_CLAUSE]
                   [EMIT_MODE_CLAUSE]

PARTITION_BY_CLAUSE = 'PARTITION' 'BY' expression_list

ORDER_BY_CLAUSE = 'ORDER' 'BY' order_item (',' order_item)*

WINDOW_FRAME_CLAUSE = 'ROWS' 'BETWEEN' frame_bound 'AND' frame_bound
                    | 'RANGE' 'BETWEEN' frame_bound 'AND' frame_bound

frame_bound = 'UNBOUNDED' 'PRECEDING'
            | 'CURRENT' 'ROW'
            | 'UNBOUNDED' 'FOLLOWING'
            | number 'PRECEDING'
            | number 'FOLLOWING'

EMIT_MODE_CLAUSE = 'EMIT' ('FINAL' | 'CHANGES')

partition_order_spec = [PARTITION_BY_CLAUSE] [ORDER_BY_CLAUSE] [WINDOW_FRAME_CLAUSE]
```

**AST Structure**:
```rust
WindowSpec::Rows {
    buffer_size: u32,
    partition_by: Vec<Expr>,
    order_by: Vec<OrderByExpr>,
    time_gap: Option<Duration>,
    window_frame: Option<WindowFrame>,
    emit_mode: RowsEmitMode,
    expire_after: RowExpirationMode,
}
```

**Examples**:
```sql
-- Basic 100-row window, no partitioning
LAG(price) OVER (ROWS WINDOW BUFFER 100 ROWS)

-- 100-row window per symbol, ordered by timestamp
AVG(price) OVER (
    ROWS WINDOW BUFFER 100 ROWS
    PARTITION BY symbol
    ORDER BY timestamp
)

-- Last 50 rows only (window frame)
SUM(amount) OVER (
    ROWS WINDOW BUFFER 100 ROWS
    PARTITION BY symbol
    ORDER BY timestamp
    ROWS BETWEEN 50 PRECEDING AND CURRENT ROW
)

-- Emit changes as new rows arrive
COUNT(*) OVER (
    ROWS WINDOW BUFFER 1000 ROWS
    PARTITION BY trader_id
    EMIT CHANGES
)
```

---

## Expression Syntax

### Operator Precedence (Highest to Lowest)

```
1. Parentheses and function calls:     ( )  FUNC()
2. Unary operators:                    -x   NOT x
3. Exponentiation:                     ^
4. Multiplication/Division/Modulo:     *    /    %
5. Addition/Subtraction:               +    -
6. Comparison operators:               =    !=   <>   <   >   <=   >=
7. Logical AND:                        AND
8. Logical OR:                         OR
```

### Expression Types

```
expression = logical_or_expr

logical_or_expr = logical_and_expr ('OR' logical_and_expr)*

logical_and_expr = comparison_expr ('AND' comparison_expr)*

comparison_expr = additive_expr (comparison_op additive_expr)*
comparison_op = '=' | '!=' | '<>' | '<' | '>' | '<=' | '>='
              | 'IN' | 'NOT' 'IN'
              | 'BETWEEN' additive_expr 'AND' additive_expr
              | 'LIKE' | 'IS' 'NULL' | 'IS' 'NOT' 'NULL'

additive_expr = multiplicative_expr (('+' | '-') multiplicative_expr)*

multiplicative_expr = unary_expr (('*' | '/' | '%') unary_expr)*

unary_expr = ['-' | 'NOT'] primary_expr

primary_expr = function_call
             | literal
             | identifier
             | '(' expression ')'
             | identifier '.' identifier  # Table.Column reference
             | identifier '[' number ']'   # Array access
```

### Literals

```
literal = NUMBER
        | STRING
        | 'TRUE' | 'FALSE'
        | 'NULL'

NUMBER = integer | float
STRING = '"' chars '"' | "'" chars "'"

identifier = [a-zA-Z_][a-zA-Z0-9_]*
           | "`" chars "`"         # Quoted identifier
           | '"' chars '"'         # Double-quoted identifier
```

### Function Calls

```
function_call = identifier '(' [function_args] ')'

function_args = expression (',' expression)*
              | '*'                  # COUNT(*), COUNT(DISTINCT *)
              | 'DISTINCT' expression
```

---

## Aggregation Functions

### Built-in Aggregates

These functions can be used in SELECT and WINDOW clauses:

```
COUNT([*] | [DISTINCT] expression)
SUM([DISTINCT] expression)
AVG([DISTINCT] expression)
MIN([DISTINCT] expression)
MAX([DISTINCT] expression)
STDDEV(expression)
VARIANCE(expression)
```

### Window Functions

These functions require OVER clause:

```
LAG(expression [, offset [, default]])
LEAD(expression [, offset [, default]])
ROW_NUMBER()
RANK()
DENSE_RANK()
FIRST_VALUE(expression)
LAST_VALUE(expression)
NTH_VALUE(expression, n)
PERCENT_RANK()
CUME_DIST()
```

---

## Common Syntax Rules

### Table and Column Aliases

```
-- AS is optional
SELECT column_name AS alias_name FROM table_name
SELECT column_name alias_name FROM table_name       -- Also valid

-- Table alias
SELECT * FROM my_table AS t
SELECT * FROM my_table t                            -- Also valid

-- Qualified columns
SELECT t.column_name FROM my_table t
SELECT table_name.column_name FROM table_name
```

### Case Insensitivity

```
-- All equivalent
SELECT * FROM orders WHERE amount > 100
select * from orders where amount > 100
SeLeCt * FrOm OrDeRs WhErE aMoUnT > 100
```

### Required Elements

- ✅ `FROM` clause is required (except `SELECT 1` style queries)
- ✅ `SELECT` must precede field list
- ❌ `WHERE` before `GROUP BY`
- ❌ `HAVING` before `ORDER BY`
- ❌ `ORDER BY` before `LIMIT`

---

## Complete Query Examples

### Simple SELECT with WHERE

```sql
SELECT order_id, customer_id, amount
FROM orders
WHERE amount > 100
LIMIT 10
```

**AST**:
```
StreamingQuery::Select {
  fields: [order_id, customer_id, amount],
  from: "orders",
  where_clause: Some(amount > 100),
  limit: Some(10)
}
```

### GROUP BY with Time Window

```sql
SELECT symbol, COUNT(*) as trade_count, AVG(price) as avg_price
FROM market_data
WHERE price > 0
GROUP BY symbol
WINDOW TUMBLING(INTERVAL '1' MINUTE)
```

**AST**:
```
StreamingQuery::Select {
  fields: [symbol, COUNT(*), AVG(price)],
  from: "market_data",
  where_clause: Some(price > 0),
  group_by: Some([symbol]),
  window: Some(Tumbling { size: 1m, time_column: None })
}
```

### ROWS WINDOW with Partition and Order

```sql
SELECT symbol, price,
       LAG(price, 1) OVER (
           ROWS WINDOW BUFFER 100 ROWS
           PARTITION BY symbol
           ORDER BY timestamp
       ) as prev_price
FROM trades
```

**AST**:
```
StreamingQuery::Select {
  fields: [
    symbol,
    price,
    LAG(price, 1) with OverClause {
      window_spec: Some(Rows {
        buffer_size: 100,
        partition_by: [symbol],
        order_by: [timestamp ASC],
        ...
      })
    }
  ],
  from: "trades"
}
```

### Complex: GROUP BY + Time Window + ROWS WINDOW

```sql
SELECT trader_id, symbol,
       COUNT(*) as trade_count,
       AVG(price) OVER (
           ROWS WINDOW BUFFER 1000 ROWS
           PARTITION BY trader_id, symbol
           ORDER BY timestamp
       ) as moving_avg
FROM market_data
GROUP BY trader_id, symbol
WINDOW TUMBLING(market_data.timestamp, INTERVAL '1' MINUTE)
HAVING COUNT(*) > 5
```

**AST**:
```
StreamingQuery::Select {
  fields: [...],
  from: "market_data",
  group_by: Some([trader_id, symbol]),
  having: Some(COUNT(*) > 5),
  window: Some(Tumbling { size: 1m, time_column: "market_data.timestamp" })
}
```

---

## Error Cases (What NOT to Do)

### ❌ Window Syntax Errors

```sql
❌ ROWS BUFFER 100 ROWS
✅ ROWS WINDOW BUFFER 100 ROWS

❌ ROWS WINDOW BUFFER 100
✅ ROWS WINDOW BUFFER 100 ROWS

❌ WINDOW ROWS 100
✅ ROWS WINDOW BUFFER 100 ROWS

❌ AVG(price) OVER ROWS WINDOW BUFFER 100
✅ AVG(price) OVER (ROWS WINDOW BUFFER 100 ROWS)
```

### ❌ Clause Order Errors

```sql
❌ SELECT * FROM orders GROUP BY customer_id WHERE amount > 100
✅ SELECT * FROM orders WHERE amount > 100 GROUP BY customer_id

❌ SELECT * FROM orders ORDER BY amount WINDOW TUMBLING(1m)
✅ SELECT * FROM orders WINDOW TUMBLING(1m) ORDER BY amount

❌ SELECT * FROM orders LIMIT 10 WHERE amount > 100
✅ SELECT * FROM orders WHERE amount > 100 LIMIT 10
```

### ❌ Missing Required Syntax

```sql
❌ SELECT * WHERE amount > 100
✅ SELECT * FROM orders WHERE amount > 100

❌ SELECT FROM orders
✅ SELECT * FROM orders

❌ COUNT(*) OVER BUFFER 100
✅ COUNT(*) OVER (ROWS WINDOW BUFFER 100 ROWS)
```

---

## Testing Grammar

When unsure about syntax, check:

1. **Parser Tests**: `tests/unit/sql/parser/` - Unit tests with exact syntax
2. **Integration Tests**: `tests/integration/sql/` - Real query examples
3. **Performance Tests**: `tests/performance/analysis/sql_operations/` - Production queries

Search for patterns:
```bash
grep -r "ROWS WINDOW BUFFER" tests/
grep -r "WINDOW TUMBLING" tests/
grep -r "GROUP BY.*WINDOW" tests/
```

---

## Quick Reference Table

| Feature | WINDOW Clause | ROWS WINDOW Clause | Location |
|---------|---------------|--------------------|----------|
| **Time-based** | ✅ YES | ❌ NO | SELECT window |
| **Count-based** | ❌ NO | ✅ YES | OVER clause |
| **PARTITION BY** | ❌ Not in syntax | ✅ YES | OVER clause |
| **ORDER BY** | ❌ Not in syntax | ✅ YES | OVER clause |
| **Watermarks** | ✅ YES (implicit) | ❌ NO | - |
| **Grace Period** | ✅ YES | ❌ NO | - |
| **Late Records** | ✅ Handled | ❌ N/A | - |
| **Use Case** | Aggregations | Moving windows, LAG/LEAD | - |

