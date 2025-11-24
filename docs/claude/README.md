# Claude Code Documentation

This directory contains documentation specifically for Claude Code working on the Velostream project.

## SQL Grammar Documentation

The most important documents for writing SQL correctly:

### 1. **[SQL_GRAMMAR_RULES.md](SQL_GRAMMAR_RULES.md)** ⭐ START HERE
- 14 specific rules Claude must follow
- Common mistakes to avoid
- Quick reference table
- How to handle uncertainty
- **Read this before writing ANY SQL**

### 2. **[../sql/COPY_PASTE_EXAMPLES.md](../sql/COPY_PASTE_EXAMPLES.md)** - Working Examples
- Copy-paste templates for all query types
- Pure SELECT, GROUP BY, Windows, ROWS WINDOW
- Time-based vs count-based windows
- Exactly what syntax works
- **Use this as a template library**

### 3. **[../sql/PARSER_GRAMMAR.md](../sql/PARSER_GRAMMAR.md)** - Complete Grammar Reference
- EBNF-style formal grammar
- AST structure for each construct
- Complete syntax rules
- Operator precedence
- Error cases and what NOT to do
- **Use this when you need formal definition**

## Quick Decision Tree

```
Do you need to write SQL?
│
├─ YES
│  ├─ Check: docs/sql/COPY_PASTE_EXAMPLES.md
│  ├─ If not found: Check: docs/sql/PARSER_GRAMMAR.md
│  ├─ If still unsure: grep tests/unit/sql/parser/
│  └─ If still unsure: Ask the user (don't guess!)
│
└─ NO
   └─ Continue with your task
```

## Key Rules Summary

### Rule 1: ROWS WINDOW Syntax
```
❌ ROWS BUFFER 100
✅ ROWS WINDOW BUFFER 100 ROWS
```

### Rule 2: Two Different Window Types
- **Time-based**: `WINDOW TUMBLING(INTERVAL '5' MINUTE)` (SELECT statement)
- **Count-based**: `ROWS WINDOW BUFFER 100 ROWS` (OVER clause)

### Rule 3: Clause Order
```sql
SELECT ... FROM ... WHERE ... GROUP BY ... HAVING ... WINDOW ... ORDER BY ... LIMIT ...
```

### Rule 4: Always Have FROM
```
❌ SELECT *
✅ SELECT * FROM table
```

## Truth Hierarchy (Use in This Order)

1. **COPY_PASTE_EXAMPLES.md** - Working examples (fastest)
2. **PARSER_GRAMMAR.md** - Formal grammar (when examples not found)
3. **tests/unit/sql/parser/** - Unit tests (grep for patterns)
4. **Ask the user** - If still uncertain (never guess)

## File Structure

```
docs/claude/
├── README.md                    (This file)
├── SQL_GRAMMAR_RULES.md        (14 rules Claude must follow)
│
docs/sql/
├── PARSER_GRAMMAR.md           (Formal EBNF grammar)
├── COPY_PASTE_EXAMPLES.md      (Working SQL templates)
├── (other SQL docs...)
│
CLAUDE.md                         (Main development guide)
                                  [Now includes SQL section]
```

## When to Use Each Document

### SQL_GRAMMAR_RULES.md
**Use when**: Writing SQL or unsure about syntax
- ✅ Before writing any SQL query
- ✅ When you see a syntax error
- ✅ When comparing two syntax options
- ✅ When verifying your generated SQL

### COPY_PASTE_EXAMPLES.md
**Use when**: Looking for working query patterns
- ✅ Need a template for a specific query type
- ✅ Want to see how to use ROWS WINDOW
- ✅ Looking for TIME WINDOW examples
- ✅ Need GROUP BY patterns

### PARSER_GRAMMAR.md
**Use when**: Need formal definition
- ✅ Understanding what's valid syntax
- ✅ Learning operator precedence
- ✅ Seeing AST structure
- ✅ Understanding grammar rules in detail

## Common Questions

### Q: What's the difference between WINDOW and ROWS WINDOW?
**A**: Different systems in different parts of AST. See SQL_GRAMMAR_RULES.md Rule 3 or PARSER_GRAMMAR.md "Two Different Window Types"

### Q: Why does `ROWS BUFFER 100` fail?
**A**: Must use `ROWS WINDOW BUFFER 100 ROWS`. See SQL_GRAMMAR_RULES.md Rule 2

### Q: Can I use GROUP BY and PARTITION BY together?
**A**: Yes, but they do different things. GROUP BY is top-level, PARTITION BY is in OVER clause. See SQL_GRAMMAR_RULES.md Rule 5

### Q: What's the correct clause order?
**A**: `SELECT...FROM...WHERE...GROUP BY...HAVING...WINDOW...ORDER BY...LIMIT...` See SQL_GRAMMAR_RULES.md Rule 4

## Before Every SQL Task

1. ✅ Open SQL_GRAMMAR_RULES.md
2. ✅ Search COPY_PASTE_EXAMPLES.md for similar query
3. ✅ Copy the pattern exactly
4. ✅ Verify against PARSER_GRAMMAR.md if unsure
5. ✅ Never guess - ask user if uncertain

## Links in CLAUDE.md

Main development guide now includes:
- Section: "SQL Grammar Rules for Claude"
- Links to all three grammar documents
- Quick reference of common mistakes
- Truth source hierarchy

See [CLAUDE.md](../../CLAUDE.md#sql-grammar-rules-for-claude) for integration.

## Updates & Maintenance

These documents are maintained in sync with:
- `src/velostream/sql/parser.rs` - Parser implementation
- `src/velostream/sql/ast.rs` - AST structure
- `tests/unit/sql/parser/*` - Grammar tests

If parser changes, these docs are updated.

---

**Remember**: SQL syntax is NOT optional in Velostream. Always verify against these sources before writing queries.
