# SQL Grammar Documentation - Implementation Checklist

## What Was Created

✅ **4 New Documentation Files** specifically to teach Claude the correct SQL syntax

### 1. docs/sql/PARSER_GRAMMAR.md (12 KB)
- **Purpose**: Formal grammar reference with EBNF notation
- **Contains**: 
  - Complete grammar definitions for all SQL constructs
  - AST structure for each element
  - Two window systems clearly explained
  - Operator precedence table
  - Complete query examples with AST breakdown
  - Error cases and what NOT to do

### 2. docs/sql/COPY_PASTE_EXAMPLES.md (10 KB)
- **Purpose**: Working SQL templates you can copy directly
- **Contains**:
  - 40+ working SQL query examples
  - Organized by query type
  - Pure SELECT, GROUP BY, Windows, ROWS WINDOW
  - JOIN queries
  - Complex combinations
  - Common mistakes clearly marked
  - How to modify each example

### 3. docs/claude/SQL_GRAMMAR_RULES.md (12 KB)
- **Purpose**: Specific rules Claude must follow (14 rules total)
- **Contains**:
  - 14 non-negotiable syntax rules
  - ROWS WINDOW syntax (most common mistake)
  - Clause order (mandatory sequence)
  - Truth source hierarchy
  - How to verify syntax
  - Checklist before submitting SQL
  - Quick reference tables

### 4. docs/claude/README.md (4.8 KB)
- **Purpose**: Navigation and quick reference for all grammar docs
- **Contains**:
  - Quick decision tree (which doc to use)
  - File structure and organization
  - When to use each document
  - Common Q&A
  - Truth hierarchy summary

### 5. CLAUDE.md (Main Development Guide)
- **Updated**: Added "SQL Grammar Rules for Claude" section
- **Includes**:
  - Links to all new documents
  - Quick overview of two window systems
  - Common mistakes summary
  - Clause order reference
  - Truth sources

## How Claude Will Use These

### Before Writing SQL
1. Check `docs/sql/COPY_PASTE_EXAMPLES.md` for similar example
2. If not found → Check `docs/sql/PARSER_GRAMMAR.md`
3. If still unsure → Search `tests/unit/sql/parser/` 
4. If still uncertain → Ask the user (don't guess!)

### To Understand Grammar
- Reference `docs/sql/PARSER_GRAMMAR.md` with EBNF notation
- See real AST structure for each construct
- Understand operator precedence

### To Fix Syntax Errors
- Check `docs/claude/SQL_GRAMMAR_RULES.md` Rule 1-14
- Look up specific error in "Common Mistakes" section
- Compare with COPY_PASTE_EXAMPLES.md

### To Navigate Quickly
- Use `docs/claude/README.md` to find right document
- Use decision tree to pick which doc to read
- Use truth hierarchy to get fastest answer

## Key Distinctions Documented

### The TWO Window Systems
Both fully explained in each document:

1. **Time-Based WINDOW Clause** (in SELECT statement)
   - Syntax: `WINDOW TUMBLING(INTERVAL '5' MINUTE)`
   - For: Time bucketing with GROUP BY
   - Supports: Watermarks, grace periods, late records

2. **Count-Based ROWS WINDOW** (in OVER clause)
   - Syntax: `ROWS WINDOW BUFFER 100 ROWS`
   - For: Moving window functions (LAG, LEAD, moving avg)
   - No: Watermarks (rows only)

### The CRITICAL Syntax Rules
All clearly marked in SQL_GRAMMAR_RULES.md:

1. **ROWS WINDOW Syntax**
   - ✅ `ROWS WINDOW BUFFER 100 ROWS`
   - ❌ `ROWS BUFFER 100`
   - ❌ `ROWS WINDOW BUFFER 100` (missing ROWS)

2. **Clause Order** (mandatory sequence)
   - SELECT...FROM...WHERE...GROUP BY...HAVING...WINDOW...ORDER BY...LIMIT

3. **FROM Required**
   - ✅ `SELECT * FROM table`
   - ❌ `SELECT *` (incomplete)

4. **PARTITION BY vs GROUP BY**
   - GROUP BY: Aggregation (reduces rows)
   - PARTITION BY: Window function (keeps rows)

## Testing These Documents

### Verify Claude Will Use Them
```bash
# Search for examples
grep -r "ROWS WINDOW BUFFER" docs/sql/COPY_PASTE_EXAMPLES.md
# Should find: 20+ examples

# Check grammar reference
grep -n "ROWS_WINDOW =" docs/sql/PARSER_GRAMMAR.md
# Should find: Grammar definition

# Check rules
grep -n "Rule.*ROWS" docs/claude/SQL_GRAMMAR_RULES.md
# Should find: Detailed rules with examples
```

### Cross-Reference Verification
All documents reference each other:
- CLAUDE.md → All docs
- SQL_GRAMMAR_RULES.md → COPY_PASTE_EXAMPLES.md
- PARSER_GRAMMAR.md → Examples with AST breakdown
- README.md → All docs with hierarchy

## What This Solves

### Problem 1: "Claude guesses SQL syntax"
**Solution**: Can't guess when exact examples are in COPY_PASTE_EXAMPLES.md

### Problem 2: "Wrong window syntax"
**Solution**: Rule 2 in SQL_GRAMMAR_RULES.md + examples + checklist

### Problem 3: "Confusing ROWS WINDOW and WINDOW"
**Solution**: Clear explanation in every document, separate sections

### Problem 4: "Clause order errors"
**Solution**: Rule 4 + multiple examples showing exact order

### Problem 5: "No truth source for verification"
**Solution**: Four documents forming hierarchy, tests as ultimate source

## Implementation Complete ✅

All files created, formatted, and cross-referenced:
- ✅ PARSER_GRAMMAR.md - Formal grammar (12 KB)
- ✅ COPY_PASTE_EXAMPLES.md - Working examples (10 KB)
- ✅ SQL_GRAMMAR_RULES.md - Claude-specific rules (12 KB)
- ✅ docs/claude/README.md - Navigation guide (4.8 KB)
- ✅ CLAUDE.md - Updated with new SQL section

**Total Size**: 51 KB of carefully structured documentation
**Truth Sources**: 4 levels of specificity
**Examples**: 40+ working SQL queries
**Rules**: 14 explicit, non-negotiable syntax rules

## Next Steps

1. **When asking Claude to write SQL**:
   - Point to: `docs/claude/SQL_GRAMMAR_RULES.md`
   - Or point to: `docs/sql/COPY_PASTE_EXAMPLES.md`
   - Or both!

2. **For quick reference**:
   - Use: `docs/claude/README.md` (4.8 KB, quick decision tree)

3. **For formal grammar**:
   - Use: `docs/sql/PARSER_GRAMMAR.md` (comprehensive EBNF)

4. **For working templates**:
   - Use: `docs/sql/COPY_PASTE_EXAMPLES.md` (copy-paste ready)

5. **To train Claude on this codebase**:
   - Include path to: `docs/claude/README.md`
   - Then point to specific document needed

## File Locations Summary

```
docs/
├── claude/
│   ├── README.md                 ← Navigation guide
│   ├── SQL_GRAMMAR_RULES.md      ← 14 rules for Claude
│   └── IMPLEMENTATION_CHECKLIST.md (this file)
│
└── sql/
    ├── PARSER_GRAMMAR.md         ← Formal EBNF grammar
    ├── COPY_PASTE_EXAMPLES.md    ← Working templates
    └── (other SQL docs)

CLAUDE.md                           ← Main guide (now with SQL section)
```

---

**Status**: ✅ COMPLETE and READY FOR USE

These documents will ensure Claude writes correct SQL syntax for Velostream by providing:
1. Working examples to copy
2. Formal grammar to understand
3. Specific rules to follow
4. Truth sources to verify against

Claude will no longer need to guess!
