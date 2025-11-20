# SELECT Alias Reuse: Three Approaches Compared

**Question**: Which approach is simplest and most intuitive for solving your circuit breaker alias problem?

---

## Your Query Problem

```sql
SELECT
    symbol,
    -- ... metrics ...
    CASE
        WHEN MAX(volume) > 5 * AVG(volume) THEN 'EXTREME_SPIKE'
        WHEN MAX(volume) > 3 * AVG(volume) THEN 'HIGH_SPIKE'
        ELSE 'NORMAL'
    END AS spike_classification,

    -- ❌ PROBLEM: Can't reference spike_classification here
    CASE
        WHEN spike_classification IN ('EXTREME_SPIKE', 'STATISTICAL_ANOMALY')
            AND STDDEV_POP(volume) > 3 THEN 'PAUSE_FEED'
        ELSE 'ALLOW'
    END AS circuit_state
```

---

## Three Possible Solutions

### **Approach 1: SELECT Alias Reuse (Native Support)**

**Status**: ❌ Not yet implemented | 🔧 Proposed in this design doc
**LoE**: 40-60 hours

```sql
-- AFTER implementation, this works directly:
SELECT
    spike_classification,
    CASE WHEN spike_classification IN ('EXTREME', 'STATISTICAL_ANOMALY')
        THEN 'TRIGGER_BREAKER'
    ELSE 'ALLOW'
    END AS circuit_state
```

**Pros:**
- ✅ Simplest query structure (1 SELECT)
- ✅ Most readable and intuitive
- ✅ Standard in MySQL 8.0+ and SQL Server
- ✅ No workaround complexity
- ✅ Best performance (no extra layers)

**Cons:**
- ❌ Requires implementation work (40-60 hours)
- ❌ Not yet available

**When to use**: Long-term (implement now, use tomorrow)

---

### **Approach 2: Subquery (SQL Standard Workaround)**

**Status**: ✅ Works today with Velostream | ⚠️ Nested SELECT limitation
**LoE**: 0 hours (immediate)

```sql
-- Workaround: Use subquery to materialize first SELECT
SELECT
    spike_classification,
    CASE WHEN spike_classification IN ('EXTREME', 'STATISTICAL_ANOMALY')
        THEN 'TRIGGER_BREAKER'
    ELSE 'ALLOW'
    END AS circuit_state
FROM (
    SELECT
        symbol,
        -- ... metrics ...
        CASE
            WHEN MAX(volume) > 5 * AVG(volume) THEN 'EXTREME_SPIKE'
            ELSE 'NORMAL'
        END AS spike_classification
    FROM market_data_ts
    GROUP BY symbol
) t
```

**Pros:**
- ✅ Works TODAY (no waiting)
- ✅ Standardized SQL approach
- ✅ Good for complex logic separation
- ✅ Alias available in outer SELECT

**Cons:**
- ❌ Extra nesting (harder to read)
- ❌ Slight performance overhead
- ❌ More verbose (8-10 more lines)
- ⚠️ Nested subqueries not supported (you can't nest this further)

**When to use**: If you need it NOW, and nesting is sufficient

---

### **Approach 3: Repeat Expression (No Workaround)**

**Status**: ✅ Works today | ⚠️ Not maintainable
**LoE**: 0 hours (immediate)

```sql
-- Repeat the CASE logic in both SELECT expressions
SELECT
    symbol,
    CASE
        WHEN MAX(volume) > 5 * AVG(volume) THEN 'EXTREME_SPIKE'
        WHEN MAX(volume) > 3 * AVG(volume) THEN 'HIGH_SPIKE'
        ELSE 'NORMAL'
    END AS spike_classification,

    -- Repeat condition instead of referencing alias
    CASE
        WHEN (
            CASE
                WHEN MAX(volume) > 5 * AVG(volume) THEN 'EXTREME_SPIKE'
                WHEN MAX(volume) > 3 * AVG(volume) THEN 'HIGH_SPIKE'
                ELSE 'NORMAL'
            END
        ) IN ('EXTREME_SPIKE', 'HIGH_SPIKE')
            AND STDDEV_POP(volume) > 3 THEN 'PAUSE_FEED'
        ELSE 'ALLOW'
    END AS circuit_state
FROM market_data_ts
GROUP BY symbol
```

**Pros:**
- ✅ Works TODAY
- ✅ No nesting required

**Cons:**
- ❌ Code duplication (DRY violation)
- ❌ Hard to maintain (edit in 2 places)
- ❌ Easy to make mistakes (keep logic in sync)
- ❌ Less readable
- ❌ Performance: evaluates same expression twice

**When to use**: Temporary quick fix only

---

## Comparison Matrix

| Criterion | Alias Reuse | Subquery | Repeat |
|-----------|------------|----------|--------|
| **Available Now** | ❌ | ✅ | ✅ |
| **LoE to Implement** | 40-60 hrs | 0 hrs | 0 hrs |
| **Implementation Date** | ~1 week | Immediate | Immediate |
| **Query Readability** | ⭐⭐⭐⭐⭐ | ⭐⭐⭐ | ⭐ |
| **Maintainability** | ⭐⭐⭐⭐⭐ | ⭐⭐⭐ | ⭐ |
| **Performance** | ⭐⭐⭐⭐⭐ | ⭐⭐⭐ | ⭐⭐ |
| **SQL Standard** | ❌ | ✅ | ✅ |
| **MySQL Compatible** | ✅ | ✅ | ✅ |
| **Nested Subquery** | N/A | ⚠️ Limited | N/A |
| **Vertical Scalability** | ✅ | ⚠️ | ⚠️ |
| **Team Familiarity** | Low | High | Medium |

---

## Recommendation

### For Your Trading Query

**Recommended Path**:

1. **SHORT TERM** (This week): Use **Approach 2 (Subquery)**
   - Works immediately
   - Provides clear separation of logic
   - Single level of nesting is acceptable
   - Gets your feature into production

2. **MEDIUM TERM** (Next sprint): Implement **Approach 1 (Alias Reuse)**
   - Start implementation now (40-60 hours)
   - Available in 1 week
   - Refactor query to use native support
   - Remove subquery layer
   - Improve readability

3. **NEVER**: Use Approach 3 (Repeat Expression)
   - Only temporary debugging
   - Maintenance nightmare
   - Code duplication anti-pattern

---

## Implementation Timeline

### Now (Day 1)
Use **Subquery Approach**:
```sql
CREATE STREAM volume_spike_analysis AS
SELECT
    spike_classification,
    CASE WHEN spike_classification IN ('EXTREME_SPIKE', 'STATISTICAL_ANOMALY')
        THEN 'PAUSE_FEED'
    ELSE 'ALLOW'
    END AS circuit_state
FROM (
    SELECT
        symbol,
        -- ... your metrics ...
        CASE ... END AS spike_classification
    FROM market_data_ts
    GROUP BY symbol
)
```

**Status**: ✅ Ready to deploy
**Time**: < 30 minutes to refactor

### Tomorrow-ish (Sprint 2)
Start implementing **Alias Reuse**:
- Planning: 2 hours
- Implementation: 40-50 hours
- Testing: 8-10 hours
- Review/Deploy: 2-3 hours

**Total**: ~1 week

### End of Week
Use **Alias Reuse Approach**:
```sql
CREATE STREAM volume_spike_analysis AS
SELECT
    symbol,
    CASE WHEN MAX(volume) > 5 * AVG(volume) THEN 'EXTREME_SPIKE'
    ELSE 'NORMAL'
    END AS spike_classification,
    CASE WHEN spike_classification IN ('EXTREME_SPIKE', 'STATISTICAL_ANOMALY')
        THEN 'PAUSE_FEED'
    ELSE 'ALLOW'
    END AS circuit_state
FROM market_data_ts
```

**Status**: ✅ Cleaner, native support
**Benefit**: No subquery overhead, more intuitive

---

## Simplicity Ranking

**Simplest Approach to Learn & Use** (for developers):
1. **Approach 1 (Alias Reuse)** ⭐ BEST
   - MySQL/SQL Server standard
   - Intuitive (references work like variables)
   - No nesting complexity

2. **Approach 2 (Subquery)** ⭐ GOOD
   - Standard SQL workaround
   - Learning curve for subquery mechanics
   - Performance mental model required

3. **Approach 3 (Repeat)** ⭐ POOR
   - Tempts bad habits
   - No learning value
   - Increases cognitive load

---

## Intuition & Best Practices

### Why Developers Expect Alias Reuse to Work

```sql
-- This FEELS like it should work:
SELECT
    x + y AS sum_xy,           -- 1. Define sum_xy
    sum_xy * 2 AS double_sum   -- 2. Use it here
```

**Natural mental model**: "I defined `sum_xy`, so I can use it"

### SQL Reality (Pre-MySQL 8.0)

```sql
-- This is how SQL actually works (traditional):
SELECT
    x + y AS sum_xy,              -- Alias ONLY available after SELECT
    (x + y) * 2 AS double_sum     -- Must repeat expression
```

**Unintuitive for many developers**

### Modern SQL (MySQL 8.0+, SQL Server)

```sql
-- Both work now:
SELECT
    x + y AS sum_xy,
    sum_xy * 2 AS double_sum   -- ✅ Alias reference works!
```

**Velostream will follow this model** (Approach 1)

---

## Decision Matrix for Your Case

**Answer these questions**:

1. **Do you need this TODAY?**
   - YES → Use Subquery (Approach 2)
   - NO → Implement Alias Reuse (Approach 1)

2. **Is simplicity the top priority?**
   - YES → Use Alias Reuse (requires implementation)
   - NO → Use Subquery (available now)

3. **Will you reference this query often?**
   - YES → Invest in Alias Reuse (cleaner to maintain)
   - NO → Use Subquery (one-time cost)

4. **Do you have time for implementation?**
   - YES → Implement Alias Reuse (1 week)
   - NO → Use Subquery (30 mins)

---

## Conclusion

**For your trading analytics feature:**

| Timeline | Approach | Why |
|----------|----------|-----|
| **This week** | Subquery | Immediate delivery, acceptable nesting |
| **Next week** | Alias Reuse (new feature) | Cleaner, native support, reduce technical debt |
| **Long-term** | Alias Reuse | Standard, maintainable, team best practices |

**The hybrid strategy**:
1. Deploy with subquery TODAY (working solution)
2. Implement alias reuse NEXT WEEK (permanent solution)
3. Refactor query to use native support (cleanup)

---

**Document Summary**:
- Alias Reuse: Best long-term (requires 40-60 hour implementation)
- Subquery: Best short-term (works now, minor overhead)
- Repeat: Never (code duplication, maintenance nightmare)

**Your Choice**: Balance immediate needs vs. long-term code quality

---

**Next Steps**:
- [ ] Choose approach based on timeline
- [ ] If Subquery: Refactor query in next 30 mins
- [ ] If Alias Reuse: Review design doc, start implementation planning
- [ ] If Hybrid: Do subquery now, schedule alias reuse for next sprint
