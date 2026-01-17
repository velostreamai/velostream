# Schema Reference

This document describes the schema definition format for generating test data.

## Schema Structure

```yaml
schema:
  name: orders              # Schema name (required)
  namespace: trading        # Namespace for organization (optional)
  description: Order events # Human-readable description (optional)

fields:
  - name: field_name        # Field name (required)
    type: string            # Field type (required)
    description: Field desc # Description (optional)
    constraints: {}         # Constraints for data generation (optional)
```

## Field Types

### string

String/text values.

```yaml
- name: symbol
  type: string
  constraints:
    enum: [AAPL, GOOGL, MSFT]  # Fixed values
    weights: [0.5, 0.3, 0.2]   # Optional: probability weights
```

```yaml
- name: order_id
  type: string
  constraints:
    pattern: "ORD-[A-Z]{3}-[0-9]{6}"  # Regex pattern
```

```yaml
- name: description
  type: string
  constraints:
    min_length: 10
    max_length: 100
```

### integer

Integer values (i64).

```yaml
- name: quantity
  type: integer
  constraints:
    min: 1
    max: 1000
    distribution: uniform  # uniform, normal, log_normal, zipf
```

### decimal

Fixed-precision decimal values (for financial data).

```yaml
- name: price
  type: decimal
  precision: 19            # Total digits
  scale: 4                 # Decimal places
  constraints:
    min: 0.01
    max: 10000.00
    distribution: normal
```

### float

Floating-point values (f64).

```yaml
- name: temperature
  type: float
  constraints:
    min: -40.0
    max: 120.0
    distribution: normal
```

### boolean

Boolean true/false values.

```yaml
- name: is_active
  type: boolean
  constraints:
    probability: 0.8  # 80% chance of true
```

### timestamp

Date/time values.

```yaml
- name: event_time
  type: timestamp
  constraints:
    range: relative        # relative or absolute
    start: "-1h"           # For relative: duration before now
    end: "now"             # For relative: "now" or duration
    distribution: uniform
```

```yaml
- name: created_at
  type: timestamp
  constraints:
    range: absolute
    start: "2024-01-01T00:00:00Z"
    end: "2024-12-31T23:59:59Z"
```

## Constraint Types

### enum

Generate values from a fixed list.

```yaml
constraints:
  enum: [PENDING, APPROVED, REJECTED, CANCELLED]
  weights: [0.4, 0.3, 0.2, 0.1]  # Optional probability weights
```

If weights are not specified, values are uniformly distributed.

### min/max

Generate values within a range.

```yaml
# Integer range
constraints:
  min: 0
  max: 100

# Decimal range
constraints:
  min: 0.01
  max: 999.99

# Float range
constraints:
  min: -180.0
  max: 180.0
```

### pattern

Generate strings matching a regex pattern.

```yaml
constraints:
  pattern: "USR-[A-Z]{2}[0-9]{4}"
```

Supported pattern elements:
- `[A-Z]` - Uppercase letters
- `[a-z]` - Lowercase letters
- `[0-9]` - Digits
- `{n}` - Repeat exactly n times
- `{n,m}` - Repeat between n and m times
- Literal characters

### distribution

Control the statistical distribution of generated values.

```yaml
constraints:
  min: 0
  max: 100
  distribution: normal  # Bell curve, mean at center
```

**Available distributions:**

| Distribution | Description | Use Case |
|--------------|-------------|----------|
| `uniform` | Equal probability across range | Random IDs, dates |
| `normal` | Bell curve (Gaussian) | Natural measurements |
| `log_normal` | Skewed toward lower values | Prices, volumes |
| `zipf` | Power law (few large, many small) | Popularity rankings |

### derived

Generate values based on other fields.

```yaml
- name: total
  type: decimal
  constraints:
    derived: "price * quantity"
```

```yaml
- name: adjusted_price
  type: decimal
  constraints:
    derived: "price * random(0.95, 1.05)"  # Add random noise
```

**Supported operations:**
- Arithmetic: `+`, `-`, `*`, `/`
- Field references: `field_name`
- Random: `random(min, max)`

### references (Foreign Key)

Generate values that reference another schema.

```yaml
- name: customer_id
  type: string
  constraints:
    references:
      schema: customers    # Referenced schema name
      field: id            # Referenced field name
```

The generator will:
1. Sample values from previously generated records of the referenced schema
2. Fall back to placeholder values if no reference data is loaded

## Complete Example

```yaml
schema:
  name: orders
  namespace: ecommerce
  description: E-commerce order events

fields:
  - name: order_id
    type: string
    description: Unique order identifier
    constraints:
      pattern: "ORD-[A-Z]{3}-[0-9]{6}"

  - name: customer_id
    type: string
    description: Customer placing the order
    constraints:
      references:
        schema: customers
        field: id

  - name: product_id
    type: string
    description: Product being ordered
    constraints:
      references:
        schema: products
        field: id

  - name: status
    type: string
    description: Order status
    constraints:
      enum: [PENDING, CONFIRMED, SHIPPED, DELIVERED, CANCELLED]
      weights: [0.2, 0.3, 0.25, 0.2, 0.05]

  - name: quantity
    type: integer
    description: Number of items
    constraints:
      min: 1
      max: 10
      distribution: log_normal

  - name: unit_price
    type: decimal
    precision: 19
    scale: 2
    description: Price per unit
    constraints:
      min: 0.99
      max: 999.99
      distribution: log_normal

  - name: total_amount
    type: decimal
    precision: 19
    scale: 2
    description: Total order amount
    constraints:
      derived: "quantity * unit_price"

  - name: is_priority
    type: boolean
    description: Priority shipping requested
    constraints:
      probability: 0.15

  - name: order_time
    type: timestamp
    description: When order was placed
    constraints:
      range: relative
      start: "-24h"
      end: "now"
      distribution: uniform

  - name: notes
    type: string
    description: Optional order notes
    constraints:
      min_length: 0
      max_length: 200
```

## Schema Registry Integration

Schemas can be registered with an in-memory schema registry for Avro/Protobuf testing:

```rust
use velostream::test_harness::infra::TestHarnessInfra;

let mut infra = TestHarnessInfra::with_kafka("localhost:9092");
infra.start().await?;

// Register Avro schema
let avro_schema = r#"{
  "type": "record",
  "name": "Order",
  "fields": [
    {"name": "order_id", "type": "string"},
    {"name": "quantity", "type": "int"}
  ]
}"#;

let schema_id = infra.register_schema("orders-value", avro_schema).await?;
```

## Loading Reference Data

For foreign key relationships, load reference data before generating:

```rust
use velostream::test_harness::generator::SchemaDataGenerator;

let mut generator = SchemaDataGenerator::new(Some(42));  // Seed for reproducibility

// Option 1: Load explicit values
generator.load_reference_data("customers", "id", vec![
    FieldValue::String("CUST001".to_string()),
    FieldValue::String("CUST002".to_string()),
    FieldValue::String("CUST003".to_string()),
]);

// Option 2: Load from previously generated records
let customer_records = generator.generate(&customer_schema, 100)?;
generator.load_reference_data_from_records("customers", "id", &customer_records);

// Now generate orders with valid customer_id references
let order_records = generator.generate(&order_schema, 1000)?;
```

## Best Practices

1. **Use appropriate types** - Use `decimal` for money, `integer` for counts
2. **Set realistic ranges** - Match production data distributions
3. **Add weights to enums** - Reflect real-world frequency
4. **Use foreign keys** - Ensure referential integrity in test data
5. **Document fields** - Add descriptions for clarity
6. **Use seeds** - Set a seed for reproducible test runs
7. **Match production schemas** - Keep test schemas aligned with actual data
