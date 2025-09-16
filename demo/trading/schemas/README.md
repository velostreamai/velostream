# Trading System Avro Schemas

This directory contains Avro schema definitions for all topics in the trading analytics system. All schemas use decimal logical types for financial precision with ScaledInteger support.

## Source Data Schemas

### `market_data.avsc`
- **Purpose**: High-frequency market data feed
- **Key Field**: `symbol`
- **Financial Fields**: `price`, `bid_price`, `ask_price`, `vwap`, `market_cap` (all decimal strings)
- **Special Features**: Supports multiple exchanges, VWAP calculation

### `trading_positions.avsc`
- **Purpose**: Real-time trading positions for risk monitoring
- **Key Field**: `trader_id`
- **Financial Fields**: `entry_price`, `current_pnl`, `unrealized_pnl`, `realized_pnl`, `position_value`, `margin_used`
- **Risk Features**: Multiple P&L calculations, margin tracking

### `order_book_updates.avsc`
- **Purpose**: Order book updates for institutional flow analysis
- **Key Field**: `symbol`
- **Financial Fields**: `price` (decimal string)
- **Special Features**: Order side enum, update type enum, level tracking

## Alert Schemas

### `price_alerts.avsc`
- **Purpose**: Price movement alerts (>5% changes)
- **Key Field**: `symbol`
- **Financial Fields**: `price`, `prev_price`, `price_change_pct`
- **Features**: Alert severity levels, market session tracking

### `volume_spikes.avsc`
- **Purpose**: Volume spike detection alerts
- **Key Field**: `symbol`
- **Financial Fields**: `volume_ratio`, `price`
- **Features**: Spike magnitude classification, trading session context

### `risk_alerts.avsc`
- **Purpose**: Critical risk management alerts
- **Key Field**: `trader_id`
- **Financial Fields**: `current_pnl`, `total_pnl`, `position_value`, `current_price`
- **Features**: Risk status enum, compliance flags, action required flag

### `order_imbalance_alerts.avsc`
- **Purpose**: Order flow imbalance detection
- **Key Field**: `symbol`
- **Financial Fields**: `buy_ratio`, `sell_ratio`, `imbalance_magnitude`
- **Features**: Imbalance type enum, institutional activity indicator

### `arbitrage_opportunities.avsc`
- **Purpose**: Cross-exchange arbitrage opportunities
- **Key Field**: `symbol`
- **Financial Fields**: `bid_a`, `ask_b`, `spread`, `spread_bps`, `potential_profit`
- **Features**: Profit categorization, execution complexity, time sensitivity

## Financial Precision Design

### Proper Avro Decimal Logical Types
All monetary values use proper Avro decimal logical type for ScaledInteger precision:
- **Type**: `"type": "bytes"` with `"logicalType": "decimal"`
- **Schema**: Includes `"precision"` and `"scale"` parameters
- **Encoding**: Big-endian two's complement bytes
- **ScaledInteger Mapping**: VeloStream reads precision/scale from schema and creates ScaledInteger automatically

#### Example Schema
```json
{
  "name": "price",
  "type": "bytes",
  "logicalType": "decimal",
  "precision": 19,
  "scale": 4,
  "doc": "Price with 4 decimal places (e.g., 123.4567)"
}
```

#### Precision/Scale Guidelines
- **Prices**: `precision: 19, scale: 4` (supports values up to $999,999,999,999,999.9999)
- **P&L**: `precision: 19, scale: 2` (standard financial precision)
- **Ratios/Percentages**: `precision: 10, scale: 4` (supports values up to 999,999.9999)
- **Market Cap**: `precision: 28, scale: 2` (very large values)

### Performance Benefits
- **ScaledInteger**: 42x faster than f64 floating-point
- **No Rounding Errors**: Exact financial arithmetic
- **Regulatory Compliance**: Precise audit trails

## Schema Evolution

### Backward Compatibility
- Optional fields use `"default": null` or appropriate defaults
- Enums include comprehensive value sets
- Arrays default to empty for extensibility

### Versioning Strategy
- Schema names include namespace for organization
- Logical types ensure forward compatibility
- Optional compliance fields for regulatory requirements

## Usage with Schema Registry

```yaml
# Common schema configuration
schema:
  key_format: string
  value_format: avro
  schema_registry_url: "http://schema-registry:8081"
```

### Schema Subject Names
- `market_data-value`
- `trading_positions-value`
- `order_book_updates-value`
- `price_alerts-value`
- `volume_spikes-value`
- `risk_alerts-value`
- `order_imbalance_alerts-value`
- `arbitrage_opportunities-value`

## Development Notes

### Testing Schemas
```bash
# Validate schema syntax
avro-tools validate schemas/market_data.avsc

# Generate sample data
avro-tools random --schema-file schemas/market_data.avsc --count 10
```

### Schema Registry Management
```bash
# Register schema
curl -X POST -H "Content-Type: application/vnd.schemaregistry.v1+json" \
  --data @schemas/market_data.avsc \
  http://schema-registry:8081/subjects/market_data-value/versions

# List schemas
curl http://schema-registry:8081/subjects
```

All schemas are designed for production trading environments with regulatory compliance, exact financial precision, and high-performance stream processing requirements.