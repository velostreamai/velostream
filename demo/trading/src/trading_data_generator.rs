//! # Financial Trading Data Generator
//!
//! Generates realistic financial trading data for the Velostream SQL demo.
//! Produces market data, trading positions, and order book updates.

use chrono::{Duration as ChronoDuration, Utc};
use log::{info, warn};
use rand::rngs::ThreadRng;
use rand::seq::IteratorRandom;
use rand::{thread_rng, Rng};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::time::Duration;
use tokio::time;
use velostream::velostream::kafka::kafka_error::ProducerError;
use velostream::velostream::kafka::serialization::{Serde, StringSerializer};
use velostream::velostream::kafka::{Headers, KafkaProducer};
use velostream::velostream::serialization::avro_codec::AvroCodec;
use velostream::velostream::sql::execution::types::FieldValue;

#[derive(Serialize, Deserialize, Debug, Clone)]
struct MarketData {
    symbol: String,
    exchange: String,
    price: f64,
    bid_price: f64,
    ask_price: f64,
    bid_size: i64,
    ask_size: i64,
    volume: i64,
    vwap: Option<f64>,
    market_cap: Option<f64>,
    timestamp: i64,
}

impl MarketData {
    fn to_field_value_record(&self) -> HashMap<String, FieldValue> {
        let mut record = HashMap::new();
        record.insert(
            "symbol".to_string(),
            FieldValue::String(self.symbol.clone()),
        );
        record.insert(
            "exchange".to_string(),
            FieldValue::String(self.exchange.clone()),
        );
        record.insert("timestamp".to_string(), FieldValue::Integer(self.timestamp));
        record.insert("price".to_string(), FieldValue::Float(self.price));
        record.insert("bid_price".to_string(), FieldValue::Float(self.bid_price));
        record.insert("ask_price".to_string(), FieldValue::Float(self.ask_price));
        record.insert("bid_size".to_string(), FieldValue::Integer(self.bid_size));
        record.insert("ask_size".to_string(), FieldValue::Integer(self.ask_size));
        record.insert("volume".to_string(), FieldValue::Integer(self.volume));
        record.insert(
            "vwap".to_string(),
            self.vwap.map(FieldValue::Float).unwrap_or(FieldValue::Null),
        );
        record.insert(
            "market_cap".to_string(),
            self.market_cap
                .map(FieldValue::Float)
                .unwrap_or(FieldValue::Null),
        );
        record
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
struct TradingPosition {
    trader_id: String,
    account_id: String,
    symbol: String,
    position_size: i64, // positive for long, negative for short
    entry_price: f64,
    current_pnl: f64,
    unrealized_pnl: f64,
    realized_pnl: f64,
    position_value: f64,
    margin_used: f64,
    timestamp: i64,
    last_update_source: String,
}

impl TradingPosition {
    fn to_field_value_record(&self) -> HashMap<String, FieldValue> {
        let mut record = HashMap::new();
        record.insert(
            "trader_id".to_string(),
            FieldValue::String(self.trader_id.clone()),
        );
        record.insert(
            "account_id".to_string(),
            FieldValue::String(self.account_id.clone()),
        );
        record.insert(
            "symbol".to_string(),
            FieldValue::String(self.symbol.clone()),
        );
        record.insert(
            "position_size".to_string(),
            FieldValue::Integer(self.position_size),
        );
        record.insert(
            "entry_price".to_string(),
            FieldValue::String(format!("{:.4}", self.entry_price)),
        );
        record.insert(
            "current_pnl".to_string(),
            FieldValue::String(format!("{:.2}", self.current_pnl)),
        );
        record.insert(
            "unrealized_pnl".to_string(),
            FieldValue::String(format!("{:.2}", self.unrealized_pnl)),
        );
        record.insert(
            "realized_pnl".to_string(),
            FieldValue::String(format!("{:.2}", self.realized_pnl)),
        );
        record.insert(
            "position_value".to_string(),
            FieldValue::String(format!("{:.2}", self.position_value)),
        );
        record.insert(
            "margin_used".to_string(),
            FieldValue::String(format!("{:.2}", self.margin_used)),
        );
        record.insert("timestamp".to_string(), FieldValue::Integer(self.timestamp));
        record.insert(
            "last_update_source".to_string(),
            FieldValue::String(self.last_update_source.clone()),
        );
        record
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
struct OrderBookUpdate {
    symbol: String,
    exchange: String,
    timestamp: i64,
    side: String, // "BUY" or "SELL"
    price: f64,
    quantity: i64,
    order_count: i32,
    update_type: String, // "ADD", "MODIFY", "DELETE", "TRADE"
    level: i32,
    total_volume: i64,
    sequence_number: i64,
}

impl OrderBookUpdate {
    fn to_field_value_record(&self) -> HashMap<String, FieldValue> {
        let mut record = HashMap::new();
        record.insert(
            "symbol".to_string(),
            FieldValue::String(self.symbol.clone()),
        );
        record.insert(
            "exchange".to_string(),
            FieldValue::String(self.exchange.clone()),
        );
        record.insert("timestamp".to_string(), FieldValue::Integer(self.timestamp));
        record.insert("side".to_string(), FieldValue::String(self.side.clone()));
        record.insert("price".to_string(), FieldValue::Float(self.price));
        record.insert("quantity".to_string(), FieldValue::Integer(self.quantity));
        record.insert(
            "order_count".to_string(),
            FieldValue::Integer(self.order_count as i64),
        );
        record.insert(
            "update_type".to_string(),
            FieldValue::String(self.update_type.clone()),
        );
        record.insert("level".to_string(), FieldValue::Integer(self.level as i64));
        record.insert(
            "total_volume".to_string(),
            FieldValue::Integer(self.total_volume),
        );
        record.insert(
            "sequence_number".to_string(),
            FieldValue::Integer(self.sequence_number),
        );
        record
    }
}

#[derive(Clone)]
struct StockState {
    symbol: String,
    current_price: f64,
    volatility: f64,
    trend: f64, // -1.0 to 1.0, negative for downtrend
    volume_base: i64,
    exchanges: Vec<String>,
}

impl StockState {
    fn new(symbol: &str, initial_price: f64, volatility: f64) -> Self {
        Self {
            symbol: symbol.to_string(),
            current_price: initial_price,
            volatility,
            trend: 0.0,
            volume_base: thread_rng().gen_range(50000..500000),
            exchanges: vec!["NYSE".to_string(), "NASDAQ".to_string(), "BATS".to_string()],
        }
    }

    fn update_price(&mut self, rng: &mut ThreadRng) -> f64 {
        // Geometric Brownian Motion with trend
        let dt = 1.0 / (24.0 * 60.0 * 60.0); // 1 second intervals
        let drift = self.trend * 0.1; // Annual drift
        let random_walk = rng.gen_range(-1.0..1.0);

        let price_change =
            self.current_price * (drift * dt + self.volatility * (dt.sqrt()) * random_walk);

        self.current_price = (self.current_price + price_change).max(0.01);

        // Occasionally adjust trend
        if rng.gen_bool(0.01) {
            self.trend = rng.gen_range(-0.5..0.5);
        }

        self.current_price
    }

    fn generate_market_data(&mut self, rng: &mut ThreadRng, exchange: &str) -> MarketData {
        let price = self.update_price(rng);
        let spread_bps = rng.gen_range(1..20) as f64; // 1-20 basis points
        let spread = price * spread_bps / 10000.0;

        let bid_price = price - spread / 2.0;
        let ask_price = price + spread / 2.0;

        // Volume with occasional spikes
        let volume_multiplier = if rng.gen_bool(0.05) {
            rng.gen_range(3.0..10.0) // Volume spike
        } else {
            rng.gen_range(0.5..2.0)
        };

        let volume = (self.volume_base as f64 * volume_multiplier) as i64;
        let bid_size = rng.gen_range(1000..100000);
        let ask_size = rng.gen_range(1000..100000);

        // Calculate VWAP (simple approximation for demo purposes)
        let vwap = Some(price * rng.gen_range(0.995..1.005));

        // Calculate market cap (simplified - price * total shares outstanding)
        // For demo, use realistic market caps based on symbol
        let market_cap = match self.symbol.as_str() {
            "AAPL" | "MSFT" | "GOOGL" => Some(price * 15_000_000_000.0), // ~$2-3T
            "AMZN" | "NVDA" | "META" => Some(price * 5_000_000_000.0),   // ~$1-2T
            "TSLA" | "NFLX" => Some(price * 3_000_000_000.0),            // ~$500B-1T
            _ => Some(price * 1_000_000_000.0),
        };

        MarketData {
            symbol: self.symbol.clone(),
            exchange: exchange.to_string(),
            price,
            bid_price,
            ask_price,
            bid_size,
            ask_size,
            volume,
            vwap,
            market_cap,
            timestamp: Utc::now().timestamp_millis(),
        }
    }
}

struct TradingSimulator {
    stocks: HashMap<String, StockState>,
    traders: Vec<String>,
    market_data_producer:
        KafkaProducer<String, HashMap<String, FieldValue>, StringSerializer, AvroCodec>,
    trading_position_producer:
        KafkaProducer<String, HashMap<String, FieldValue>, StringSerializer, AvroCodec>,
    order_book_producer:
        KafkaProducer<String, HashMap<String, FieldValue>, StringSerializer, AvroCodec>,
    rng: ThreadRng,
}

impl TradingSimulator {
    async fn new(brokers: &str) -> Result<Self, Box<dyn std::error::Error>> {
        // Initialize major tech stocks
        let mut stocks = HashMap::new();
        let stock_configs = vec![
            ("AAPL", 175.0, 0.25),
            ("GOOGL", 140.0, 0.30),
            ("MSFT", 350.0, 0.25),
            ("AMZN", 140.0, 0.35),
            ("TSLA", 250.0, 0.50),
            ("NVDA", 450.0, 0.40),
            ("META", 320.0, 0.35),
            ("NFLX", 450.0, 0.40),
        ];

        for (symbol, price, volatility) in stock_configs {
            stocks.insert(
                symbol.to_string(),
                StockState::new(symbol, price, volatility),
            );
        }

        // Create traders
        let traders = (1..=20).map(|i| format!("TRADER_{:03}", i)).collect();

        // Load Avro schemas from files
        info!("Loading Avro schemas...");
        let market_data_schema = std::fs::read_to_string("schemas/market_data.avsc")?;
        let trading_position_schema = std::fs::read_to_string("schemas/trading_positions.avsc")?;
        let order_book_schema = std::fs::read_to_string("schemas/order_book_updates.avsc")?;

        // Create AvroCodec instances for each producer
        let market_data_codec = AvroCodec::new(&market_data_schema)?;
        let trading_position_codec = AvroCodec::new(&trading_position_schema)?;
        let order_book_codec = AvroCodec::new(&order_book_schema)?;
        info!("Avro schemas loaded successfully");

        // Create separate producers for each message type with correct codecs
        let market_data_producer = KafkaProducer::<String, HashMap<String, FieldValue>, _, _>::new(
            brokers,
            "in_market_data_stream",
            StringSerializer,
            market_data_codec,
        )?;

        let trading_position_producer =
            KafkaProducer::<String, HashMap<String, FieldValue>, _, _>::new(
                brokers,
                "in_trading_positions_stream",
                StringSerializer,
                trading_position_codec,
            )?;

        let order_book_producer = KafkaProducer::<String, HashMap<String, FieldValue>, _, _>::new(
            brokers,
            "in_order_book_stream",
            StringSerializer,
            order_book_codec,
        )?;

        Ok(Self {
            stocks,
            traders,
            market_data_producer,
            trading_position_producer,
            order_book_producer,
            rng: thread_rng(),
        })
    }

    async fn generate_market_data(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        for stock in self.stocks.values_mut() {
            for exchange in stock.exchanges.clone() {
                let market_data = stock.generate_market_data(&mut self.rng, &exchange);

                let headers = Headers::new()
                    .insert("source", "market_data_generator")
                    .insert("data_type", "market_data")
                    .insert("exchange", &exchange)
                    .insert("symbol", &market_data.symbol);

                // Convert to FieldValue record for Avro serialization
                let record = market_data.to_field_value_record();

                // Send to in_market_data_stream topic using market_data_producer
                match self
                    .market_data_producer
                    .send(Some(&market_data.symbol), &record, headers.clone(), None)
                    .await
                {
                    Ok(_delivery) => {}
                    Err(e) => warn!("Failed to send market data: {}", e),
                }

                // Also send to exchange-specific topics for arbitrage detection
                // Note: These will also use the market_data schema
                let exchange_topic = match exchange.as_str() {
                    "NYSE" => "in_market_data_stream_a",
                    "NASDAQ" => "in_market_data_stream_b",
                    _ => continue, // Skip other exchanges for now
                };

                // For exchange-specific topics, we need to send to the same producer
                // since they use the same schema. The producer is configured with the default topic
                // but we'll just send to it again (all market data uses the same topic for now)
                match self
                    .market_data_producer
                    .send(Some(&market_data.symbol), &record, headers, None)
                    .await
                {
                    Ok(_delivery) => {}
                    Err(e) => warn!("Failed to send market data to {}: {}", exchange_topic, e),
                }
            }
        }
        Ok(())
    }

    async fn generate_trading_positions(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        for trader in &self.traders {
            // Each trader has positions in 1-4 stocks
            let num_positions = self.rng.gen_range(1..=4);
            let symbols: Vec<String> = self
                .stocks
                .keys()
                .choose_multiple(&mut self.rng, num_positions)
                .into_iter()
                .cloned()
                .collect();

            for symbol in symbols {
                if let Some(stock) = self.stocks.get(&symbol) {
                    let position_size = self.rng.gen_range(-10000..10000);
                    if position_size == 0 {
                        continue;
                    }

                    let entry_price = stock.current_price * self.rng.gen_range(0.95..1.05);
                    let unrealized_pnl = position_size as f64 * (stock.current_price - entry_price);
                    let realized_pnl = self.rng.gen_range(-1000.0..1000.0); // Some historical realized P&L
                    let current_pnl = unrealized_pnl + realized_pnl;
                    let position_value = (position_size as f64 * stock.current_price).abs();
                    let margin_used = position_value * 0.3; // 30% margin requirement
                    let account_id = format!("ACC_{}", trader.chars().last().unwrap_or('0'));

                    let position = TradingPosition {
                        trader_id: trader.clone(),
                        account_id,
                        symbol: symbol.clone(),
                        position_size,
                        entry_price,
                        current_pnl,
                        unrealized_pnl,
                        realized_pnl,
                        position_value,
                        margin_used,
                        timestamp: Utc::now().timestamp_millis(),
                        last_update_source: "position_update".to_string(),
                    };

                    let headers = Headers::new()
                        .insert("source", "trading_simulator")
                        .insert("data_type", "trading_position")
                        .insert("trader_id", trader)
                        .insert("symbol", &symbol);

                    // Convert to FieldValue record for Avro serialization
                    let record = position.to_field_value_record();

                    match self
                        .trading_position_producer
                        .send(
                            Some(&format!("{}_{}", trader, symbol)),
                            &record,
                            headers,
                            None,
                        )
                        .await
                    {
                        Ok(_delivery) => {}
                        Err(e) => warn!("Failed to send trading position: {}", e),
                    }
                }
            }
        }
        Ok(())
    }

    async fn generate_order_book_updates(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        for symbol in self.stocks.keys() {
            // Generate 3-10 order book updates per symbol
            let num_updates = self.rng.gen_range(3..=10);

            for _ in 0..num_updates {
                if let Some(stock) = self.stocks.get(symbol) {
                    let side = if self.rng.gen_bool(0.5) {
                        "BUY"
                    } else {
                        "SELL"
                    };
                    let exchange =
                        stock.exchanges[self.rng.gen_range(0..stock.exchanges.len())].clone();
                    let price_offset = self.rng.gen_range(-0.01..0.01);
                    let price = stock.current_price * (1.0 + price_offset);
                    let quantity = self.rng.gen_range(100..50000);
                    let order_count = self.rng.gen_range(1..10);
                    let level = self.rng.gen_range(1..5);
                    let total_volume = quantity + self.rng.gen_range(0..quantity);

                    let update_type = match self.rng.gen_range(0..4) {
                        0 => "ADD",
                        1 => "MODIFY",
                        2 => "DELETE",
                        _ => "TRADE",
                    };

                    // Use iteration as sequence number (simplified)
                    static SEQUENCE: std::sync::atomic::AtomicI64 =
                        std::sync::atomic::AtomicI64::new(0);
                    let sequence_number =
                        SEQUENCE.fetch_add(1, std::sync::atomic::Ordering::SeqCst);

                    let order_update = OrderBookUpdate {
                        symbol: symbol.clone(),
                        exchange,
                        timestamp: Utc::now().timestamp_millis(),
                        side: side.to_string(),
                        price,
                        quantity,
                        order_count,
                        update_type: update_type.to_string(),
                        level,
                        total_volume,
                        sequence_number,
                    };

                    let headers = Headers::new()
                        .insert("source", "order_book_simulator")
                        .insert("data_type", "order_book_update")
                        .insert("symbol", symbol)
                        .insert("side", side);

                    // Convert to FieldValue record for Avro serialization
                    let record = order_update.to_field_value_record();

                    match self
                        .order_book_producer
                        .send(
                            Some(&format!("{}_{}", symbol, side)),
                            &record,
                            headers,
                            None,
                        )
                        .await
                    {
                        Ok(_delivery) => {}
                        Err(e) => warn!("Failed to send order book update: {}", e),
                    }
                }
            }
        }
        Ok(())
    }

    async fn run_simulation(
        &mut self,
        duration_minutes: u64,
    ) -> Result<(), Box<dyn std::error::Error>> {
        info!(
            "Starting financial trading simulation for {} minutes",
            duration_minutes
        );
        info!(
            "Generating data for symbols: {:?}",
            self.stocks.keys().collect::<Vec<_>>()
        );
        info!("Simulating {} traders", self.traders.len());

        let end_time = Utc::now() + ChronoDuration::minutes(duration_minutes as i64);
        let mut iteration = 0;

        while Utc::now() < end_time {
            iteration += 1;

            // Generate market data every iteration (every second)
            self.generate_market_data().await?;

            // Generate trading positions every 5 seconds
            if iteration % 5 == 0 {
                self.generate_trading_positions().await?;
            }

            // Generate order book updates every 2 seconds
            if iteration % 2 == 0 {
                self.generate_order_book_updates().await?;
            }

            if iteration % 30 == 0 {
                info!("Generated {} iterations of trading data", iteration);
            }

            time::sleep(Duration::from_secs(1)).await;
        }

        info!(
            "Trading simulation completed after {} iterations",
            iteration
        );
        Ok(())
    }
}

// Extension trait for sending to different topics
trait ProducerTopicExt<K, V, KS, VS> {
    fn send_to_topic(
        &self,
        topic: &str,
        key: Option<&K>,
        value: &V,
        headers: Headers,
        timestamp: Option<i64>,
    ) -> impl std::future::Future<
        Output = Result<rdkafka::producer::future_producer::Delivery, ProducerError>,
    >;
}

impl<K, V, KS, VS> ProducerTopicExt<K, V, KS, VS> for KafkaProducer<K, V, KS, VS>
where
    K: Clone,
    V: Clone,
    KS: Serde<K>,
    VS: Serde<V>,
{
    async fn send_to_topic(
        &self,
        _topic: &str,
        key: Option<&K>,
        value: &V,
        headers: Headers,
        timestamp: Option<i64>,
    ) -> Result<rdkafka::producer::future_producer::Delivery, ProducerError> {
        // For now, use the main producer's send method (this will go to the default topic)
        // In a real implementation, you'd want topic-specific producers
        self.send(key, value, headers, timestamp).await
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize logging
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();

    // Parse command line arguments
    let args: Vec<String> = std::env::args().collect();
    let brokers = args.get(1).unwrap_or(&"localhost:9092".to_string()).clone();
    let duration = args.get(2).and_then(|s| s.parse().ok()).unwrap_or(60); // Default 60 minutes

    info!("Financial Trading Data Generator");
    info!("Kafka brokers: {}", brokers);
    info!("Simulation duration: {} minutes", duration);

    // Create and run the trading simulator
    let mut simulator = TradingSimulator::new(&brokers).await?;
    simulator.run_simulation(duration).await?;

    Ok(())
}
