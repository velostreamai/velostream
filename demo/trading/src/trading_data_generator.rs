//! # Financial Trading Data Generator
//! 
//! Generates realistic financial trading data for the VeloStream SQL demo.
//! Produces market data, trading positions, and order book updates.

use chrono::{Utc, Duration as ChronoDuration};
use velostream::velostream::kafka::{Headers, JsonSerializer, KafkaProducer};
use log::{info, warn};
use rand::{thread_rng, Rng};
use rand::rngs::ThreadRng;
use rand::seq::IteratorRandom;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::time::Duration;
use tokio::time;

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
    timestamp: i64,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
struct TradingPosition {
    trader_id: String,
    symbol: String,
    position_size: i64,  // positive for long, negative for short
    avg_price: f64,
    current_pnl: f64,
    timestamp: i64,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
struct OrderBookUpdate {
    symbol: String,
    side: String,  // "BUY" or "SELL"
    price: f64,
    quantity: i64,
    order_type: String,  // "LIMIT", "MARKET"
    timestamp: i64,
}

#[derive(Clone)]
struct StockState {
    symbol: String,
    current_price: f64,
    volatility: f64,
    trend: f64,  // -1.0 to 1.0, negative for downtrend
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
        
        let price_change = self.current_price * (
            drift * dt + 
            self.volatility * (dt.sqrt()) * random_walk
        );
        
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

        MarketData {
            symbol: self.symbol.clone(),
            exchange: exchange.to_string(),
            price,
            bid_price,
            ask_price,
            bid_size,
            ask_size,
            volume,
            timestamp: Utc::now().timestamp_millis(),
        }
    }
}

struct TradingSimulator {
    stocks: HashMap<String, StockState>,
    traders: Vec<String>,
    producer: KafkaProducer<String, serde_json::Value, JsonSerializer, JsonSerializer>,
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
            stocks.insert(symbol.to_string(), StockState::new(symbol, price, volatility));
        }

        // Create traders
        let traders = (1..=20)
            .map(|i| format!("TRADER_{:03}", i))
            .collect();

        let producer = KafkaProducer::<String, serde_json::Value, _, _>::new(
            brokers,
            "market-data", // We'll change topics when sending
            JsonSerializer,
            JsonSerializer,
        )?;

        Ok(Self {
            stocks,
            traders,
            producer,
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

                // Send to market_data topic
                match self.producer.send_to_topic(
                    "market_data",
                    Some(&market_data.symbol),
                    &serde_json::to_value(&market_data)?,
                    headers,
                    None,
                ).await {
                    Ok(_delivery) => {},
                    Err(e) => warn!("Failed to send market data: {}", e),
                }
            }
        }
        Ok(())
    }

    async fn generate_trading_positions(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        for trader in &self.traders {
            // Each trader has positions in 1-4 stocks
            let num_positions = self.rng.gen_range(1..=4);
            let symbols: Vec<String> = self.stocks.keys()
                .choose_multiple(&mut self.rng, num_positions)
                .into_iter()
                .cloned()
                .collect();

            for symbol in symbols {
                if let Some(stock) = self.stocks.get(&symbol) {
                    let position_size = self.rng.gen_range(-10000..10000);
                    if position_size == 0 { continue; }

                    let avg_price = stock.current_price * self.rng.gen_range(0.95..1.05);
                    let current_pnl = position_size as f64 * (stock.current_price - avg_price);

                    let position = TradingPosition {
                        trader_id: trader.clone(),
                        symbol: symbol.clone(),
                        position_size,
                        avg_price,
                        current_pnl,
                        timestamp: Utc::now().timestamp_millis(),
                    };

                    let headers = Headers::new()
                        .insert("source", "trading_simulator")
                        .insert("data_type", "trading_position")
                        .insert("trader_id", trader)
                        .insert("symbol", &symbol);

                    match self.producer.send_to_topic(
                        "trading_positions",
                        Some(&format!("{}_{}", trader, symbol)),
                        &serde_json::to_value(&position)?,
                        headers,
                        None,
                    ).await {
                        Ok(_delivery) => {},
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
                    let side = if self.rng.gen_bool(0.5) { "BUY" } else { "SELL" };
                    let price_offset = self.rng.gen_range(-0.01..0.01);
                    let price = stock.current_price * (1.0 + price_offset);
                    let quantity = self.rng.gen_range(100..50000);
                    let order_type = if self.rng.gen_bool(0.8) { "LIMIT" } else { "MARKET" };

                    let order_update = OrderBookUpdate {
                        symbol: symbol.clone(),
                        side: side.to_string(),
                        price,
                        quantity,
                        order_type: order_type.to_string(),
                        timestamp: Utc::now().timestamp_millis(),
                    };

                    let headers = Headers::new()
                        .insert("source", "order_book_simulator")
                        .insert("data_type", "order_book_update")
                        .insert("symbol", symbol)
                        .insert("side", side);

                    match self.producer.send_to_topic(
                        "order_book_updates",
                        Some(&format!("{}_{}", symbol, side)),
                        &serde_json::to_value(&order_update)?,
                        headers,
                        None,
                    ).await {
                        Ok(_delivery) => {},
                        Err(e) => warn!("Failed to send order book update: {}", e),
                    }
                }
            }
        }
        Ok(())
    }

    async fn run_simulation(&mut self, duration_minutes: u64) -> Result<(), Box<dyn std::error::Error>> {
        info!("Starting financial trading simulation for {} minutes", duration_minutes);
        info!("Generating data for symbols: {:?}", self.stocks.keys().collect::<Vec<_>>());
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

        info!("Trading simulation completed after {} iterations", iteration);
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
    ) -> impl std::future::Future<Output = Result<rdkafka::producer::future_producer::Delivery, velostream::velostream::kafka::ProducerError>>;
}

impl<K, V, KS, VS> ProducerTopicExt<K, V, KS, VS> for KafkaProducer<K, V, KS, VS>
where
    K: Clone,
    V: Clone,
    KS: velostream::velostream::kafka::Serializer<K>,
    VS: velostream::velostream::kafka::Serializer<V>,
{
    async fn send_to_topic(
        &self,
        _topic: &str,
        key: Option<&K>,
        value: &V,
        headers: Headers,
        timestamp: Option<i64>,
    ) -> Result<rdkafka::producer::future_producer::Delivery, velostream::velostream::kafka::ProducerError> {
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
    let duration = args.get(2)
        .and_then(|s| s.parse().ok())
        .unwrap_or(60); // Default 60 minutes

    info!("Financial Trading Data Generator");
    info!("Kafka brokers: {}", brokers);
    info!("Simulation duration: {} minutes", duration);

    // Create and run the trading simulator
    let mut simulator = TradingSimulator::new(&brokers).await?;
    simulator.run_simulation(duration).await?;

    Ok(())
}