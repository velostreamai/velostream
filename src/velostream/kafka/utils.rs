use rdkafka::config::RDKafkaLogLevel;
use rdkafka::producer::{BaseProducer, DefaultProducerContext, Producer};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;
use std::time::Duration;

pub fn convert_kafka_log_level(kafka_level: RDKafkaLogLevel) -> log::Level {
    match kafka_level {
        RDKafkaLogLevel::Emerg | RDKafkaLogLevel::Alert | RDKafkaLogLevel::Critical => {
            log::Level::Error
        }
        RDKafkaLogLevel::Error => log::Level::Error,
        RDKafkaLogLevel::Warning => log::Level::Warn,
        RDKafkaLogLevel::Notice | RDKafkaLogLevel::Info => log::Level::Info,
        RDKafkaLogLevel::Debug => log::Level::Debug,
    }
}

/// Adaptive backoff thresholds for poll thread idle detection.
const IDLE_TIGHT_LOOP_THRESHOLD: u32 = 100;
const IDLE_SHORT_SLEEP_THRESHOLD: u32 = 1000;
const SHORT_SLEEP_MS: u64 = 1;
const LONG_SLEEP_MS: u64 = 10;

/// Run an adaptive poll loop for a Kafka producer.
///
/// Tight-loops when messages are in flight, backs off to 1ms then 10ms sleep
/// when idle. Drains remaining callbacks on exit.
pub fn adaptive_poll_loop(
    producer: Arc<BaseProducer<DefaultProducerContext>>,
    poll_stop: Arc<AtomicBool>,
    label: &str,
) {
    log::debug!("{}: Poll thread started", label);
    let mut idle_streak: u32 = 0;
    while !poll_stop.load(Ordering::Relaxed) {
        producer.poll(Duration::from_millis(0));

        if producer.in_flight_count() == 0 {
            idle_streak = idle_streak.saturating_add(1);
            let sleep_ms = match idle_streak {
                0..=IDLE_TIGHT_LOOP_THRESHOLD => 0,
                ..=IDLE_SHORT_SLEEP_THRESHOLD => SHORT_SLEEP_MS,
                _ => LONG_SLEEP_MS,
            };
            if sleep_ms > 0 {
                thread::sleep(Duration::from_millis(sleep_ms));
            }
        } else {
            idle_streak = 0;
        }
    }
    // Drain remaining callbacks before exiting
    producer.poll(Duration::from_millis(100));
    log::debug!("{}: Poll thread stopped", label);
}
