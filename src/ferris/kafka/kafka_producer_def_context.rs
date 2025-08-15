use crate::ferris::kafka::convert_kafka_log_level;
use log::error;
use rdkafka::config::RDKafkaLogLevel;
use rdkafka::error::KafkaError;
use rdkafka::message::DeliveryResult;
use rdkafka::producer::{NoCustomPartitioner, ProducerContext};
use rdkafka::{ClientContext, Message};

/// Custom context for tracking producer state and errors
#[derive(Debug, Clone)]
pub struct LastDelivery {
    pub status: Result<(), KafkaError>,
    pub partition: i32,
    pub offset: i64,
}
pub struct LoggingProducerContext {
    pub last_delivery: std::sync::Mutex<Option<LastDelivery>>,
}

impl LoggingProducerContext {
    pub fn last_delivery(&self) -> Option<LastDelivery> {
        self.last_delivery.lock().unwrap().clone()
    }
}

impl Default for LoggingProducerContext {
    fn default() -> Self {
        LoggingProducerContext {
            last_delivery: std::sync::Mutex::new(None),
        }
    }
}

impl LoggingProducerContext {
    pub fn new() -> Self {
        Self::default()
    }
}

impl ProducerContext for LoggingProducerContext {
    type DeliveryOpaque = ();

    fn delivery(
        &self,
        _delivery_result: &DeliveryResult<'_>,
        _delivery_opaque: Self::DeliveryOpaque,
    ) {
        let info = match _delivery_result {
            Ok(delivery) => LastDelivery {
                status: Ok(()),
                partition: delivery.partition(),
                offset: delivery.offset(),
            },
            Err((err, _msg)) => LastDelivery {
                status: Err(err.clone()),
                partition: -1,
                offset: -1,
            },
        };
        let mut last = self.last_delivery.lock().unwrap();
        *last = Some(info);
    }

    fn get_custom_partitioner(&self) -> Option<&NoCustomPartitioner> {
        todo!()
    }
}

impl ClientContext for LoggingProducerContext {
    // This method is called by rdkafka to provide internal log messages.
    fn log(&self, level: RDKafkaLogLevel, fac: &str, message: &str) {
        // Use the `log::log!` macro to emit the log message with the determined level.
        // The `fac` (facility) string often indicates the source within librdkafka (e.g., "BROKER", "TOPIC").
        log::log!(
            convert_kafka_log_level(level),
            "Kafka log ({}): {}",
            fac,
            message
        );
    }

    // This method is called by rdkafka when a global error occurs.
    fn error(&self, error: KafkaError, reason: &str) {
        // Use the 'error!' macro from the `log` crate for consistency.
        // It automatically uses the 'error' log level.
        error!("Kafka client error: {:?}, reason: {}", error, reason);
        // You can add custom logic here, e.g., incrementing error metrics,
        // sending alerts, or attempting recovery.
    }
}
