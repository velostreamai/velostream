use rdkafka::config::RDKafkaLogLevel;

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
