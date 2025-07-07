// tests/ferris/kafka/test_utils.rs
use std::net::TcpStream;

/// Helper functions
pub(crate) fn is_kafka_running() -> bool {
    match TcpStream::connect("localhost:9092") {
        Ok(_) => true,
        Err(_) => {
            println!("WARNING: Kafka is not running at localhost:9092");
            println!("Start Kafka with: docker-compose up -d");
            println!("Wait for it to start with: ./test-kafka.sh");
            println!("Tests requiring Kafka will be skipped.");
            false
        }
    }
}
pub(crate) fn init_logger() {
    let _ = env_logger::builder().is_test(true).try_init();
}

pub(crate) fn init() -> bool {
    init_logger();
    // Check if Kafka is running and return the status
    is_kafka_running()
}
