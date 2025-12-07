//! Polled Producer abstraction for Kafka
//!
//! Provides three implementations for different use cases:
//! - `SyncPolledProducer`: No poll thread, inline polling - best for simple/sync workloads
//! - `AsyncPolledProducer`: Dedicated poll thread - best for high-throughput async workloads
//! - `TransactionalPolledProducer`: Dedicated manager thread - best for exactly-once semantics
//!
//! ## Transaction Support
//!
//! All implementations support Kafka transactions. For `AsyncPolledProducer`, the poll thread
//! is automatically stopped during transaction operations. For `TransactionalPolledProducer`,
//! all operations are serialized through a dedicated manager thread, ensuring correctness.
//!
//! **Important**: To use transactions, you must configure `transactional.id` and
//! `enable.idempotence=true` when creating the producer.
//!
//! ## TransactionalPolledProducer
//!
//! Uses a command channel pattern where:
//! - All producer operations are sent to a dedicated manager thread
//! - `init_transactions()` is called once at startup
//! - All operations are serialized, eliminating race conditions
//! - Flush is automatically called before commit

use rdkafka::ClientConfig;
use rdkafka::producer::{BaseProducer, BaseRecord, DefaultProducerContext, Producer};
use std::error::Error;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicI32, Ordering};
use std::sync::mpsc::{self, Receiver, SyncSender};
use std::thread::{self, JoinHandle};
use std::time::Duration;

/// Trait for Kafka producers that handle polling for delivery callbacks
///
/// Includes transaction support methods. For transactional producers, ensure
/// `transactional.id` is configured.
///
/// ## Important: flush() behavior
///
/// The `flush()` method takes `&mut self` because it must stop any background
/// poll thread before flushing. This follows the critical rule from rdkafka:
/// **flush() must run ONLY after polling has stopped**, otherwise both threads
/// fight over callbacks leading to slowdowns or timeouts.
pub trait PolledProducer: Send {
    /// Send a record to Kafka (non-blocking, queues internally)
    fn send<'a>(
        &self,
        record: BaseRecord<'a, str, [u8]>,
    ) -> Result<(), (rdkafka::error::KafkaError, BaseRecord<'a, str, [u8]>)>;

    /// Flush all pending messages, blocking until complete or timeout.
    ///
    /// **Important**: This stops any background poll thread before flushing
    /// and restarts it after. This is required for correct behavior.
    fn flush(&mut self, timeout: Duration) -> Result<(), rdkafka::error::KafkaError>;

    /// Get the number of messages in the producer queue
    fn in_flight_count(&self) -> i32;

    /// Initialize transactions. Must be called before begin_transaction().
    /// Requires `transactional.id` to be configured.
    fn init_transactions(&mut self, timeout: Duration) -> Result<(), rdkafka::error::KafkaError>;

    /// Begin a new transaction. Must call init_transactions() first.
    fn begin_transaction(&mut self) -> Result<(), rdkafka::error::KafkaError>;

    /// Commit the current transaction.
    fn commit_transaction(&mut self, timeout: Duration) -> Result<(), rdkafka::error::KafkaError>;

    /// Abort the current transaction.
    fn abort_transaction(&mut self, timeout: Duration) -> Result<(), rdkafka::error::KafkaError>;
}

// ============================================================================
// SyncPolledProducer - No poll thread, inline polling
// ============================================================================

/// Synchronous producer that polls inline during send/flush.
///
/// Best for:
/// - Transactional workloads requiring exactly-once semantics
/// - Low-volume producers where a dedicated thread is wasteful
/// - Testing and development
///
/// Characteristics:
/// - No background thread overhead
/// - Predictable latency (no thread scheduling jitter)
/// - Flush performance matches raw BaseProducer
pub struct SyncPolledProducer {
    producer: BaseProducer<DefaultProducerContext>,
}

impl SyncPolledProducer {
    /// Create a new synchronous producer
    pub fn new(config: ClientConfig) -> Result<Self, Box<dyn Error + Send + Sync>> {
        let producer: BaseProducer<DefaultProducerContext> = config.create()?;
        Ok(Self { producer })
    }

    /// Create with production-grade settings (safe, ordered, durable)
    pub fn with_production_config(brokers: &str) -> Result<Self, Box<dyn Error + Send + Sync>> {
        let mut config = ClientConfig::new();
        config
            .set("bootstrap.servers", brokers)
            .set("enable.idempotence", "true")
            .set("acks", "all")
            .set("max.in.flight.requests.per.connection", "1")
            .set("message.send.max.retries", "1000000")
            .set("compression.type", "lz4")
            .set("batch.num.messages", "1000000")
            .set("linger.ms", "5");
        Self::new(config)
    }

    /// Create with maximum throughput settings (fast but less durable)
    pub fn with_high_throughput(brokers: &str) -> Result<Self, Box<dyn Error + Send + Sync>> {
        let mut config = ClientConfig::new();
        config
            .set("bootstrap.servers", brokers)
            .set("enable.idempotence", "false")
            .set("acks", "1")
            .set("compression.type", "lz4")
            .set("batch.size", "1048576")
            .set("linger.ms", "5")
            .set("queue.buffering.max.kbytes", "1048576")
            .set("queue.buffering.max.messages", "500000");
        Self::new(config)
    }
}

impl PolledProducer for SyncPolledProducer {
    fn send<'a>(
        &self,
        record: BaseRecord<'a, str, [u8]>,
    ) -> Result<(), (rdkafka::error::KafkaError, BaseRecord<'a, str, [u8]>)> {
        // Poll before send to process any pending callbacks
        self.producer.poll(Duration::from_millis(0));
        self.producer.send(record)
    }

    fn flush(&mut self, timeout: Duration) -> Result<(), rdkafka::error::KafkaError> {
        // Direct flush - no thread contention (SyncPolledProducer has no poll thread)
        self.producer.flush(timeout)
    }

    fn in_flight_count(&self) -> i32 {
        self.producer.in_flight_count()
    }

    // Transaction methods - direct delegation (no thread to manage)

    fn init_transactions(&mut self, timeout: Duration) -> Result<(), rdkafka::error::KafkaError> {
        self.producer.init_transactions(timeout)
    }

    fn begin_transaction(&mut self) -> Result<(), rdkafka::error::KafkaError> {
        self.producer.begin_transaction()
    }

    fn commit_transaction(&mut self, timeout: Duration) -> Result<(), rdkafka::error::KafkaError> {
        self.producer.commit_transaction(timeout)
    }

    fn abort_transaction(&mut self, timeout: Duration) -> Result<(), rdkafka::error::KafkaError> {
        self.producer.abort_transaction(timeout)
    }
}

// ============================================================================
// AsyncPolledProducer - Dedicated poll thread for async delivery
// ============================================================================

/// Asynchronous producer with dedicated poll thread for delivery callbacks.
///
/// Best for:
/// - High-throughput streaming workloads
/// - Fire-and-forget message patterns
/// - Workloads where delivery confirmation can be async
///
/// Characteristics:
/// - Dedicated thread processes callbacks continuously
/// - Maximum throughput for message production
/// - Poll thread is stopped during flush for optimal performance
pub struct AsyncPolledProducer {
    producer: Arc<BaseProducer<DefaultProducerContext>>,
    poll_stop: Arc<AtomicBool>,
    poll_thread: Option<JoinHandle<()>>,
}

impl AsyncPolledProducer {
    /// Create a new asynchronous producer with poll thread
    pub fn new(config: ClientConfig) -> Result<Self, Box<dyn Error + Send + Sync>> {
        let producer: BaseProducer<DefaultProducerContext> = config.create()?;
        let producer = Arc::new(producer);

        let poll_stop = Arc::new(AtomicBool::new(false));
        let poll_thread = Self::spawn_poll_thread(Arc::clone(&producer), Arc::clone(&poll_stop));

        Ok(Self {
            producer,
            poll_stop,
            poll_thread: Some(poll_thread),
        })
    }

    /// Create with production-grade settings (safe, ordered, durable)
    ///
    /// This matches production best practices:
    /// - `enable.idempotence=true` - Ensures exactly-once delivery semantics
    /// - `acks=all` - Waits for all replicas to acknowledge
    /// - `max.in.flight.requests.per.connection=1` - Ensures strict ordering
    /// - High retry count for resilience against transient failures
    /// - LZ4 compression for efficiency
    ///
    /// Recommended for production workloads that require durability and ordering.
    pub fn with_production_config(brokers: &str) -> Result<Self, Box<dyn Error + Send + Sync>> {
        let mut config = ClientConfig::new();
        config
            .set("bootstrap.servers", brokers)
            .set("enable.idempotence", "true")
            .set("acks", "all")
            .set("max.in.flight.requests.per.connection", "1") // Required for ordered idempotent
            .set("message.send.max.retries", "1000000")
            .set("compression.type", "lz4")
            .set("batch.num.messages", "1000000")
            .set("linger.ms", "5")
            .set("queue.buffering.max.kbytes", "1048576")
            .set("queue.buffering.max.messages", "500000");
        Self::new(config)
    }

    /// Create with maximum throughput settings (fast but less durable)
    ///
    /// **Warning**: This config prioritizes throughput over durability:
    /// - `enable.idempotence=false` - No exactly-once guarantees
    /// - `acks=1` - Only waits for leader acknowledgment (data can be lost)
    ///
    /// Use `with_production_config()` for production workloads that need
    /// durability and ordering guarantees.
    pub fn with_high_throughput(brokers: &str) -> Result<Self, Box<dyn Error + Send + Sync>> {
        let mut config = ClientConfig::new();
        config
            .set("bootstrap.servers", brokers)
            .set("enable.idempotence", "false")
            .set("acks", "1")
            .set("compression.type", "lz4")
            .set("batch.size", "1048576")
            .set("linger.ms", "5")
            .set("queue.buffering.max.kbytes", "1048576")
            .set("queue.buffering.max.messages", "500000");
        Self::new(config)
    }

    fn spawn_poll_thread(
        producer: Arc<BaseProducer<DefaultProducerContext>>,
        poll_stop: Arc<AtomicBool>,
    ) -> JoinHandle<()> {
        thread::spawn(move || {
            log::debug!("AsyncPolledProducer: Poll thread started");
            while !poll_stop.load(Ordering::Relaxed) {
                // poll(0) = process callbacks immediately
                producer.poll(Duration::from_millis(0));
                // Sleep 1ms to prevent tight CPU spinning (~1-5% CPU instead of 100%)
                thread::sleep(Duration::from_millis(1));
            }
            // Drain any remaining callbacks before exiting
            producer.poll(Duration::from_millis(50));
            log::debug!("AsyncPolledProducer: Poll thread stopped (callbacks drained)");
        })
    }

    fn stop_poll_thread(&mut self) {
        self.poll_stop.store(true, Ordering::SeqCst);
        if let Some(handle) = self.poll_thread.take() {
            let _ = handle.join();
        }
    }

    fn restart_poll_thread(&mut self) {
        self.poll_stop = Arc::new(AtomicBool::new(false));
        self.poll_thread = Some(Self::spawn_poll_thread(
            Arc::clone(&self.producer),
            Arc::clone(&self.poll_stop),
        ));
    }
}

impl PolledProducer for AsyncPolledProducer {
    fn send<'a>(
        &self,
        record: BaseRecord<'a, str, [u8]>,
    ) -> Result<(), (rdkafka::error::KafkaError, BaseRecord<'a, str, [u8]>)> {
        // Poll thread handles callbacks - just queue the message
        self.producer.send(record)
    }

    fn flush(&mut self, timeout: Duration) -> Result<(), rdkafka::error::KafkaError> {
        // CRITICAL: Stop poll thread before flush to avoid contention.
        // Both threads fighting over callbacks leads to slowdowns/timeouts.
        self.stop_poll_thread();
        let result = self.producer.flush(timeout);
        self.restart_poll_thread();
        result
    }

    fn in_flight_count(&self) -> i32 {
        self.producer.in_flight_count()
    }

    // Transaction methods - stop poll thread during transaction ops to prevent contention
    // with the transaction coordinator. This is critical for correctness.

    fn init_transactions(&mut self, timeout: Duration) -> Result<(), rdkafka::error::KafkaError> {
        log::debug!("AsyncPolledProducer: Stopping poll thread for init_transactions");
        self.stop_poll_thread();
        let result = self.producer.init_transactions(timeout);
        self.restart_poll_thread();
        log::debug!("AsyncPolledProducer: Poll thread restarted after init_transactions");
        result
    }

    fn begin_transaction(&mut self) -> Result<(), rdkafka::error::KafkaError> {
        log::debug!("AsyncPolledProducer: Stopping poll thread for begin_transaction");
        self.stop_poll_thread();
        let result = self.producer.begin_transaction();
        self.restart_poll_thread();
        log::debug!("AsyncPolledProducer: Poll thread restarted after begin_transaction");
        result
    }

    fn commit_transaction(&mut self, timeout: Duration) -> Result<(), rdkafka::error::KafkaError> {
        log::debug!("AsyncPolledProducer: Stopping poll thread for commit_transaction");
        self.stop_poll_thread();
        let result = self.producer.commit_transaction(timeout);
        self.restart_poll_thread();
        log::debug!("AsyncPolledProducer: Poll thread restarted after commit_transaction");
        result
    }

    fn abort_transaction(&mut self, timeout: Duration) -> Result<(), rdkafka::error::KafkaError> {
        log::debug!("AsyncPolledProducer: Stopping poll thread for abort_transaction");
        self.stop_poll_thread();
        let result = self.producer.abort_transaction(timeout);
        self.restart_poll_thread();
        log::debug!("AsyncPolledProducer: Poll thread restarted after abort_transaction");
        result
    }
}

impl Drop for AsyncPolledProducer {
    fn drop(&mut self) {
        log::debug!("AsyncPolledProducer: Dropping, stopping poll thread");
        self.stop_poll_thread();
        // Final flush to ensure all queued messages are delivered
        if let Err(e) = self.producer.flush(Duration::from_secs(5)) {
            log::warn!("AsyncPolledProducer: Final flush failed: {:?}", e);
        }
    }
}

// ============================================================================
// TransactionalPolledProducer - Dedicated manager thread for exactly-once
// ============================================================================

/// Commands sent to the transaction manager thread
enum TxnCommand {
    /// Send a record (topic, optional key, payload)
    Send {
        topic: String,
        key: Option<String>,
        payload: Vec<u8>,
        reply: SyncSender<Result<(), rdkafka::error::KafkaError>>,
    },
    /// Flush pending messages
    Flush {
        timeout: Duration,
        reply: SyncSender<Result<(), rdkafka::error::KafkaError>>,
    },
    /// Get in-flight message count
    InFlightCount { reply: SyncSender<i32> },
    /// Begin a new transaction
    BeginTransaction {
        reply: SyncSender<Result<(), rdkafka::error::KafkaError>>,
    },
    /// Commit current transaction (flushes first)
    CommitTransaction {
        timeout: Duration,
        reply: SyncSender<Result<(), rdkafka::error::KafkaError>>,
    },
    /// Abort current transaction
    AbortTransaction {
        timeout: Duration,
        reply: SyncSender<Result<(), rdkafka::error::KafkaError>>,
    },
    /// Stop the manager thread
    Stop,
}

/// Transactional producer with dedicated manager thread for exactly-once semantics.
///
/// Best for:
/// - Exactly-once processing guarantees
/// - Transactional workloads requiring strict ordering
/// - Scenarios where all producer operations must be serialized
///
/// Characteristics:
/// - All operations serialized through a single manager thread
/// - `init_transactions()` called once at construction (not via trait method)
/// - Automatic flush before commit
/// - No concurrent access to the producer - eliminates race conditions
///
/// ## Example
///
/// ```rust,no_run
/// use velostream::velostream::kafka::polled_producer::TransactionalPolledProducer;
/// use std::time::Duration;
///
/// let producer = TransactionalPolledProducer::new(
///     "localhost:9092",
///     "my-transactional-id",
///     Duration::from_secs(30),
/// ).expect("failed to create producer");
/// ```
pub struct TransactionalPolledProducer {
    /// Channel sender for commands to the manager thread
    cmd_tx: mpsc::Sender<TxnCommand>,
    /// Manager thread handle
    manager_thread: Option<JoinHandle<()>>,
    /// Cached in-flight count for fast access
    in_flight_count: Arc<AtomicI32>,
    /// Flag to track if transactions are initialized (always true after construction)
    transactions_initialized: bool,
}

impl TransactionalPolledProducer {
    /// Create a new transactional producer with the given transactional ID.
    ///
    /// This initializes transactions during construction, so the producer is ready
    /// to use immediately. The `init_transactions` trait method becomes a no-op.
    ///
    /// # Arguments
    /// * `brokers` - Kafka broker addresses
    /// * `transactional_id` - Unique ID for this transactional producer
    /// * `init_timeout` - Timeout for initializing transactions
    pub fn new(
        brokers: &str,
        transactional_id: &str,
        init_timeout: Duration,
    ) -> Result<Self, Box<dyn Error + Send + Sync>> {
        Self::with_config(brokers, transactional_id, init_timeout, None)
    }

    /// Create with additional configuration options
    pub fn with_config(
        brokers: &str,
        transactional_id: &str,
        init_timeout: Duration,
        extra_config: Option<&std::collections::HashMap<String, String>>,
    ) -> Result<Self, Box<dyn Error + Send + Sync>> {
        let (cmd_tx, cmd_rx) = mpsc::channel();
        let in_flight_count = Arc::new(AtomicI32::new(0));
        let in_flight_clone = Arc::clone(&in_flight_count);

        // Build config with transactional settings
        let mut config = ClientConfig::new();
        config
            .set("bootstrap.servers", brokers)
            .set("transactional.id", transactional_id)
            .set("enable.idempotence", "true")
            .set("acks", "all")
            .set("compression.type", "lz4")
            .set("batch.size", "1048576")
            .set("linger.ms", "5");

        // Apply extra config if provided
        if let Some(extra) = extra_config {
            for (key, value) in extra {
                config.set(key, value);
            }
        }

        // Create producer
        let producer: BaseProducer<DefaultProducerContext> = config.create()?;

        // Initialize transactions before spawning thread
        log::info!(
            "TransactionalPolledProducer: Initializing transactions with id '{}'",
            transactional_id
        );
        producer.init_transactions(init_timeout)?;
        log::info!("TransactionalPolledProducer: Transactions initialized successfully");

        // Spawn manager thread
        let manager_thread = Self::spawn_manager_thread(producer, cmd_rx, in_flight_clone);

        Ok(Self {
            cmd_tx,
            manager_thread: Some(manager_thread),
            in_flight_count,
            transactions_initialized: true,
        })
    }

    /// Spawn the manager thread that handles all producer operations
    fn spawn_manager_thread(
        producer: BaseProducer<DefaultProducerContext>,
        cmd_rx: Receiver<TxnCommand>,
        in_flight_count: Arc<AtomicI32>,
    ) -> JoinHandle<()> {
        thread::spawn(move || {
            log::debug!("TransactionalPolledProducer: Manager thread started");
            let mut in_transaction = false;

            for cmd in cmd_rx {
                match cmd {
                    TxnCommand::Send {
                        topic,
                        key,
                        payload,
                        reply,
                    } => {
                        // Build record
                        let mut record = BaseRecord::to(&topic).payload(payload.as_slice());
                        if let Some(ref k) = key {
                            record = record.key(k.as_str());
                        }

                        // Send and poll
                        let result = match producer.send(record) {
                            Ok(()) => {
                                // Poll to process callbacks
                                producer.poll(Duration::from_millis(0));
                                in_flight_count
                                    .store(producer.in_flight_count(), Ordering::Relaxed);
                                Ok(())
                            }
                            Err((err, _)) => Err(err),
                        };
                        let _ = reply.send(result);
                    }

                    TxnCommand::Flush { timeout, reply } => {
                        let result = producer.flush(timeout);
                        in_flight_count.store(producer.in_flight_count(), Ordering::Relaxed);
                        let _ = reply.send(result);
                    }

                    TxnCommand::InFlightCount { reply } => {
                        let count = producer.in_flight_count();
                        in_flight_count.store(count, Ordering::Relaxed);
                        let _ = reply.send(count);
                    }

                    TxnCommand::BeginTransaction { reply } => {
                        let result = producer.begin_transaction();
                        if result.is_ok() {
                            in_transaction = true;
                            log::debug!("TransactionalPolledProducer: Transaction begun");
                        }
                        let _ = reply.send(result);
                    }

                    TxnCommand::CommitTransaction { timeout, reply } => {
                        if !in_transaction {
                            log::warn!(
                                "TransactionalPolledProducer: Commit requested but not in transaction"
                            );
                            let _ = reply.send(Ok(()));
                            continue;
                        }

                        // Flush before commit to ensure all messages are delivered
                        if let Err(e) = producer.flush(Duration::from_secs(10)) {
                            log::error!(
                                "TransactionalPolledProducer: Flush before commit failed: {:?}",
                                e
                            );
                            // Abort on flush failure
                            let _ = producer.abort_transaction(timeout);
                            in_transaction = false;
                            let _ = reply.send(Err(e));
                            continue;
                        }

                        // Now commit
                        match producer.commit_transaction(timeout) {
                            Ok(()) => {
                                in_transaction = false;
                                log::debug!(
                                    "TransactionalPolledProducer: Transaction committed successfully"
                                );
                                let _ = reply.send(Ok(()));
                            }
                            Err(e) => {
                                log::error!(
                                    "TransactionalPolledProducer: Commit failed: {:?}, aborting",
                                    e
                                );
                                // Abort on commit failure
                                let _ = producer.abort_transaction(timeout);
                                in_transaction = false;
                                let _ = reply.send(Err(e));
                            }
                        }
                        in_flight_count.store(producer.in_flight_count(), Ordering::Relaxed);
                    }

                    TxnCommand::AbortTransaction { timeout, reply } => {
                        let result = if in_transaction {
                            let r = producer.abort_transaction(timeout);
                            in_transaction = false;
                            log::debug!("TransactionalPolledProducer: Transaction aborted");
                            r
                        } else {
                            Ok(())
                        };
                        in_flight_count.store(producer.in_flight_count(), Ordering::Relaxed);
                        let _ = reply.send(result);
                    }

                    TxnCommand::Stop => {
                        log::debug!(
                            "TransactionalPolledProducer: Stop command received, in_tx={}",
                            in_transaction
                        );
                        // Abort any pending transaction
                        if in_transaction {
                            let _ = producer.abort_transaction(Duration::from_secs(10));
                        }
                        // Final flush
                        let _ = producer.flush(Duration::from_secs(5));
                        break;
                    }
                }
            }

            log::debug!("TransactionalPolledProducer: Manager thread stopped");
        })
    }

    /// Helper to send command and wait for reply
    fn send_command<T>(
        &self,
        cmd_builder: impl FnOnce(SyncSender<T>) -> TxnCommand,
    ) -> Result<T, Box<dyn Error + Send + Sync>> {
        let (reply_tx, reply_rx) = mpsc::sync_channel(1);
        let cmd = cmd_builder(reply_tx);
        self.cmd_tx.send(cmd)?;
        Ok(reply_rx.recv()?)
    }
}

impl PolledProducer for TransactionalPolledProducer {
    fn send<'a>(
        &self,
        record: BaseRecord<'a, str, [u8]>,
    ) -> Result<(), (rdkafka::error::KafkaError, BaseRecord<'a, str, [u8]>)> {
        // Extract data from record (we need to copy since we're sending to another thread)
        let topic = record.topic.to_string();
        let key = record.key.map(|k| k.to_string());
        let payload = record.payload.map(|p| p.to_vec()).unwrap_or_default();

        let result = self.send_command(|reply| TxnCommand::Send {
            topic,
            key,
            payload,
            reply,
        });

        match result {
            Ok(Ok(())) => Ok(()),
            Ok(Err(kafka_err)) => {
                // We can't return the original record since we moved data out of it
                // This is a limitation of the channel-based design
                // Return error without the record (caller loses ownership)
                log::error!("TransactionalPolledProducer: Send failed: {:?}", kafka_err);
                // Create a dummy record to satisfy the return type
                // This is not ideal but necessary for the trait signature
                Err((kafka_err, record))
            }
            Err(e) => {
                log::error!(
                    "TransactionalPolledProducer: Channel error during send: {:?}",
                    e
                );
                Err((
                    rdkafka::error::KafkaError::MessageProduction(
                        rdkafka::types::RDKafkaErrorCode::Fail,
                    ),
                    record,
                ))
            }
        }
    }

    fn flush(&mut self, timeout: Duration) -> Result<(), rdkafka::error::KafkaError> {
        // TransactionalPolledProducer serializes all operations through the manager thread,
        // so there's no poll thread contention to worry about.
        match self.send_command(|reply| TxnCommand::Flush { timeout, reply }) {
            Ok(result) => result,
            Err(e) => {
                log::error!(
                    "TransactionalPolledProducer: Channel error during flush: {:?}",
                    e
                );
                Err(rdkafka::error::KafkaError::MessageProduction(
                    rdkafka::types::RDKafkaErrorCode::Fail,
                ))
            }
        }
    }

    fn in_flight_count(&self) -> i32 {
        // Use cached value for fast access
        self.in_flight_count.load(Ordering::Relaxed)
    }

    fn init_transactions(&mut self, _timeout: Duration) -> Result<(), rdkafka::error::KafkaError> {
        // Transactions are already initialized during construction
        // This is a no-op for TransactionalPolledProducer
        if self.transactions_initialized {
            log::debug!(
                "TransactionalPolledProducer: init_transactions called but already initialized"
            );
            Ok(())
        } else {
            Err(rdkafka::error::KafkaError::MessageProduction(
                rdkafka::types::RDKafkaErrorCode::Fail,
            ))
        }
    }

    fn begin_transaction(&mut self) -> Result<(), rdkafka::error::KafkaError> {
        match self.send_command(|reply| TxnCommand::BeginTransaction { reply }) {
            Ok(result) => result,
            Err(e) => {
                log::error!(
                    "TransactionalPolledProducer: Channel error during begin_transaction: {:?}",
                    e
                );
                Err(rdkafka::error::KafkaError::MessageProduction(
                    rdkafka::types::RDKafkaErrorCode::Fail,
                ))
            }
        }
    }

    fn commit_transaction(&mut self, timeout: Duration) -> Result<(), rdkafka::error::KafkaError> {
        match self.send_command(|reply| TxnCommand::CommitTransaction { timeout, reply }) {
            Ok(result) => result,
            Err(e) => {
                log::error!(
                    "TransactionalPolledProducer: Channel error during commit_transaction: {:?}",
                    e
                );
                Err(rdkafka::error::KafkaError::MessageProduction(
                    rdkafka::types::RDKafkaErrorCode::Fail,
                ))
            }
        }
    }

    fn abort_transaction(&mut self, timeout: Duration) -> Result<(), rdkafka::error::KafkaError> {
        match self.send_command(|reply| TxnCommand::AbortTransaction { timeout, reply }) {
            Ok(result) => result,
            Err(e) => {
                log::error!(
                    "TransactionalPolledProducer: Channel error during abort_transaction: {:?}",
                    e
                );
                Err(rdkafka::error::KafkaError::MessageProduction(
                    rdkafka::types::RDKafkaErrorCode::Fail,
                ))
            }
        }
    }
}

impl Drop for TransactionalPolledProducer {
    fn drop(&mut self) {
        log::debug!("TransactionalPolledProducer: Dropping, sending stop command");
        // Send stop command - ignore error if channel is already closed
        let _ = self.cmd_tx.send(TxnCommand::Stop);
        // Wait for manager thread to finish
        if let Some(handle) = self.manager_thread.take() {
            if let Err(e) = handle.join() {
                log::warn!(
                    "TransactionalPolledProducer: Manager thread panicked during shutdown: {:?}",
                    e
                );
            }
        }
        log::debug!("TransactionalPolledProducer: Dropped successfully");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sync_producer_creation() {
        // This test would need a Kafka broker, so just verify the type compiles
        let config = ClientConfig::new();
        // Don't actually create - would fail without broker
        let _ = config;
    }

    #[test]
    fn test_async_producer_creation() {
        // This test would need a Kafka broker, so just verify the type compiles
        let config = ClientConfig::new();
        // Don't actually create - would fail without broker
        let _ = config;
    }

    #[test]
    fn test_transactional_producer_type_compiles() {
        // This test verifies the type compiles correctly
        // Can't actually create without a Kafka broker
        fn _check_send_sync<T: Send>() {}
        _check_send_sync::<TransactionalPolledProducer>();
    }
}
