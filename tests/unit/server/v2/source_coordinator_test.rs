//! Source Coordinator Tests
//!
//! Tests for the SourceCoordinator for stream-stream join source reading.

use std::collections::HashMap;
use std::error::Error;
use std::time::Duration;

use async_trait::async_trait;
use velostream::velostream::datasource::{DataReader, SourceOffset};
use velostream::velostream::server::v2::{
    SourceCoordinator, SourceCoordinatorConfig, SourceCoordinatorError,
};
use velostream::velostream::sql::execution::StreamRecord;
use velostream::velostream::sql::execution::join::JoinSide;
use velostream::velostream::sql::execution::types::FieldValue;

/// Mock reader for testing
struct MockReader {
    records: Vec<StreamRecord>,
    position: usize,
}

impl MockReader {
    fn new(records: Vec<StreamRecord>) -> Self {
        Self {
            records,
            position: 0,
        }
    }

    fn from_count(count: usize) -> Self {
        let records: Vec<StreamRecord> = (0..count)
            .map(|i| {
                let mut fields = HashMap::new();
                fields.insert("id".to_string(), FieldValue::Integer(i as i64));
                StreamRecord::new(fields)
            })
            .collect();
        Self::new(records)
    }
}

#[async_trait]
impl DataReader for MockReader {
    async fn read(&mut self) -> Result<Vec<StreamRecord>, Box<dyn Error + Send + Sync>> {
        if self.position >= self.records.len() {
            return Ok(vec![]);
        }

        let batch_end = (self.position + 10).min(self.records.len());
        let batch = self.records[self.position..batch_end].to_vec();
        self.position = batch_end;
        Ok(batch)
    }

    async fn commit(&mut self) -> Result<(), Box<dyn Error + Send + Sync>> {
        Ok(())
    }

    async fn seek(&mut self, _offset: SourceOffset) -> Result<(), Box<dyn Error + Send + Sync>> {
        Ok(())
    }

    async fn has_more(&self) -> Result<bool, Box<dyn Error + Send + Sync>> {
        Ok(self.position < self.records.len())
    }
}

#[tokio::test]
async fn test_single_source_reading() {
    let config = SourceCoordinatorConfig::default();
    let (mut coordinator, mut rx) = SourceCoordinator::new(config);

    let reader = Box::new(MockReader::from_count(25));
    coordinator
        .add_source("test_source".to_string(), JoinSide::Left, reader)
        .unwrap();

    coordinator.close_sender();

    let mut received = Vec::new();
    while let Ok(record) = tokio::time::timeout(Duration::from_millis(100), rx.recv()).await {
        if let Some(r) = record {
            received.push(r);
        } else {
            break;
        }
    }

    assert_eq!(received.len(), 25);
    assert!(received.iter().all(|r| r.side == JoinSide::Left));
    assert!(received.iter().all(|r| r.source_name == "test_source"));
}

#[tokio::test]
async fn test_dual_source_concurrent_reading() {
    let config = SourceCoordinatorConfig::default();
    let (mut coordinator, mut rx) = SourceCoordinator::new(config);

    let left_reader = Box::new(MockReader::from_count(20));
    let right_reader = Box::new(MockReader::from_count(15));

    coordinator
        .add_source("left".to_string(), JoinSide::Left, left_reader)
        .unwrap();
    coordinator
        .add_source("right".to_string(), JoinSide::Right, right_reader)
        .unwrap();

    coordinator.close_sender();

    let mut left_count = 0;
    let mut right_count = 0;

    while let Ok(record) = tokio::time::timeout(Duration::from_millis(100), rx.recv()).await {
        if let Some(r) = record {
            match r.side {
                JoinSide::Left => left_count += 1,
                JoinSide::Right => right_count += 1,
            }
        } else {
            break;
        }
    }

    assert_eq!(left_count, 20);
    assert_eq!(right_count, 15);
}

#[tokio::test]
async fn test_stop_signal() {
    let config = SourceCoordinatorConfig::default();
    let (mut coordinator, mut rx) = SourceCoordinator::new(config);

    let reader = Box::new(MockReader::from_count(1000));
    coordinator
        .add_source("test".to_string(), JoinSide::Left, reader)
        .unwrap();

    let mut count = 0;
    while let Ok(Some(_)) = tokio::time::timeout(Duration::from_millis(10), rx.recv()).await {
        count += 1;
        if count >= 10 {
            coordinator.stop();
            break;
        }
    }

    assert!(coordinator.is_stopped());
    assert!(count >= 10);
}

#[tokio::test]
async fn test_stats_tracking() {
    let config = SourceCoordinatorConfig::default();
    let (mut coordinator, mut rx) = SourceCoordinator::new(config);

    let left_reader = Box::new(MockReader::from_count(10));
    let right_reader = Box::new(MockReader::from_count(5));

    coordinator
        .add_source("left".to_string(), JoinSide::Left, left_reader)
        .unwrap();
    coordinator
        .add_source("right".to_string(), JoinSide::Right, right_reader)
        .unwrap();

    coordinator.close_sender();

    while let Ok(Some(_)) = tokio::time::timeout(Duration::from_millis(100), rx.recv()).await {}

    let stats = coordinator.stats_snapshot();
    assert_eq!(stats.left_records, 10);
    assert_eq!(stats.right_records, 5);
    assert_eq!(stats.records_sent, 15);
    assert_eq!(stats.records_dropped, 0);
}

#[tokio::test]
async fn test_sequence_numbers() {
    let config = SourceCoordinatorConfig::default();
    let (mut coordinator, mut rx) = SourceCoordinator::new(config);

    let reader = Box::new(MockReader::from_count(10));
    coordinator
        .add_source("test".to_string(), JoinSide::Left, reader)
        .unwrap();

    coordinator.close_sender();

    let mut sequences = Vec::new();
    while let Ok(Some(record)) = tokio::time::timeout(Duration::from_millis(100), rx.recv()).await {
        sequences.push(record.sequence);
    }

    assert_eq!(sequences.len(), 10);
    for (i, seq) in sequences.iter().enumerate() {
        assert_eq!(*seq, (i + 1) as u64);
    }
}

#[tokio::test]
async fn test_duplicate_source_rejected() {
    let config = SourceCoordinatorConfig::default();
    let (mut coordinator, _rx) = SourceCoordinator::new(config);

    let reader1 = Box::new(MockReader::from_count(10));
    coordinator
        .add_source("my_source".to_string(), JoinSide::Left, reader1)
        .unwrap();

    let reader2 = Box::new(MockReader::from_count(10));
    let result = coordinator.add_source("my_source".to_string(), JoinSide::Right, reader2);

    assert!(result.is_err());
    match result {
        Err(SourceCoordinatorError::DuplicateSource(name)) => {
            assert_eq!(name, "my_source");
        }
        _ => panic!("Expected DuplicateSource error"),
    }

    let reader3 = Box::new(MockReader::from_count(10));
    coordinator
        .add_source("other_source".to_string(), JoinSide::Right, reader3)
        .unwrap();
}
