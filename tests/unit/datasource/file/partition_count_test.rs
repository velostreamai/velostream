//! Tests for DataSource::partition_count() — sources must NOT constrain processing parallelism.
//!
//! Source-level partition_count returns None because processing parallelism
//! (how many Adaptive partition workers hash-distribute GROUP BY work) is
//! independent of source reader count.

use velostream::velostream::datasource::DataSource;
use velostream::velostream::datasource::file::FileDataSource;
use velostream::velostream::datasource::file::FileMmapDataSource;
use velostream::velostream::datasource::kafka::KafkaDataSource;

#[test]
fn test_file_data_source_partition_count_is_none() {
    let ds = FileDataSource::new();
    assert_eq!(
        ds.partition_count(),
        None,
        "FileDataSource must not constrain processing parallelism"
    );
}

#[test]
fn test_file_mmap_data_source_partition_count_is_none() {
    let ds = FileMmapDataSource::new();
    assert_eq!(
        ds.partition_count(),
        None,
        "FileMmapDataSource must not constrain processing parallelism"
    );
}

#[test]
fn test_kafka_data_source_partition_count_is_none() {
    let ds = KafkaDataSource::new("localhost:9092".to_string(), "test_topic".to_string());
    assert_eq!(
        ds.partition_count(),
        None,
        "KafkaDataSource must not constrain processing parallelism"
    );
}

/// All three source types must agree: processing parallelism is unconstrained.
#[test]
fn test_all_sources_agree_on_none() {
    let file = FileDataSource::new();
    let mmap = FileMmapDataSource::new();
    let kafka = KafkaDataSource::new("localhost:9092".to_string(), "topic".to_string());

    assert_eq!(file.partition_count(), mmap.partition_count());
    assert_eq!(mmap.partition_count(), kafka.partition_count());
    assert_eq!(kafka.partition_count(), None);
}
