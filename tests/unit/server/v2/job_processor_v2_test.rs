//! V2 JobProcessor trait implementation tests

use velostream::velostream::server::processors::JobProcessor;
use velostream::velostream::server::v2::PartitionedJobCoordinator;

#[test]
fn test_v2_processor_interface() {
    // This test ensures PartitionedJobCoordinator can be used as JobProcessor
    let coordinator = PartitionedJobCoordinator::new(Default::default());
    assert_eq!(coordinator.processor_version(), "V2");
    assert_eq!(coordinator.processor_name(), "PartitionedJobCoordinator");
    assert!(coordinator.num_partitions() > 0);
}

#[test]
fn test_v2_processor_name() {
    let coordinator = PartitionedJobCoordinator::new(Default::default());
    assert_eq!(coordinator.processor_name(), "PartitionedJobCoordinator");
}

#[test]
fn test_v2_processor_version() {
    let coordinator = PartitionedJobCoordinator::new(Default::default());
    assert_eq!(coordinator.processor_version(), "V2");
}

#[test]
fn test_v2_processor_partition_count() {
    let coordinator = PartitionedJobCoordinator::new(Default::default());
    let num_partitions = coordinator.num_partitions();
    assert!(
        num_partitions > 0,
        "V2 processor must have at least 1 partition"
    );
}
