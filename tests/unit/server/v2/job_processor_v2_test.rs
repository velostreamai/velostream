//! V2 JobProcessor trait implementation tests

use velostream::velostream::server::processors::JobProcessor;
use velostream::velostream::server::v2::AdaptiveJobProcessor;

#[test]
fn test_v2_processor_interface() {
    // This test ensures AdaptiveJobProcessor can be used as JobProcessor
    let coordinator = AdaptiveJobProcessor::new(Default::default());
    assert_eq!(coordinator.processor_version(), "V2");
    assert_eq!(coordinator.processor_name(), "AdaptiveJobProcessor");
    assert!(coordinator.num_partitions() > 0);
}

#[test]
fn test_v2_processor_name() {
    let coordinator = AdaptiveJobProcessor::new(Default::default());
    assert_eq!(coordinator.processor_name(), "AdaptiveJobProcessor");
}

#[test]
fn test_v2_processor_version() {
    let coordinator = AdaptiveJobProcessor::new(Default::default());
    assert_eq!(coordinator.processor_version(), "V2");
}

#[test]
fn test_v2_processor_partition_count() {
    let coordinator = AdaptiveJobProcessor::new(Default::default());
    let num_partitions = coordinator.num_partitions();
    assert!(
        num_partitions > 0,
        "V2 processor must have at least 1 partition"
    );
}
