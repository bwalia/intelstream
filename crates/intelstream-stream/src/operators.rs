//! Stream processing operators: filter, map, flat_map, etc.

use crate::StreamRecord;

/// Trait for stream processing operators.
pub trait Operator: Send + Sync + std::fmt::Debug {
    /// Process a single record and return zero or more output records.
    fn process(&self, record: StreamRecord) -> Vec<StreamRecord>;

    /// Name of this operator (for debugging and metrics).
    fn name(&self) -> &str;
}

/// Filters records based on a predicate.
pub struct FilterOperator {
    predicate: Box<dyn Fn(&StreamRecord) -> bool + Send + Sync>,
}

impl FilterOperator {
    pub fn new<F>(predicate: F) -> Self
    where
        F: Fn(&StreamRecord) -> bool + Send + Sync + 'static,
    {
        Self {
            predicate: Box::new(predicate),
        }
    }
}

impl std::fmt::Debug for FilterOperator {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FilterOperator").finish()
    }
}

impl Operator for FilterOperator {
    fn process(&self, record: StreamRecord) -> Vec<StreamRecord> {
        if (self.predicate)(&record) {
            vec![record]
        } else {
            vec![]
        }
    }

    fn name(&self) -> &str {
        "filter"
    }
}

/// Transforms each record using a mapping function.
pub struct MapOperator {
    transform: Box<dyn Fn(StreamRecord) -> StreamRecord + Send + Sync>,
}

impl MapOperator {
    pub fn new<F>(transform: F) -> Self
    where
        F: Fn(StreamRecord) -> StreamRecord + Send + Sync + 'static,
    {
        Self {
            transform: Box::new(transform),
        }
    }
}

impl std::fmt::Debug for MapOperator {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MapOperator").finish()
    }
}

impl Operator for MapOperator {
    fn process(&self, record: StreamRecord) -> Vec<StreamRecord> {
        vec![(self.transform)(record)]
    }

    fn name(&self) -> &str {
        "map"
    }
}

/// Transforms each record into zero or more records.
pub struct FlatMapOperator {
    transform: Box<dyn Fn(StreamRecord) -> Vec<StreamRecord> + Send + Sync>,
}

impl FlatMapOperator {
    pub fn new<F>(transform: F) -> Self
    where
        F: Fn(StreamRecord) -> Vec<StreamRecord> + Send + Sync + 'static,
    {
        Self {
            transform: Box::new(transform),
        }
    }
}

impl std::fmt::Debug for FlatMapOperator {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FlatMapOperator").finish()
    }
}

impl Operator for FlatMapOperator {
    fn process(&self, record: StreamRecord) -> Vec<StreamRecord> {
        (self.transform)(record)
    }

    fn name(&self) -> &str {
        "flat_map"
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::StreamRecord;

    fn test_record(value: &str) -> StreamRecord {
        StreamRecord {
            key: Some(b"key".to_vec()),
            value: value.as_bytes().to_vec(),
            timestamp: 1234567890,
            topic: "test-topic".to_string(),
            partition: 0,
            offset: 0,
            headers: std::collections::HashMap::new(),
        }
    }

    #[test]
    fn test_filter_passes_matching() {
        let filter = FilterOperator::new(|r: &StreamRecord| r.value.len() > 3);
        let record = test_record("hello");
        let result = filter.process(record);
        assert_eq!(result.len(), 1);
    }

    #[test]
    fn test_filter_blocks_non_matching() {
        let filter = FilterOperator::new(|r: &StreamRecord| r.value.len() > 10);
        let record = test_record("hi");
        let result = filter.process(record);
        assert!(result.is_empty());
    }

    #[test]
    fn test_map_transforms_record() {
        let map_op = MapOperator::new(|mut r: StreamRecord| {
            r.value = r.value.iter().map(|b| b.to_ascii_uppercase()).collect();
            r
        });
        let record = test_record("hello");
        let result = map_op.process(record);
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].value, b"HELLO");
    }

    #[test]
    fn test_flat_map_expands_records() {
        let flat_map = FlatMapOperator::new(|r: StreamRecord| vec![r.clone(), r]);
        let record = test_record("hello");
        let result = flat_map.process(record);
        assert_eq!(result.len(), 2);
    }

    #[test]
    fn test_flat_map_can_produce_empty() {
        let flat_map = FlatMapOperator::new(|_r: StreamRecord| vec![]);
        let record = test_record("hello");
        let result = flat_map.process(record);
        assert!(result.is_empty());
    }

    #[test]
    fn test_operator_names() {
        let filter = FilterOperator::new(|_: &StreamRecord| true);
        let map_op = MapOperator::new(|r: StreamRecord| r);
        let flat_map = FlatMapOperator::new(|r: StreamRecord| vec![r]);

        assert_eq!(filter.name(), "filter");
        assert_eq!(map_op.name(), "map");
        assert_eq!(flat_map.name(), "flat_map");
    }
}
