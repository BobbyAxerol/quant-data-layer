//! Stage A: canonical -> state topics, exactly once (KN-3 K3.2, decision D1).
//!
//! One engine step polls a bounded batch of committed canonical records,
//! transforms each into zero or more state records, and publishes them with
//! the next input offsets in one transaction. Any abortable failure aborts the
//! transaction and rewinds the consumer to its committed offsets, so the batch
//! is processed again from the same input: outputs of an aborted transaction
//! are invisible to `read_committed` readers and a retry cannot duplicate
//! them. A record the transform cannot interpret is never skipped: the engine
//! stops (fail closed) and reports the partition and offset.

use std::collections::BTreeMap;
use std::time::Duration;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct InputRecord {
    pub partition: i32,
    pub offset: i64,
    pub key: Vec<u8>,
    pub payload: Vec<u8>,
}

/// A state record to publish; `value: None` is a tombstone.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OutputRecord {
    pub topic: String,
    pub partition: i32,
    pub key: Vec<u8>,
    pub value: Option<Vec<u8>>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PipeError {
    /// The transaction must be aborted; the batch is then retried.
    Abortable(String),
    /// The client can no longer be used (fenced by a newer instance with the
    /// same transactional id, or a fatal broker error): the engine stops.
    Fatal(String),
}

/// The consumer + transactional producer pair (Kafka in production, an
/// in-memory log in tests).
pub trait Pipe {
    fn poll(&mut self, max: usize, timeout: Duration) -> Result<Vec<InputRecord>, PipeError>;
    fn begin(&mut self) -> Result<(), PipeError>;
    fn send(&mut self, record: &OutputRecord) -> Result<(), PipeError>;
    /// Add `next` (partition -> next offset to consume) to the transaction and
    /// commit it.
    fn commit(&mut self, next: &BTreeMap<i32, i64>) -> Result<(), PipeError>;
    fn abort(&mut self) -> Result<(), PipeError>;
    /// Reposition every assigned partition at its committed offset.
    fn rewind(&mut self) -> Result<(), PipeError>;
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TransformError {
    /// The record cannot be interpreted (corrupt, contract violation): stop.
    Integrity(String),
}

pub trait Transform {
    fn transform(&mut self, record: &InputRecord) -> Result<Vec<OutputRecord>, TransformError>;
}

#[derive(Clone, Debug)]
pub struct StageALimits {
    pub max_batch_records: usize,
    pub poll_timeout: Duration,
}

impl Default for StageALimits {
    fn default() -> Self {
        Self {
            max_batch_records: 1_000,
            poll_timeout: Duration::from_millis(50),
        }
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct StageAMetrics {
    pub transactions: u64,
    pub inputs: u64,
    pub outputs: u64,
    pub aborts: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Step {
    Idle,
    Committed { inputs: usize, outputs: usize },
    Aborted(String),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum StageAError {
    Fatal(String),
    Integrity {
        partition: i32,
        offset: i64,
        reason: String,
    },
}

pub struct StageA<P: Pipe, T: Transform> {
    pub pipe: P,
    pub transform: T,
    pub limits: StageALimits,
    pub metrics: StageAMetrics,
}

impl<P: Pipe, T: Transform> StageA<P, T> {
    pub fn new(pipe: P, transform: T, limits: StageALimits) -> Self {
        Self {
            pipe,
            transform,
            limits,
            metrics: StageAMetrics::default(),
        }
    }

    /// One poll -> transaction cycle.
    pub fn step(&mut self) -> Result<Step, StageAError> {
        let batch = self
            .pipe
            .poll(self.limits.max_batch_records, self.limits.poll_timeout)
            .map_err(|error| match error {
                PipeError::Fatal(reason) | PipeError::Abortable(reason) => {
                    StageAError::Fatal(reason)
                }
            })?;
        if batch.is_empty() {
            return Ok(Step::Idle);
        }
        // Transform the whole batch before opening the transaction: an
        // integrity stop then leaves nothing half-published.
        let mut outputs = Vec::new();
        let mut next = BTreeMap::new();
        for record in &batch {
            let produced = self.transform.transform(record).map_err(|error| {
                let TransformError::Integrity(reason) = error;
                StageAError::Integrity {
                    partition: record.partition,
                    offset: record.offset,
                    reason,
                }
            })?;
            outputs.extend(produced);
            let entry = next.entry(record.partition).or_insert(record.offset + 1);
            *entry = (*entry).max(record.offset + 1);
        }
        match self.publish(&outputs, &next) {
            Ok(()) => {
                self.metrics.transactions += 1;
                self.metrics.inputs += batch.len() as u64;
                self.metrics.outputs += outputs.len() as u64;
                Ok(Step::Committed {
                    inputs: batch.len(),
                    outputs: outputs.len(),
                })
            }
            Err(PipeError::Fatal(reason)) => Err(StageAError::Fatal(reason)),
            Err(PipeError::Abortable(reason)) => {
                self.metrics.aborts += 1;
                self.pipe.abort().map_err(|error| match error {
                    PipeError::Fatal(reason) | PipeError::Abortable(reason) => {
                        StageAError::Fatal(format!("abort failed: {reason}"))
                    }
                })?;
                self.pipe.rewind().map_err(|error| match error {
                    PipeError::Fatal(reason) | PipeError::Abortable(reason) => {
                        StageAError::Fatal(format!("rewind failed: {reason}"))
                    }
                })?;
                Ok(Step::Aborted(reason))
            }
        }
    }

    fn publish(
        &mut self,
        outputs: &[OutputRecord],
        next: &BTreeMap<i32, i64>,
    ) -> Result<(), PipeError> {
        self.pipe.begin()?;
        for output in outputs {
            self.pipe.send(output)?;
        }
        self.pipe.commit(next)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// An in-memory pipe with `read_committed` semantics: outputs of an open
    /// or aborted transaction are invisible, consumer offsets move only with
    /// a committed transaction, and failures can be injected.
    #[derive(Default)]
    struct MemoryPipe {
        log: BTreeMap<i32, Vec<InputRecord>>,
        committed: BTreeMap<i32, i64>,
        position: BTreeMap<i32, i64>,
        open: Option<Vec<OutputRecord>>,
        visible: Vec<OutputRecord>,
        fail_send_at: Option<usize>,
        fail_commit: Option<PipeError>,
        sends: usize,
    }

    impl MemoryPipe {
        fn with(records: &[(i32, i64, &str)]) -> Self {
            let mut pipe = MemoryPipe::default();
            for &(partition, offset, payload) in records {
                pipe.log.entry(partition).or_default().push(InputRecord {
                    partition,
                    offset,
                    key: b"k".to_vec(),
                    payload: payload.as_bytes().to_vec(),
                });
            }
            pipe
        }
    }

    impl Pipe for MemoryPipe {
        fn poll(&mut self, max: usize, _timeout: Duration) -> Result<Vec<InputRecord>, PipeError> {
            let mut batch = Vec::new();
            for (partition, records) in &self.log {
                let from = *self.position.get(partition).unwrap_or(&0);
                for record in records.iter().filter(|record| record.offset >= from) {
                    if batch.len() == max {
                        break;
                    }
                    batch.push(record.clone());
                    self.position.insert(*partition, record.offset + 1);
                }
            }
            Ok(batch)
        }
        fn begin(&mut self) -> Result<(), PipeError> {
            assert!(self.open.is_none(), "one transaction at a time");
            self.open = Some(Vec::new());
            Ok(())
        }
        fn send(&mut self, record: &OutputRecord) -> Result<(), PipeError> {
            self.sends += 1;
            if self.fail_send_at == Some(self.sends) {
                return Err(PipeError::Abortable("injected send failure".into()));
            }
            self.open.as_mut().expect("open").push(record.clone());
            Ok(())
        }
        fn commit(&mut self, next: &BTreeMap<i32, i64>) -> Result<(), PipeError> {
            if let Some(error) = self.fail_commit.take() {
                return Err(error);
            }
            let outputs = self.open.take().expect("open");
            self.visible.extend(outputs);
            for (&partition, &offset) in next {
                self.committed.insert(partition, offset);
            }
            Ok(())
        }
        fn abort(&mut self) -> Result<(), PipeError> {
            self.open = None;
            Ok(())
        }
        fn rewind(&mut self) -> Result<(), PipeError> {
            self.position = self.committed.clone();
            Ok(())
        }
    }

    /// Two outputs per input, keyed by the input; `bad` payloads are corrupt.
    struct Doubler;

    impl Transform for Doubler {
        fn transform(&mut self, record: &InputRecord) -> Result<Vec<OutputRecord>, TransformError> {
            if record.payload == b"bad" {
                return Err(TransformError::Integrity("undecodable".into()));
            }
            Ok((0..2)
                .map(|index| OutputRecord {
                    topic: "md.latest.v2".into(),
                    partition: index,
                    key: format!("{}:{}:{index}", record.partition, record.offset).into_bytes(),
                    value: Some(record.payload.clone()),
                })
                .collect())
        }
    }

    fn keys(pipe: &MemoryPipe) -> Vec<String> {
        pipe.visible
            .iter()
            .map(|record| String::from_utf8(record.key.clone()).unwrap())
            .collect()
    }

    fn limits(max: usize) -> StageALimits {
        StageALimits {
            max_batch_records: max,
            poll_timeout: Duration::ZERO,
        }
    }

    #[test]
    fn a_committed_batch_publishes_outputs_with_the_next_offsets() {
        let pipe = MemoryPipe::with(&[(0, 0, "a"), (0, 1, "b"), (1, 7, "c")]);
        let mut stage = StageA::new(pipe, Doubler, limits(10));
        assert_eq!(
            stage.step().unwrap(),
            Step::Committed {
                inputs: 3,
                outputs: 6
            }
        );
        assert_eq!(stage.pipe.committed, BTreeMap::from([(0, 2), (1, 8)]));
        assert_eq!(stage.step().unwrap(), Step::Idle);
        assert_eq!(stage.metrics.transactions, 1);
    }

    #[test]
    fn an_aborted_transaction_is_retried_from_the_committed_offsets_once() {
        let mut pipe = MemoryPipe::with(&[(0, 0, "a"), (0, 1, "b")]);
        pipe.fail_send_at = Some(3);
        let mut stage = StageA::new(pipe, Doubler, limits(10));
        assert!(matches!(stage.step().unwrap(), Step::Aborted(_)));
        assert!(
            stage.pipe.visible.is_empty(),
            "aborted outputs are invisible"
        );
        assert!(stage.pipe.committed.is_empty(), "offsets did not move");
        assert_eq!(
            stage.step().unwrap(),
            Step::Committed {
                inputs: 2,
                outputs: 4
            }
        );
        assert_eq!(keys(&stage.pipe), ["0:0:0", "0:0:1", "0:1:0", "0:1:1"]);
        assert_eq!(stage.metrics.aborts, 1);
    }

    #[test]
    fn a_failed_commit_aborts_and_a_crash_before_commit_publishes_nothing() {
        let mut pipe = MemoryPipe::with(&[(0, 0, "a")]);
        pipe.fail_commit = Some(PipeError::Abortable("coordinator moved".into()));
        let mut stage = StageA::new(pipe, Doubler, limits(10));
        assert!(matches!(stage.step().unwrap(), Step::Aborted(_)));
        assert_eq!(
            stage.step().unwrap(),
            Step::Committed {
                inputs: 1,
                outputs: 2
            }
        );
        // A crash between begin and commit: the next instance starts from the
        // committed offsets, and the open transaction never became visible.
        let mut pipe = MemoryPipe::with(&[(0, 0, "a"), (0, 1, "b")]);
        pipe.begin().unwrap();
        pipe.send(&OutputRecord {
            topic: "md.latest.v2".into(),
            partition: 0,
            key: b"orphan".to_vec(),
            value: None,
        })
        .unwrap();
        pipe.abort().unwrap(); // what the broker does when the transaction times out
        pipe.rewind().unwrap();
        let mut restarted = StageA::new(pipe, Doubler, limits(10));
        restarted.step().unwrap();
        assert!(!keys(&restarted.pipe).contains(&"orphan".to_owned()));
        assert_eq!(restarted.pipe.committed, BTreeMap::from([(0, 2)]));
    }

    #[test]
    fn an_integrity_failure_stops_before_anything_is_published() {
        let pipe = MemoryPipe::with(&[(0, 0, "a"), (0, 1, "bad"), (0, 2, "c")]);
        let mut stage = StageA::new(pipe, Doubler, limits(10));
        assert_eq!(
            stage.step().unwrap_err(),
            StageAError::Integrity {
                partition: 0,
                offset: 1,
                reason: "undecodable".into()
            }
        );
        assert!(stage.pipe.visible.is_empty());
        assert!(
            stage.pipe.committed.is_empty(),
            "the checkpoint never passes the data"
        );
    }

    #[test]
    fn a_fatal_error_stops_the_engine() {
        let mut pipe = MemoryPipe::with(&[(0, 0, "a")]);
        pipe.fail_commit = Some(PipeError::Fatal("fenced by a newer instance".into()));
        let mut stage = StageA::new(pipe, Doubler, limits(10));
        assert_eq!(
            stage.step().unwrap_err(),
            StageAError::Fatal("fenced by a newer instance".into())
        );
    }

    #[test]
    fn batches_are_bounded() {
        let pipe = MemoryPipe::with(&[(0, 0, "a"), (0, 1, "b"), (0, 2, "c")]);
        let mut stage = StageA::new(pipe, Doubler, limits(2));
        assert_eq!(
            stage.step().unwrap(),
            Step::Committed {
                inputs: 2,
                outputs: 4
            }
        );
        assert_eq!(
            stage.step().unwrap(),
            Step::Committed {
                inputs: 1,
                outputs: 2
            }
        );
        assert_eq!(stage.pipe.committed, BTreeMap::from([(0, 3)]));
    }
}
