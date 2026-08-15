#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Cursor {
    pub stream: String,
    pub transport_partition: i32,
    pub partition_key: String,
    pub offset: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DurableRecord {
    pub stream: String,
    pub partition_key: String,
    pub event_id: Vec<u8>,
    pub payload: Vec<u8>,
    pub accepted_at_ns: i64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AppendResult {
    pub cursor: Cursor,
    pub duplicate: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RetryClass {
    Retryable,
    NonRetryable,
    Capacity,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum QueuePolicy {
    LosslessBackpressure,
    LatestStateCoalescing,
}

pub trait EventSink {
    type Error;
    fn append(&mut self, record: DurableRecord) -> Result<AppendResult, Self::Error>;
}

pub trait EventSource {
    type Error;
    fn read_after(
        &self,
        cursor: Option<&Cursor>,
        limit: usize,
    ) -> Result<Vec<DurableRecord>, Self::Error>;
    fn checkpoint(&mut self, consumer_id: &str, cursor: &Cursor) -> Result<(), Self::Error>;
}

pub trait VenueAdapter {
    type RawFrame;
    type Error;
    fn canonicalize(&self, frame: &Self::RawFrame) -> Result<Vec<u8>, Self::Error>;
}

pub trait DurableBrokerClient: EventSink + EventSource {}
impl<T: EventSink + EventSource> DurableBrokerClient for T {}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct FencingLease {
    pub epoch: u64,
    pub expires_at_ns: i64,
}

impl FencingLease {
    pub fn permits(self, event_epoch: u64, now_ns: i64) -> bool {
        event_epoch == self.epoch && now_ns < self.expires_at_ns
    }
}
