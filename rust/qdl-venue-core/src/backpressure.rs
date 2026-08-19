use std::collections::{BTreeMap, VecDeque};

use serde::Deserialize;

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum DeliveryClass {
    Lossless,
    LatestState,
    InProgressBar,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct QueueItem {
    pub key: String,
    pub payload: Vec<u8>,
    pub class: DeliveryClass,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum EnqueueDecision {
    Enqueued,
    Coalesced,
    Backpressure,
    SpoolRequired,
    Disconnect,
}

#[derive(Clone, Debug)]
pub struct BoundedLifecycleQueue {
    items: VecDeque<QueueItem>,
    latest_indexes: BTreeMap<String, usize>,
    max_items: usize,
    spool_available: bool,
    spool_full: bool,
}

impl BoundedLifecycleQueue {
    pub fn new(max_items: usize, spool_available: bool) -> Result<Self, String> {
        if max_items == 0 {
            return Err("queue bound must be positive".into());
        }
        Ok(Self {
            items: VecDeque::new(),
            latest_indexes: BTreeMap::new(),
            max_items,
            spool_available,
            spool_full: false,
        })
    }

    pub fn set_spool_full(&mut self, full: bool) {
        self.spool_full = full;
    }

    pub fn len(&self) -> usize {
        self.items.len()
    }

    pub fn is_empty(&self) -> bool {
        self.items.is_empty()
    }

    pub fn enqueue(&mut self, item: QueueItem) -> EnqueueDecision {
        if item.class != DeliveryClass::Lossless {
            if let Some(index) = self.latest_indexes.get(&item.key).copied() {
                if let Some(existing) = self.items.get_mut(index) {
                    *existing = item;
                    return EnqueueDecision::Coalesced;
                }
            }
        }
        if self.items.len() >= self.max_items {
            return match item.class {
                DeliveryClass::LatestState | DeliveryClass::InProgressBar => {
                    EnqueueDecision::Backpressure
                }
                DeliveryClass::Lossless if self.spool_available && !self.spool_full => {
                    EnqueueDecision::SpoolRequired
                }
                DeliveryClass::Lossless if self.spool_full => EnqueueDecision::Disconnect,
                DeliveryClass::Lossless => EnqueueDecision::Backpressure,
            };
        }
        let index = self.items.len();
        if item.class != DeliveryClass::Lossless {
            self.latest_indexes.insert(item.key.clone(), index);
        }
        self.items.push_back(item);
        EnqueueDecision::Enqueued
    }

    pub fn pop_front(&mut self) -> Option<QueueItem> {
        let item = self.items.pop_front()?;
        self.reindex();
        Some(item)
    }

    fn reindex(&mut self) {
        self.latest_indexes.clear();
        for (index, item) in self.items.iter().enumerate() {
            if item.class != DeliveryClass::Lossless {
                self.latest_indexes.insert(item.key.clone(), index);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{BoundedLifecycleQueue, DeliveryClass, EnqueueDecision, QueueItem};

    fn item(key: &str, value: u8, class: DeliveryClass) -> QueueItem {
        QueueItem {
            key: key.into(),
            payload: vec![value],
            class,
        }
    }

    #[test]
    fn latest_state_coalesces_but_lossless_never_silently_drops() {
        let mut queue = BoundedLifecycleQueue::new(2, true).unwrap();
        assert_eq!(
            queue.enqueue(item("bbo", 1, DeliveryClass::LatestState)),
            EnqueueDecision::Enqueued
        );
        assert_eq!(
            queue.enqueue(item("bbo", 2, DeliveryClass::LatestState)),
            EnqueueDecision::Coalesced
        );
        assert_eq!(
            queue.enqueue(item("trade", 3, DeliveryClass::Lossless)),
            EnqueueDecision::Enqueued
        );
        assert_eq!(
            queue.enqueue(item("trade", 4, DeliveryClass::Lossless)),
            EnqueueDecision::SpoolRequired
        );
        queue.set_spool_full(true);
        assert_eq!(
            queue.enqueue(item("trade", 5, DeliveryClass::Lossless)),
            EnqueueDecision::Disconnect
        );
        assert_eq!(queue.pop_front().unwrap().payload, vec![2]);
    }

    #[test]
    fn final_bar_must_be_declared_lossless() {
        let mut queue = BoundedLifecycleQueue::new(1, false).unwrap();
        queue.enqueue(item("bar:open", 1, DeliveryClass::InProgressBar));
        assert_eq!(
            queue.enqueue(item("bar:final", 2, DeliveryClass::Lossless)),
            EnqueueDecision::Backpressure
        );
    }
}
