use std::collections::VecDeque;

use crate::segment_position::SegmentPosition;

/// Stores requeued message positions in publish order.
///
/// When a message is nacked/rejected with requeue=true, its segment position
/// is inserted back into this store. Messages are consumed from the front.
pub struct RequeuedStore {
    queue: VecDeque<SegmentPosition>,
}

impl RequeuedStore {
    pub fn new() -> Self {
        Self {
            queue: VecDeque::new(),
        }
    }

    /// Requeue a message position, maintaining sorted (publish) order.
    pub fn requeue(&mut self, sp: SegmentPosition) {
        // Insert in sorted position to maintain publish order
        let pos = self.queue.partition_point(|existing| existing < &sp);
        self.queue.insert(pos, sp);
    }

    /// Pop the first (oldest) requeued message, if any.
    pub fn pop_front(&mut self) -> Option<SegmentPosition> {
        self.queue.pop_front()
    }

    /// Peek at the first requeued message without removing it.
    pub fn peek_front(&self) -> Option<&SegmentPosition> {
        self.queue.front()
    }

    pub fn is_empty(&self) -> bool {
        self.queue.is_empty()
    }

    pub fn len(&self) -> usize {
        self.queue.len()
    }
}

impl Default for RequeuedStore {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_requeue_maintains_order() {
        let mut store = RequeuedStore::new();

        // Requeue out of order
        store.requeue(SegmentPosition::new(1, 200, 50));
        store.requeue(SegmentPosition::new(1, 100, 50));
        store.requeue(SegmentPosition::new(1, 300, 50));

        // Should come back in sorted order
        assert_eq!(store.pop_front().unwrap().position, 100);
        assert_eq!(store.pop_front().unwrap().position, 200);
        assert_eq!(store.pop_front().unwrap().position, 300);
        assert!(store.is_empty());
    }

    #[test]
    fn test_requeue_across_segments() {
        let mut store = RequeuedStore::new();

        store.requeue(SegmentPosition::new(2, 0, 50));
        store.requeue(SegmentPosition::new(1, 500, 50));
        store.requeue(SegmentPosition::new(1, 100, 50));

        let first = store.pop_front().unwrap();
        assert_eq!(first.segment, 1);
        assert_eq!(first.position, 100);

        let second = store.pop_front().unwrap();
        assert_eq!(second.segment, 1);
        assert_eq!(second.position, 500);

        let third = store.pop_front().unwrap();
        assert_eq!(third.segment, 2);
        assert_eq!(third.position, 0);
    }
}
