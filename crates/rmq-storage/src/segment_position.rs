/// A pointer to a message within the segment-based message store.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct SegmentPosition {
    /// Segment file ID.
    pub segment: u32,
    /// Byte offset within the segment.
    pub position: u32,
    /// Total byte size of the message on disk.
    pub bytesize: u32,
}

impl SegmentPosition {
    pub fn new(segment: u32, position: u32, bytesize: u32) -> Self {
        Self {
            segment,
            position,
            bytesize,
        }
    }
}

impl Ord for SegmentPosition {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        (self.segment, self.position).cmp(&(other.segment, other.position))
    }
}

impl PartialOrd for SegmentPosition {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}
