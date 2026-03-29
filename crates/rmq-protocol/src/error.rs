use thiserror::Error;

#[derive(Debug, Error)]
pub enum ProtocolError {
    #[error("invalid protocol header")]
    InvalidHeader,
    #[error("invalid frame type: {0}")]
    InvalidFrameType(u8),
    #[error("frame too large: {size} > {max}")]
    FrameTooLarge { size: u32, max: u32 },
    #[error("invalid frame end byte: expected 0xCE, got {0:#x}")]
    InvalidFrameEnd(u8),
    #[error("incomplete frame: need {needed} bytes, have {available}")]
    Incomplete { needed: usize, available: usize },
    #[error("unknown class/method: {class_id}/{method_id}")]
    UnknownMethod { class_id: u16, method_id: u16 },
    #[error("invalid field table: {0}")]
    InvalidFieldTable(String),
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("string too long: {len} > {max}")]
    StringTooLong { len: usize, max: usize },
}
