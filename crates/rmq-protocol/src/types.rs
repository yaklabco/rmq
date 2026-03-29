/// AMQP 0.9.1 protocol header: "AMQP\x00\x00\x09\x01"
pub const PROTOCOL_HEADER: [u8; 8] = [b'A', b'M', b'Q', b'P', 0, 0, 9, 1];

/// Frame type constants
pub const FRAME_METHOD: u8 = 1;
pub const FRAME_HEADER: u8 = 2;
pub const FRAME_BODY: u8 = 3;
pub const FRAME_HEARTBEAT: u8 = 8;

/// Frame end marker
pub const FRAME_END: u8 = 0xCE;

/// Minimum frame size per spec
pub const FRAME_MIN_SIZE: u32 = 4096;

/// Default maximum frame size
pub const DEFAULT_FRAME_MAX: u32 = 131_072;

/// Default channel max
pub const DEFAULT_CHANNEL_MAX: u16 = 2048;

/// Default heartbeat interval in seconds
pub const DEFAULT_HEARTBEAT: u16 = 300;

/// AMQP class IDs
pub const CLASS_CONNECTION: u16 = 10;
pub const CLASS_CHANNEL: u16 = 20;
pub const CLASS_EXCHANGE: u16 = 40;
pub const CLASS_QUEUE: u16 = 50;
pub const CLASS_BASIC: u16 = 60;
pub const CLASS_CONFIRM: u16 = 85;
pub const CLASS_TX: u16 = 90;

/// Connection method IDs
pub const METHOD_CONNECTION_START: u16 = 10;
pub const METHOD_CONNECTION_START_OK: u16 = 11;
pub const METHOD_CONNECTION_SECURE: u16 = 20;
pub const METHOD_CONNECTION_SECURE_OK: u16 = 21;
pub const METHOD_CONNECTION_TUNE: u16 = 30;
pub const METHOD_CONNECTION_TUNE_OK: u16 = 31;
pub const METHOD_CONNECTION_OPEN: u16 = 40;
pub const METHOD_CONNECTION_OPEN_OK: u16 = 41;
pub const METHOD_CONNECTION_CLOSE: u16 = 50;
pub const METHOD_CONNECTION_CLOSE_OK: u16 = 51;
pub const METHOD_CONNECTION_BLOCKED: u16 = 60;
pub const METHOD_CONNECTION_UNBLOCKED: u16 = 61;
pub const METHOD_CONNECTION_UPDATE_SECRET: u16 = 70;
pub const METHOD_CONNECTION_UPDATE_SECRET_OK: u16 = 71;

/// Channel method IDs
pub const METHOD_CHANNEL_OPEN: u16 = 10;
pub const METHOD_CHANNEL_OPEN_OK: u16 = 11;
pub const METHOD_CHANNEL_FLOW: u16 = 20;
pub const METHOD_CHANNEL_FLOW_OK: u16 = 21;
pub const METHOD_CHANNEL_CLOSE: u16 = 40;
pub const METHOD_CHANNEL_CLOSE_OK: u16 = 41;

/// Exchange method IDs
pub const METHOD_EXCHANGE_DECLARE: u16 = 10;
pub const METHOD_EXCHANGE_DECLARE_OK: u16 = 11;
pub const METHOD_EXCHANGE_DELETE: u16 = 20;
pub const METHOD_EXCHANGE_DELETE_OK: u16 = 21;
pub const METHOD_EXCHANGE_BIND: u16 = 30;
pub const METHOD_EXCHANGE_BIND_OK: u16 = 31;
pub const METHOD_EXCHANGE_UNBIND: u16 = 40;
pub const METHOD_EXCHANGE_UNBIND_OK: u16 = 51;

/// Queue method IDs
pub const METHOD_QUEUE_DECLARE: u16 = 10;
pub const METHOD_QUEUE_DECLARE_OK: u16 = 11;
pub const METHOD_QUEUE_BIND: u16 = 20;
pub const METHOD_QUEUE_BIND_OK: u16 = 21;
pub const METHOD_QUEUE_UNBIND: u16 = 50;
pub const METHOD_QUEUE_UNBIND_OK: u16 = 51;
pub const METHOD_QUEUE_PURGE: u16 = 30;
pub const METHOD_QUEUE_PURGE_OK: u16 = 31;
pub const METHOD_QUEUE_DELETE: u16 = 40;
pub const METHOD_QUEUE_DELETE_OK: u16 = 41;

/// Basic method IDs
pub const METHOD_BASIC_QOS: u16 = 10;
pub const METHOD_BASIC_QOS_OK: u16 = 11;
pub const METHOD_BASIC_CONSUME: u16 = 20;
pub const METHOD_BASIC_CONSUME_OK: u16 = 21;
pub const METHOD_BASIC_CANCEL: u16 = 30;
pub const METHOD_BASIC_CANCEL_OK: u16 = 31;
pub const METHOD_BASIC_PUBLISH: u16 = 40;
pub const METHOD_BASIC_RETURN: u16 = 50;
pub const METHOD_BASIC_DELIVER: u16 = 60;
pub const METHOD_BASIC_GET: u16 = 70;
pub const METHOD_BASIC_GET_OK: u16 = 71;
pub const METHOD_BASIC_GET_EMPTY: u16 = 72;
pub const METHOD_BASIC_ACK: u16 = 80;
pub const METHOD_BASIC_REJECT: u16 = 90;
pub const METHOD_BASIC_RECOVER_ASYNC: u16 = 100;
pub const METHOD_BASIC_RECOVER: u16 = 110;
pub const METHOD_BASIC_RECOVER_OK: u16 = 111;
pub const METHOD_BASIC_NACK: u16 = 120;

/// Confirm method IDs
pub const METHOD_CONFIRM_SELECT: u16 = 10;
pub const METHOD_CONFIRM_SELECT_OK: u16 = 11;

/// Tx method IDs
pub const METHOD_TX_SELECT: u16 = 10;
pub const METHOD_TX_SELECT_OK: u16 = 11;
pub const METHOD_TX_COMMIT: u16 = 20;
pub const METHOD_TX_COMMIT_OK: u16 = 21;
pub const METHOD_TX_ROLLBACK: u16 = 30;
pub const METHOD_TX_ROLLBACK_OK: u16 = 31;

/// AMQP short string: max 255 bytes
pub const SHORT_STRING_MAX: usize = 255;

/// Reply codes
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u16)]
pub enum ReplyCode {
    Success = 200,
    ContentTooLarge = 311,
    NoConsumers = 313,
    ConnectionForced = 320,
    InvalidPath = 402,
    AccessRefused = 403,
    NotFound = 404,
    ResourceLocked = 405,
    PreconditionFailed = 406,
    FrameError = 501,
    SyntaxError = 502,
    CommandInvalid = 503,
    ChannelError = 504,
    UnexpectedFrame = 505,
    ResourceError = 506,
    NotAllowed = 530,
    NotImplemented = 540,
    InternalError = 541,
}

impl ReplyCode {
    pub fn from_u16(code: u16) -> Option<Self> {
        match code {
            200 => Some(Self::Success),
            311 => Some(Self::ContentTooLarge),
            313 => Some(Self::NoConsumers),
            320 => Some(Self::ConnectionForced),
            402 => Some(Self::InvalidPath),
            403 => Some(Self::AccessRefused),
            404 => Some(Self::NotFound),
            405 => Some(Self::ResourceLocked),
            406 => Some(Self::PreconditionFailed),
            501 => Some(Self::FrameError),
            502 => Some(Self::SyntaxError),
            503 => Some(Self::CommandInvalid),
            504 => Some(Self::ChannelError),
            505 => Some(Self::UnexpectedFrame),
            506 => Some(Self::ResourceError),
            530 => Some(Self::NotAllowed),
            540 => Some(Self::NotImplemented),
            541 => Some(Self::InternalError),
            _ => None,
        }
    }
}
