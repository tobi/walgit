use serde::{Deserialize, Serialize};
use walgit_store::{GetOptions, ObjectMeta, PutMode, PutOptions, StoreError, Version};

#[derive(Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case", deny_unknown_fields)]
pub enum Operation {
    Get {
        key: String,
        options: ReadOptions,
    },
    Head {
        key: String,
    },
    Put {
        key: String,
        len: u64,
        mode: Mode,
        immutable: bool,
        content_type: Option<String>,
    },
    Delete {
        key: String,
        version: Option<String>,
    },
    List {
        prefix: String,
        start_after: Option<String>,
    },
    ListPrefixes {
        prefix: String,
    },
    SignedGetUrl {
        key: String,
        ttl_secs: u64,
    },
    AccelTarget {
        key: String,
    },
    Compose {
        key: String,
        sources: Vec<String>,
        mode: Mode,
        immutable: bool,
        content_type: Option<String>,
    },
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReadOptions {
    pub if_none_match: Option<String>,
    pub if_match: Option<String>,
    pub range: Option<std::ops::Range<u64>>,
}
impl From<GetOptions> for ReadOptions {
    fn from(o: GetOptions) -> Self {
        Self {
            if_none_match: o.if_none_match.map(|v| v.to_string()),
            if_match: o.if_match.map(|v| v.to_string()),
            range: o.range,
        }
    }
}
impl From<ReadOptions> for GetOptions {
    fn from(o: ReadOptions) -> Self {
        Self {
            if_none_match: o.if_none_match.map(Version::new),
            if_match: o.if_match.map(Version::new),
            range: o.range,
        }
    }
}
#[derive(Serialize, Deserialize)]
#[serde(tag = "mode", content = "version", rename_all = "snake_case")]
pub enum Mode {
    Overwrite,
    Create,
    Update(String),
}
impl From<PutMode> for Mode {
    fn from(m: PutMode) -> Self {
        match m {
            PutMode::Overwrite => Self::Overwrite,
            PutMode::Create => Self::Create,
            PutMode::Update(v) => Self::Update(v.to_string()),
        }
    }
}
impl From<Mode> for PutMode {
    fn from(m: Mode) -> Self {
        match m {
            Mode::Overwrite => Self::Overwrite,
            Mode::Create => Self::Create,
            Mode::Update(v) => Self::Update(Version::new(v)),
        }
    }
}

// ObjectStore's MIME hint has a static lifetime. Only known hints can be
// represented without leaking a caller-controlled string on each put.
pub fn put_options(mode: Mode, immutable: bool, content_type: Option<&str>) -> PutOptions {
    let content_type = match content_type {
        Some("application/octet-stream") => Some("application/octet-stream"),
        Some("application/x-git-packed-objects") => Some("application/x-git-packed-objects"),
        Some("application/x-git-bundle") => Some("application/x-git-bundle"),
        Some("application/json") => Some("application/json"),
        Some("application/x-protobuf") => Some("application/x-protobuf"),
        Some("text/plain") => Some("text/plain"),
        _ => None,
    };
    PutOptions {
        mode: mode.into(),
        immutable,
        content_type,
    }
}

#[derive(Serialize, Deserialize)]
pub struct Meta {
    pub key: String,
    pub size: u64,
    pub version: String,
}
impl From<ObjectMeta> for Meta {
    fn from(m: ObjectMeta) -> Self {
        Self {
            key: m.key,
            size: m.size,
            version: m.version.to_string(),
        }
    }
}
impl From<Meta> for ObjectMeta {
    fn from(m: Meta) -> Self {
        Self {
            key: m.key,
            size: m.size,
            version: Version::new(m.version),
        }
    }
}
#[derive(Serialize, Deserialize)]
#[serde(tag = "result", rename_all = "snake_case")]
pub enum ReadResult {
    Object { meta: Meta },
    NotModified { version: String },
}
#[derive(Serialize, Deserialize)]
pub struct Accel {
    pub url: String,
    pub authorization: Option<String>,
}

#[derive(Serialize, Deserialize)]
#[serde(tag = "error", rename_all = "snake_case")]
pub enum WireError {
    NotFound {
        key: String,
    },
    PreconditionFailed {
        key: String,
        current: Option<String>,
    },
    Retryable {
        message: String,
    },
    InvalidArgument {
        message: String,
    },
    Other {
        message: String,
    },
}
impl From<StoreError> for WireError {
    fn from(e: StoreError) -> Self {
        match e {
            StoreError::NotFound { key } => Self::NotFound { key },
            StoreError::PreconditionFailed { key, current } => Self::PreconditionFailed {
                key,
                current: current.map(|v| v.to_string()),
            },
            StoreError::Retryable(e) => Self::Retryable {
                message: e.to_string(),
            },
            StoreError::InvalidArgument(message) => Self::InvalidArgument { message },
            StoreError::Other(e) => Self::Other {
                message: e.to_string(),
            },
        }
    }
}
impl From<WireError> for StoreError {
    fn from(e: WireError) -> Self {
        match e {
            WireError::NotFound { key } => Self::NotFound { key },
            WireError::PreconditionFailed { key, current } => Self::PreconditionFailed {
                key,
                current: current.map(Version::new),
            },
            WireError::Retryable { message } => Self::retryable(anyhow::anyhow!(message)),
            WireError::InvalidArgument { message } => Self::InvalidArgument(message),
            WireError::Other { message } => Self::other(anyhow::anyhow!(message)),
        }
    }
}
