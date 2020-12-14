use crate::transport::Compression;

/// Custom connection parameters
#[derive(Copy, Clone, Debug)]
pub struct ConnectionParams {
    pub compression: Option<Compression>,
}

impl Default for ConnectionParams {
    fn default() -> Self {
        Self { compression: None }
    }
}

impl Into<ConnectionParams> for Option<Compression> {
    fn into(self) -> ConnectionParams {
        ConnectionParams { compression: self }
    }
}
