use std::fmt;

use crate::client::error::ClientError;

/// Unity error
#[derive(Debug)]
pub struct UnityError {

}

impl fmt::Display for UnityError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "UnityError")
    }
}

impl std::error::Error for UnityError {}

impl From<ClientError> for UnityError {
    fn from(_: ClientError) -> Self {
        UnityError {}
    }
}

