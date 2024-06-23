use crate::client::error::ClientError;

#[derive(Debug)]
pub struct UnityError {}

impl From<ClientError> for UnityError {
    fn from(_: ClientError) -> Self {
        UnityError {}
    }
}
