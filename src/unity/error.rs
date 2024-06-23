use self::client::error::ClientError;

pub struct UnityError {}

impl From<ClientError> for UnityError {
    fn from(_: ClientError) -> Self {
        UnityError {}
    }
}
