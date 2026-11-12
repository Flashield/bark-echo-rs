//! The error face of the Device/Token plane mirrors protocol §3.2's
//! error table byte-for-byte — these Display strings are what the daemon
//! surfaces to users, so the protocol doc is the source of truth here.

use echo_core::server::ServerError;

#[test]
fn typed_errors_display_the_protocol_3_2_messages() {
    // Expected literals transcribed from bark-server-rs docs/protocol.md
    // §3.2's error table (auth-ordered: required → mismatch → not found).
    assert_eq!(
        ServerError::DeviceKeyRequired.to_string(),
        "device key required"
    );
    assert_eq!(
        ServerError::DeviceKeyMismatch.to_string(),
        "device key mismatch"
    );
    assert_eq!(ServerError::DeviceNotFound.to_string(), "device not found");
    assert_eq!(
        ServerError::StreamTokenNotFound.to_string(),
        "stream token not found"
    );
    assert_eq!(
        ServerError::StreamTokenAlreadyRevoked.to_string(),
        "stream token already revoked"
    );
}
