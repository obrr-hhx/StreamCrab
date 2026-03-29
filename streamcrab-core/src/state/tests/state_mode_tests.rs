use crate::state::{StateBackendKind, StateMode};

#[test]
fn test_state_mode_default_is_local() {
    assert_eq!(StateMode::default(), StateMode::Local);
}

#[test]
fn test_state_backend_kind_values() {
    assert_eq!(StateBackendKind::LocalHashMap as u8, 0);
    assert_eq!(StateBackendKind::Tiered as u8, 1);
}
