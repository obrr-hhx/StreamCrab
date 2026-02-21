use super::*;
use crate::state::{ListStateHandle, MapStateHandle, ValueStateHandle};

#[test]
fn test_value_state() {
    let mut backend = HashMapStateBackend::new();
    backend.set_current_key(b"user_1".to_vec());

    // Create handle (no borrowing)
    let count = ValueStateHandle::<i32>::new("count");

    assert_eq!(count.get(&backend).unwrap(), None);

    count.put(&mut backend, 42).unwrap();
    assert_eq!(count.get(&backend).unwrap(), Some(42));

    count.put(&mut backend, 100).unwrap();
    assert_eq!(count.get(&backend).unwrap(), Some(100));

    count.clear(&mut backend).unwrap();
    assert_eq!(count.get(&backend).unwrap(), None);
}

#[test]
fn test_value_state_different_keys() {
    let mut backend = HashMapStateBackend::new();

    // Create handle once
    let count = ValueStateHandle::<i32>::new("count");

    // Set key and put value for user_1
    backend.set_current_key(b"user_1".to_vec());
    count.put(&mut backend, 10).unwrap();

    // Set key and put value for user_2
    backend.set_current_key(b"user_2".to_vec());
    count.put(&mut backend, 20).unwrap();

    // Verify user_1's value
    backend.set_current_key(b"user_1".to_vec());
    assert_eq!(count.get(&backend).unwrap(), Some(10));

    // Verify user_2's value
    backend.set_current_key(b"user_2".to_vec());
    assert_eq!(count.get(&backend).unwrap(), Some(20));
}

#[test]
fn test_list_state() {
    let mut backend = HashMapStateBackend::new();
    backend.set_current_key(b"key1".to_vec());

    // Create handle (no borrowing)
    let events = ListStateHandle::<String>::new("events");

    assert_eq!(events.get(&backend).unwrap(), Vec::<String>::new());

    events.add(&mut backend, "event1".to_string()).unwrap();
    events.add(&mut backend, "event2".to_string()).unwrap();
    assert_eq!(
        events.get(&backend).unwrap(),
        vec!["event1".to_string(), "event2".to_string()]
    );

    events.clear(&mut backend).unwrap();
    assert_eq!(events.get(&backend).unwrap(), Vec::<String>::new());
}

#[test]
fn test_map_state() {
    let mut backend = HashMapStateBackend::new();
    backend.set_current_key(b"user_1".to_vec());

    // Create handle (no borrowing)
    let metrics = MapStateHandle::<String, i32>::new("metrics");

    assert_eq!(metrics.get(&backend, &"clicks".to_string()).unwrap(), None);

    metrics.put(&mut backend, "clicks".to_string(), 10).unwrap();
    metrics.put(&mut backend, "views".to_string(), 100).unwrap();
    assert_eq!(
        metrics.get(&backend, &"clicks".to_string()).unwrap(),
        Some(10)
    );
    assert_eq!(
        metrics.get(&backend, &"views".to_string()).unwrap(),
        Some(100)
    );

    let removed = metrics.remove(&mut backend, &"clicks".to_string()).unwrap();
    assert_eq!(removed, Some(10));
    assert_eq!(metrics.get(&backend, &"clicks".to_string()).unwrap(), None);

    metrics.clear(&mut backend).unwrap();
    assert_eq!(metrics.get(&backend, &"views".to_string()).unwrap(), None);
}

#[test]
fn test_snapshot_restore() {
    let mut backend = HashMapStateBackend::new();

    // Create handles
    let count = ValueStateHandle::<i32>::new("count");
    let events = ListStateHandle::<String>::new("events");
    let metrics = MapStateHandle::<String, i32>::new("metrics");

    // Put values for two keys
    backend.set_current_key(b"user_1".to_vec());
    count.put(&mut backend, 42).unwrap();

    backend.set_current_key(b"user_2".to_vec());
    count.put(&mut backend, 100).unwrap();

    backend.set_current_key(b"user_1".to_vec());
    events.add(&mut backend, "e1".to_string()).unwrap();
    events.add(&mut backend, "e2".to_string()).unwrap();
    metrics.put(&mut backend, "clicks".to_string(), 7).unwrap();

    // Snapshot
    let snapshot = backend.snapshot().unwrap();

    // Create new backend and restore
    let mut new_backend = HashMapStateBackend::new();
    new_backend.restore(&snapshot).unwrap();

    // Verify restored values
    new_backend.set_current_key(b"user_1".to_vec());
    assert_eq!(count.get(&new_backend).unwrap(), Some(42));
    assert_eq!(
        events.get(&new_backend).unwrap(),
        vec!["e1".to_string(), "e2".to_string()]
    );
    assert_eq!(
        metrics.get(&new_backend, &"clicks".to_string()).unwrap(),
        Some(7)
    );

    new_backend.set_current_key(b"user_2".to_vec());
    assert_eq!(count.get(&new_backend).unwrap(), Some(100));
}
