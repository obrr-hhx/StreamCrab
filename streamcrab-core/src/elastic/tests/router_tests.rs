use crate::elastic::{KeyRouter, PartitionRouter};
use crate::task::{TaskId, VertexId};
use std::collections::HashMap;

fn tasks(n: usize) -> Vec<TaskId> {
    (0..n)
        .map(|i| TaskId::new(VertexId::new(1), i))
        .collect::<Vec<_>>()
}

#[test]
fn test_key_router_routes_keys() {
    let router = KeyRouter::new(1, &tasks(4), 64);
    let target = router.route(b"user-1");
    assert!(target.is_some());
}

#[test]
fn test_consistent_hash_migration_ratio_is_reasonable() {
    let old_tasks = tasks(4);
    let new_tasks = tasks(5);
    let old = KeyRouter::new(1, &old_tasks, 128);
    let new = KeyRouter::new(2, &new_tasks, 128);

    let mut changed = 0usize;
    let total = 5000usize;
    for i in 0..total {
        let key = format!("key-{i}");
        if old.route(key.as_bytes()) != new.route(key.as_bytes()) {
            changed += 1;
        }
    }
    let ratio = changed as f64 / total as f64;
    assert!(ratio < 0.40, "too many keys moved: ratio={ratio}");
}

#[test]
fn test_partition_router_atomic_swap() {
    let initial = KeyRouter::new(1, &tasks(2), 32);
    let router = PartitionRouter::new(initial);
    let before = router.current_version();
    router.swap(KeyRouter::new(2, &tasks(3), 32));
    let after = router.current_version();
    assert_eq!(before, 1);
    assert_eq!(after, 2);
}

#[test]
fn test_router_spread_for_basic_distribution() {
    let router = KeyRouter::new(1, &tasks(4), 128);
    let mut counts: HashMap<TaskId, usize> = HashMap::new();
    for i in 0..10_000usize {
        let key = format!("key-{i}");
        let task = router.route(key.as_bytes()).unwrap();
        *counts.entry(task).or_default() += 1;
    }
    assert_eq!(counts.len(), 4);
}
