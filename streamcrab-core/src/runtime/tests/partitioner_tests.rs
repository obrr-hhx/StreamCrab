use super::*;

#[derive(Debug, Clone)]
#[allow(dead_code)]
struct Record {
    user_id: String,
    value: i32,
}

#[test]
fn test_hash_partitioner_same_key_same_partition() {
    let partitioner = HashPartitioner::new(|r: &Record| r.user_id.clone());

    let rec1 = Record {
        user_id: "user_1".to_string(),
        value: 100,
    };
    let rec2 = Record {
        user_id: "user_1".to_string(),
        value: 200,
    };

    let p1 = partitioner.partition(&rec1, 4);
    let p2 = partitioner.partition(&rec2, 4);

    // Same key should go to same partition
    assert_eq!(p1, p2);
}

#[test]
fn test_hash_partitioner_distribution() {
    let partitioner = HashPartitioner::new(|r: &Record| r.user_id.clone());

    let mut counts = vec![0; 4];
    for i in 0..1000 {
        let rec = Record {
            user_id: format!("user_{}", i),
            value: i,
        };
        let partition = partitioner.partition(&rec, 4);
        counts[partition] += 1;
    }

    // Check that distribution is reasonably balanced
    // Each partition should get roughly 250 records (1000 / 4)
    for count in counts {
        assert!(
            count > 200 && count < 300,
            "Unbalanced distribution: {}",
            count
        );
    }
}

#[test]
fn test_hash_partitioner_within_bounds() {
    let partitioner = HashPartitioner::new(|r: &Record| r.user_id.clone());

    for i in 0..100 {
        let rec = Record {
            user_id: format!("user_{}", i),
            value: i,
        };
        let partition = partitioner.partition(&rec, 8);
        assert!(partition < 8);
    }
}

#[test]
fn test_round_robin_partitioner() {
    let partitioner = RoundRobinPartitioner::new();

    let rec = Record {
        user_id: "user_1".to_string(),
        value: 100,
    };

    // Should cycle through partitions
    assert_eq!(partitioner.partition(&rec, 4), 0);
    assert_eq!(partitioner.partition(&rec, 4), 1);
    assert_eq!(partitioner.partition(&rec, 4), 2);
    assert_eq!(partitioner.partition(&rec, 4), 3);
    assert_eq!(partitioner.partition(&rec, 4), 0);
}
