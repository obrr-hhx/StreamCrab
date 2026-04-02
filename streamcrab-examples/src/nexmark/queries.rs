use std::collections::HashMap;

use super::event::{Auction, Bid, Person};

/// Q1: SELECT auction, price * 0.908 FROM Bid
/// Converts bid prices from dollars to euros.
pub fn q1_currency_conversion(bids: &[Bid]) -> Vec<(u64, f64)> {
    bids.iter()
        .map(|b| (b.auction, b.price as f64 * 0.908))
        .collect()
}

/// Q2: SELECT auction, price FROM Bid WHERE auction IN (1, 3, 5, 7, 9)
pub fn q2_selection(bids: &[Bid]) -> Vec<(u64, u64)> {
    const HOT_AUCTIONS: &[u64] = &[1, 3, 5, 7, 9];
    bids.iter()
        .filter(|b| HOT_AUCTIONS.contains(&b.auction))
        .map(|b| (b.auction, b.price))
        .collect()
}

/// Q5: Count bids per auction in sliding windows.
/// Window size and slide are specified in milliseconds.
/// Returns (window_end, auction_id, count) triples sorted by (window_end, auction_id).
pub fn q5_hot_items(bids: &[Bid], window_size_ms: i64, slide_ms: i64) -> Vec<(i64, u64, u64)> {
    if bids.is_empty() {
        return vec![];
    }
    let min_ts = bids.iter().map(|b| b.timestamp).min().unwrap();
    let max_ts = bids.iter().map(|b| b.timestamp).max().unwrap();

    let mut results = Vec::new();
    let first_window_start = (min_ts / slide_ms) * slide_ms;

    let mut window_start = first_window_start;
    while window_start <= max_ts {
        let window_end = window_start + window_size_ms;
        let mut counts: HashMap<u64, u64> = HashMap::new();

        for bid in bids {
            if bid.timestamp >= window_start && bid.timestamp < window_end {
                *counts.entry(bid.auction).or_insert(0) += 1;
            }
        }

        for (auction, count) in &counts {
            results.push((window_end, *auction, *count));
        }

        window_start += slide_ms;
    }

    results.sort_by_key(|&(end, auction, _)| (end, auction));
    results
}

/// Q8: Join Person and Auction in tumbling windows.
/// SELECT P.id, P.name, A.id FROM Person, Auction (same tumbling window)
/// Returns (person_id, person_name, auction_id) triples.
pub fn q8_local_item_suggestion(
    persons: &[Person],
    auctions: &[Auction],
    window_size_ms: i64,
) -> Vec<(u64, String, u64)> {
    if persons.is_empty() || auctions.is_empty() {
        return vec![];
    }

    // Group persons by tumbling window bucket
    let mut persons_by_window: HashMap<i64, Vec<&Person>> = HashMap::new();
    for person in persons {
        let bucket = (person.timestamp / window_size_ms) * window_size_ms;
        persons_by_window.entry(bucket).or_default().push(person);
    }

    // Group auctions by tumbling window bucket
    let mut auctions_by_window: HashMap<i64, Vec<&Auction>> = HashMap::new();
    for auction in auctions {
        let bucket = (auction.timestamp / window_size_ms) * window_size_ms;
        auctions_by_window.entry(bucket).or_default().push(auction);
    }

    let mut results = Vec::new();
    for (bucket, ps) in &persons_by_window {
        if let Some(as_) = auctions_by_window.get(bucket) {
            for person in ps {
                for auction in as_ {
                    results.push((person.id, person.name.clone(), auction.id));
                }
            }
        }
    }

    results.sort_by_key(|&(pid, _, aid)| (pid, aid));
    results
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_bid(auction: u64, price: u64, timestamp: i64) -> Bid {
        Bid {
            auction,
            bidder: 1,
            price,
            channel: "web".to_string(),
            timestamp,
        }
    }

    fn make_person(id: u64, name: &str, timestamp: i64) -> Person {
        Person {
            id,
            name: name.to_string(),
            email: format!("{}@test.com", name),
            credit_card: "0000-0000-0000-0000".to_string(),
            city: "Springfield".to_string(),
            state: "CA".to_string(),
            timestamp,
        }
    }

    fn make_auction(id: u64, timestamp: i64) -> Auction {
        Auction {
            id,
            seller: 1,
            category: 1,
            initial_bid: 100,
            expires: timestamp + 10_000,
            timestamp,
        }
    }

    // Q1 tests

    #[test]
    fn test_q1_currency_conversion() {
        let bids = vec![
            make_bid(1, 100, 1000),
            make_bid(2, 200, 2000),
            make_bid(3, 50, 3000),
        ];
        let result = q1_currency_conversion(&bids);
        assert_eq!(result.len(), 3);
        assert_eq!(result[0], (1, 100.0 * 0.908));
        assert_eq!(result[1], (2, 200.0 * 0.908));
        assert_eq!(result[2], (3, 50.0 * 0.908));
    }

    #[test]
    fn test_q1_empty() {
        let result = q1_currency_conversion(&[]);
        assert!(result.is_empty());
    }

    // Q2 tests

    #[test]
    fn test_q2_selection_filters() {
        let bids = vec![
            make_bid(1, 100, 1000), // in HOT_AUCTIONS
            make_bid(2, 200, 2000), // not in HOT_AUCTIONS
            make_bid(3, 300, 3000), // in HOT_AUCTIONS
            make_bid(4, 400, 4000), // not in HOT_AUCTIONS
            make_bid(5, 500, 5000), // in HOT_AUCTIONS
            make_bid(9, 900, 9000), // in HOT_AUCTIONS
        ];
        let result = q2_selection(&bids);
        assert_eq!(result.len(), 4);
        assert!(result.contains(&(1, 100)));
        assert!(result.contains(&(3, 300)));
        assert!(result.contains(&(5, 500)));
        assert!(result.contains(&(9, 900)));
        // auction 2 and 4 should not be present
        assert!(!result.iter().any(|(a, _)| *a == 2 || *a == 4));
    }

    #[test]
    fn test_q2_no_match() {
        let bids = vec![make_bid(10, 100, 1000), make_bid(20, 200, 2000)];
        let result = q2_selection(&bids);
        assert!(result.is_empty());
    }

    // Q5 tests

    #[test]
    fn test_q5_single_window() {
        // All bids in [0, 60_000)
        let bids = vec![
            make_bid(1, 100, 1000),
            make_bid(1, 200, 2000),
            make_bid(2, 300, 3000),
        ];
        let result = q5_hot_items(&bids, 60_000, 10_000);
        // Find the entry for auction=1 in the window containing ts=1000..3000
        let auction1: Vec<_> = result.iter().filter(|(_, a, _)| *a == 1).collect();
        let auction2: Vec<_> = result.iter().filter(|(_, a, _)| *a == 2).collect();
        // auction 1 should have count 2 in at least one window
        assert!(auction1.iter().any(|(_, _, c)| *c == 2));
        // auction 2 should have count 1 in at least one window
        assert!(auction2.iter().any(|(_, _, c)| *c == 1));
    }

    #[test]
    fn test_q5_multiple_windows() {
        // Bids span two non-overlapping slide windows
        // window_size=60_000, slide=60_000 (tumbling for simplicity)
        let bids = vec![
            make_bid(1, 100, 10_000), // window starting at 0
            make_bid(1, 200, 20_000), // window starting at 0
            make_bid(2, 300, 70_000), // window starting at 60_000
        ];
        let result = q5_hot_items(&bids, 60_000, 60_000);
        // Window [0, 60_000): auction 1 count=2
        let w1_a1: Vec<_> = result
            .iter()
            .filter(|(end, a, _)| *end == 60_000 && *a == 1)
            .collect();
        assert_eq!(w1_a1.len(), 1);
        assert_eq!(w1_a1[0].2, 2);

        // Window [60_000, 120_000): auction 2 count=1
        let w2_a2: Vec<_> = result
            .iter()
            .filter(|(end, a, _)| *end == 120_000 && *a == 2)
            .collect();
        assert_eq!(w2_a2.len(), 1);
        assert_eq!(w2_a2[0].2, 1);
    }

    #[test]
    fn test_q5_empty() {
        let result = q5_hot_items(&[], 60_000, 10_000);
        assert!(result.is_empty());
    }

    // Q8 tests

    #[test]
    fn test_q8_join_in_window() {
        // Both person and auction in window [0, 10_000)
        let persons = vec![make_person(1, "Alice", 1000)];
        let auctions = vec![make_auction(42, 2000)];
        let result = q8_local_item_suggestion(&persons, &auctions, 10_000);
        assert_eq!(result.len(), 1);
        assert_eq!(result[0], (1, "Alice".to_string(), 42));
    }

    #[test]
    fn test_q8_different_windows() {
        // Person in window [0, 10_000), auction in window [10_000, 20_000)
        let persons = vec![make_person(1, "Alice", 1000)];
        let auctions = vec![make_auction(42, 10_000)];
        let result = q8_local_item_suggestion(&persons, &auctions, 10_000);
        assert!(result.is_empty());
    }

    #[test]
    fn test_q8_empty() {
        let persons: Vec<Person> = vec![];
        let auctions: Vec<Auction> = vec![];
        let result = q8_local_item_suggestion(&persons, &auctions, 10_000);
        assert!(result.is_empty());

        let persons = vec![make_person(1, "Alice", 1000)];
        let result = q8_local_item_suggestion(&persons, &auctions, 10_000);
        assert!(result.is_empty());
    }
}
