use serde::{Deserialize, Serialize};

/// Nexmark Person event.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Person {
    pub id: u64,
    pub name: String,
    pub email: String,
    pub credit_card: String,
    pub city: String,
    pub state: String,
    pub timestamp: i64,
}

/// Nexmark Auction event.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Auction {
    pub id: u64,
    pub seller: u64,
    pub category: u64,
    pub initial_bid: u64,
    pub expires: i64,
    pub timestamp: i64,
}

/// Nexmark Bid event.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Bid {
    pub auction: u64,
    pub bidder: u64,
    pub price: u64,
    pub channel: String,
    pub timestamp: i64,
}

/// A Nexmark event is one of Person, Auction, or Bid.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum NexmarkEvent {
    Person(Person),
    Auction(Auction),
    Bid(Bid),
}

impl NexmarkEvent {
    pub fn timestamp(&self) -> i64 {
        match self {
            NexmarkEvent::Person(p) => p.timestamp,
            NexmarkEvent::Auction(a) => a.timestamp,
            NexmarkEvent::Bid(b) => b.timestamp,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_event_types_serialize() {
        let person = Person {
            id: 1,
            name: "Alice".to_string(),
            email: "alice@example.com".to_string(),
            credit_card: "1234-5678-9012-3456".to_string(),
            city: "Springfield".to_string(),
            state: "CA".to_string(),
            timestamp: 1_000_000,
        };
        let encoded = bincode::serialize(&person).expect("serialize person");
        let decoded: Person = bincode::deserialize(&encoded).expect("deserialize person");
        assert_eq!(person, decoded);

        let auction = Auction {
            id: 42,
            seller: 1,
            category: 5,
            initial_bid: 100,
            expires: 2_000_000,
            timestamp: 1_000_000,
        };
        let encoded = bincode::serialize(&auction).expect("serialize auction");
        let decoded: Auction = bincode::deserialize(&encoded).expect("deserialize auction");
        assert_eq!(auction, decoded);

        let bid = Bid {
            auction: 42,
            bidder: 2,
            price: 150,
            channel: "web".to_string(),
            timestamp: 1_100_000,
        };
        let encoded = bincode::serialize(&bid).expect("serialize bid");
        let decoded: Bid = bincode::deserialize(&encoded).expect("deserialize bid");
        assert_eq!(bid, decoded);
    }

    #[test]
    fn test_nexmark_event_timestamp() {
        let person = NexmarkEvent::Person(Person {
            id: 1,
            name: "Bob".to_string(),
            email: "bob@test.com".to_string(),
            credit_card: "0000-0000-0000-0000".to_string(),
            city: "Portland".to_string(),
            state: "OR".to_string(),
            timestamp: 999,
        });
        assert_eq!(person.timestamp(), 999);

        let auction = NexmarkEvent::Auction(Auction {
            id: 10,
            seller: 1,
            category: 2,
            initial_bid: 50,
            expires: 5000,
            timestamp: 1234,
        });
        assert_eq!(auction.timestamp(), 1234);

        let bid = NexmarkEvent::Bid(Bid {
            auction: 10,
            bidder: 3,
            price: 75,
            channel: "mobile".to_string(),
            timestamp: 5678,
        });
        assert_eq!(bid.timestamp(), 5678);
    }
}
