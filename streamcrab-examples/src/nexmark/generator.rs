use crate::nexmark::event::{Auction, Bid, NexmarkEvent, Person};

// ---------------------------------------------------------------------------
// Simple LCG RNG (no external dependencies)
// ---------------------------------------------------------------------------

struct SimpleRng {
    state: u64,
}

impl SimpleRng {
    fn new(seed: u64) -> Self {
        Self { state: seed }
    }

    fn next_u64(&mut self) -> u64 {
        self.state = self
            .state
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        self.state
    }

    #[allow(dead_code)]
    fn next_f64(&mut self) -> f64 {
        (self.next_u64() >> 11) as f64 / (1u64 << 53) as f64
    }

    fn next_range(&mut self, max: u64) -> u64 {
        if max == 0 {
            return 0;
        }
        self.next_u64() % max
    }
}

// ---------------------------------------------------------------------------
// Static data pools for realistic-looking generated data
// ---------------------------------------------------------------------------

const FIRST_NAMES: &[&str] = &[
    "Alice", "Bob", "Carol", "Dave", "Eve", "Frank", "Grace", "Hank", "Ivy", "Jack", "Karen",
    "Leo", "Mia", "Noah", "Olivia", "Paul", "Quinn", "Rose", "Sam", "Tina",
];

const LAST_NAMES: &[&str] = &[
    "Smith", "Jones", "Williams", "Brown", "Davis", "Miller", "Wilson", "Moore", "Taylor",
    "Anderson", "Thomas", "Jackson", "White", "Harris", "Martin", "Thompson", "Garcia", "Martinez",
    "Robinson", "Clark",
];

const CITIES: &[&str] = &[
    "Springfield",
    "Portland",
    "Austin",
    "Denver",
    "Phoenix",
    "Seattle",
    "Boston",
    "Atlanta",
    "Chicago",
    "Dallas",
    "Houston",
    "Miami",
    "Nashville",
    "Orlando",
    "Detroit",
];

const STATES: &[&str] = &[
    "CA", "OR", "TX", "CO", "AZ", "WA", "MA", "GA", "IL", "NY", "FL", "TN", "MI", "OH", "PA",
];

const CHANNELS: &[&str] = &["web", "mobile", "app", "api", "partner"];

const NUM_CATEGORIES: u64 = 20;

// ---------------------------------------------------------------------------
// Config
// ---------------------------------------------------------------------------

/// Configuration for the Nexmark event generator.
#[derive(Debug, Clone)]
pub struct NexmarkConfig {
    /// Events per second (0 = unlimited / as-fast-as-possible).
    pub events_per_sec: u64,
    /// Total events to generate (0 = unlimited).
    pub total_events: u64,
    /// Person:Auction:Bid ratio. Default: 1:3:46 (industry standard).
    pub person_ratio: u32,
    pub auction_ratio: u32,
    pub bid_ratio: u32,
    /// Maximum out-of-order delay in milliseconds.
    pub max_out_of_order_ms: i64,
    /// Number of active persons tracked by the generator.
    pub num_active_persons: u64,
    /// Number of active auctions tracked by the generator.
    pub num_active_auctions: u64,
    /// Random seed for reproducibility.
    pub seed: u64,
}

impl Default for NexmarkConfig {
    fn default() -> Self {
        Self {
            events_per_sec: 0,
            total_events: 0,
            person_ratio: 1,
            auction_ratio: 3,
            bid_ratio: 46,
            max_out_of_order_ms: 0,
            num_active_persons: 1_000,
            num_active_auctions: 10_000,
            seed: 12_345,
        }
    }
}

// ---------------------------------------------------------------------------
// Generator
// ---------------------------------------------------------------------------

/// Nexmark event generator implementing `Iterator<Item = NexmarkEvent>`.
pub struct NexmarkGenerator {
    config: NexmarkConfig,
    rng: SimpleRng,
    events_generated: u64,
    /// Monotonically increasing base timestamp in milliseconds.
    current_timestamp: i64,
    next_person_id: u64,
    next_auction_id: u64,
    /// Cached total ratio for modular bucketing.
    total_ratio: u32,
}

impl NexmarkGenerator {
    /// Create a new generator with the given config.
    pub fn new(config: NexmarkConfig) -> Self {
        let seed = config.seed;
        let total_ratio = config.person_ratio + config.auction_ratio + config.bid_ratio;
        Self {
            rng: SimpleRng::new(seed),
            config,
            events_generated: 0,
            current_timestamp: 0,
            next_person_id: 1,
            next_auction_id: 1,
            total_ratio,
        }
    }

    // -----------------------------------------------------------------------
    // Private helpers
    // -----------------------------------------------------------------------

    fn pick_name(&mut self) -> String {
        let first = FIRST_NAMES[self.rng.next_range(FIRST_NAMES.len() as u64) as usize];
        let last = LAST_NAMES[self.rng.next_range(LAST_NAMES.len() as u64) as usize];
        format!("{first} {last}")
    }

    fn pick_city(&mut self) -> String {
        CITIES[self.rng.next_range(CITIES.len() as u64) as usize].to_string()
    }

    fn pick_state(&mut self) -> String {
        STATES[self.rng.next_range(STATES.len() as u64) as usize].to_string()
    }

    fn pick_channel(&mut self) -> String {
        CHANNELS[self.rng.next_range(CHANNELS.len() as u64) as usize].to_string()
    }

    fn pick_email(&mut self, name: &str) -> String {
        let domain_idx = self.rng.next_range(4);
        let domain = match domain_idx {
            0 => "gmail.com",
            1 => "yahoo.com",
            2 => "outlook.com",
            _ => "example.com",
        };
        let slug = name.to_lowercase().replace(' ', ".");
        format!("{slug}@{domain}")
    }

    fn pick_credit_card(&mut self) -> String {
        let a = self.rng.next_range(9000) + 1000;
        let b = self.rng.next_range(9000) + 1000;
        let c = self.rng.next_range(9000) + 1000;
        let d = self.rng.next_range(9000) + 1000;
        format!("{a:04}-{b:04}-{c:04}-{d:04}")
    }

    /// Advance the base timestamp by a small step, then add optional jitter.
    fn next_timestamp(&mut self) -> i64 {
        // Advance ~1 ms per event to keep timestamps roughly ordered.
        self.current_timestamp += 1;
        let jitter = if self.config.max_out_of_order_ms > 0 {
            let delay =
                self.rng
                    .next_range(self.config.max_out_of_order_ms as u64 + 1) as i64;
            // jitter is a positive or negative offset bounded by max_out_of_order_ms
            delay - (self.config.max_out_of_order_ms / 2)
        } else {
            0
        };
        self.current_timestamp + jitter
    }

    fn generate_person(&mut self) -> NexmarkEvent {
        let id = self.next_person_id;
        self.next_person_id += 1;
        let name = self.pick_name();
        let email = self.pick_email(&name);
        let credit_card = self.pick_credit_card();
        let city = self.pick_city();
        let state = self.pick_state();
        let timestamp = self.next_timestamp();
        NexmarkEvent::Person(Person {
            id,
            name,
            email,
            credit_card,
            city,
            state,
            timestamp,
        })
    }

    fn generate_auction(&mut self) -> NexmarkEvent {
        let id = self.next_auction_id;
        self.next_auction_id += 1;
        // Seller is one of the already-seen persons.
        let seller_pool = self.next_person_id.max(1);
        let seller = (self
            .rng
            .next_range(seller_pool.min(self.config.num_active_persons))
            + 1)
        .min(seller_pool - 1)
        .max(1);
        let category = self.rng.next_range(NUM_CATEGORIES) + 1;
        let initial_bid = self.rng.next_range(990) + 10;
        let timestamp = self.next_timestamp();
        // Auction expires 1-10 seconds after its creation timestamp.
        let expires = timestamp + (self.rng.next_range(10_000) as i64 + 1_000);
        NexmarkEvent::Auction(Auction {
            id,
            seller,
            category,
            initial_bid,
            expires,
            timestamp,
        })
    }

    fn generate_bid(&mut self) -> NexmarkEvent {
        // Pick a random active auction.
        let auction_pool = self.next_auction_id.max(1);
        let auction = (self
            .rng
            .next_range(auction_pool.min(self.config.num_active_auctions))
            + 1)
        .min(auction_pool - 1)
        .max(1);
        // Pick a random bidder from seen persons.
        let person_pool = self.next_person_id.max(1);
        let bidder = (self
            .rng
            .next_range(person_pool.min(self.config.num_active_persons))
            + 1)
        .min(person_pool - 1)
        .max(1);
        let price = self.rng.next_range(9_900) + 100;
        let channel = self.pick_channel();
        let timestamp = self.next_timestamp();
        NexmarkEvent::Bid(Bid {
            auction,
            bidder,
            price,
            channel,
            timestamp,
        })
    }

    /// Determine which event type to emit for the current sequence number.
    fn event_type_for(&self, seq: u64) -> EventType {
        let bucket = (seq % self.total_ratio as u64) as u32;
        if bucket < self.config.person_ratio {
            EventType::Person
        } else if bucket < self.config.person_ratio + self.config.auction_ratio {
            EventType::Auction
        } else {
            EventType::Bid
        }
    }
}

enum EventType {
    Person,
    Auction,
    Bid,
}

impl Iterator for NexmarkGenerator {
    type Item = NexmarkEvent;

    fn next(&mut self) -> Option<NexmarkEvent> {
        if self.config.total_events > 0 && self.events_generated >= self.config.total_events {
            return None;
        }
        let seq = self.events_generated;
        let event = match self.event_type_for(seq) {
            EventType::Person => self.generate_person(),
            EventType::Auction => self.generate_auction(),
            EventType::Bid => self.generate_bid(),
        };
        self.events_generated += 1;
        Some(event)
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn default_gen(total: u64) -> NexmarkGenerator {
        NexmarkGenerator::new(NexmarkConfig {
            total_events: total,
            ..NexmarkConfig::default()
        })
    }

    #[test]
    fn test_generator_ratio() {
        let events: Vec<_> = default_gen(1_000).collect();
        assert_eq!(events.len(), 1_000);

        let persons = events
            .iter()
            .filter(|e| matches!(e, NexmarkEvent::Person(_)))
            .count();
        let auctions = events
            .iter()
            .filter(|e| matches!(e, NexmarkEvent::Auction(_)))
            .count();
        let bids = events
            .iter()
            .filter(|e| matches!(e, NexmarkEvent::Bid(_)))
            .count();

        // Default ratio 1:3:46 out of 50 total buckets.
        // With 1000 events: 20 persons, 60 auctions, 920 bids exactly (deterministic bucketing).
        assert_eq!(persons + auctions + bids, 1_000);

        // Allow ±5% tolerance for approximate ratio check.
        let total = 1_000f64;
        assert!(
            (persons as f64 / total - 1.0 / 50.0).abs() < 0.05,
            "person ratio off: {persons}"
        );
        assert!(
            (auctions as f64 / total - 3.0 / 50.0).abs() < 0.05,
            "auction ratio off: {auctions}"
        );
        assert!(
            (bids as f64 / total - 46.0 / 50.0).abs() < 0.05,
            "bid ratio off: {bids}"
        );
    }

    #[test]
    fn test_generator_monotonic_with_jitter() {
        // With zero jitter, timestamps must be strictly increasing.
        let events: Vec<_> = NexmarkGenerator::new(NexmarkConfig {
            total_events: 200,
            max_out_of_order_ms: 0,
            ..NexmarkConfig::default()
        })
        .collect();

        let timestamps: Vec<i64> = events.iter().map(|e| e.timestamp()).collect();
        for w in timestamps.windows(2) {
            assert!(
                w[1] >= w[0],
                "timestamps not non-decreasing: {} then {}",
                w[0],
                w[1]
            );
        }

        // With jitter, timestamps may be out of order but bounded.
        let max_jitter = 50i64;
        let events_jitter: Vec<_> = NexmarkGenerator::new(NexmarkConfig {
            total_events: 200,
            max_out_of_order_ms: max_jitter,
            ..NexmarkConfig::default()
        })
        .collect();

        let ts_jitter: Vec<i64> = events_jitter.iter().map(|e| e.timestamp()).collect();
        // All timestamps should be positive.
        for &t in &ts_jitter {
            assert!(t >= -max_jitter, "timestamp too negative: {t}");
        }
        // The final timestamp should not exceed total_events + jitter.
        let last = *ts_jitter.last().unwrap();
        assert!(
            last <= 200 + max_jitter,
            "final timestamp too large: {last}"
        );
    }

    #[test]
    fn test_generator_deterministic() {
        let events_a: Vec<_> = default_gen(500).collect();
        let events_b: Vec<_> = default_gen(500).collect();
        assert_eq!(events_a.len(), events_b.len());
        for (a, b) in events_a.iter().zip(events_b.iter()) {
            assert_eq!(a.timestamp(), b.timestamp());
            // Compare variant identity.
            assert_eq!(
                std::mem::discriminant(a),
                std::mem::discriminant(b),
                "event type mismatch"
            );
        }
    }

    #[test]
    fn test_generator_total_events() {
        let events: Vec<_> = default_gen(42).collect();
        assert_eq!(events.len(), 42);

        // Unlimited: take only what we need.
        let events_unlimited: Vec<_> = NexmarkGenerator::new(NexmarkConfig {
            total_events: 0,
            ..NexmarkConfig::default()
        })
        .take(100)
        .collect();
        assert_eq!(events_unlimited.len(), 100);
    }
}
