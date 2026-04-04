package io.streamcrab.flink;

/**
 * Nexmark event generator matching StreamCrab's Rust SimpleRng + NexmarkGenerator exactly.
 *
 * LCG constants must match Rust's wrapping_mul/wrapping_add:
 *   state = state * 6364136223846793005 + 1442695040888963407
 * Java long arithmetic wraps naturally (same 64-bit overflow behaviour).
 *
 * Ratio: Person:Auction:Bid = 1:3:46 (total bucket = 50).
 */
public class NexmarkGenerator {

    // -----------------------------------------------------------------------
    // Data classes
    // -----------------------------------------------------------------------

    public static class Bid {
        public long auction;
        public long bidder;
        public long price;
        public String channel;
        public long timestamp;

        public Bid(long auction, long bidder, long price, String channel, long timestamp) {
            this.auction = auction;
            this.bidder = bidder;
            this.price = price;
            this.channel = channel;
            this.timestamp = timestamp;
        }
    }

    public static class Person {
        public long id;
        public String name;
        public String email;
        public String creditCard;
        public String city;
        public String state;
        public long timestamp;

        public Person(long id, String name, String email, String creditCard,
                      String city, String state, long timestamp) {
            this.id = id;
            this.name = name;
            this.email = email;
            this.creditCard = creditCard;
            this.city = city;
            this.state = state;
            this.timestamp = timestamp;
        }
    }

    public static class Auction {
        public long id;
        public long seller;
        public long category;
        public long initialBid;
        public long expires;
        public long timestamp;

        public Auction(long id, long seller, long category, long initialBid,
                       long expires, long timestamp) {
            this.id = id;
            this.seller = seller;
            this.category = category;
            this.initialBid = initialBid;
            this.expires = expires;
            this.timestamp = timestamp;
        }
    }

    public enum EventType { PERSON, AUCTION, BID }

    public static class NexmarkEvent {
        public final EventType type;
        public final Person person;
        public final Auction auction;
        public final Bid bid;

        private NexmarkEvent(Person p)  { type = EventType.PERSON;  person = p; auction = null; bid = null; }
        private NexmarkEvent(Auction a) { type = EventType.AUCTION; person = null; auction = a; bid = null; }
        private NexmarkEvent(Bid b)     { type = EventType.BID;     person = null; auction = null; bid = b; }

        public static NexmarkEvent ofPerson(Person p)   { return new NexmarkEvent(p); }
        public static NexmarkEvent ofAuction(Auction a) { return new NexmarkEvent(a); }
        public static NexmarkEvent ofBid(Bid b)         { return new NexmarkEvent(b); }

        public long timestamp() {
            if (type == EventType.PERSON)  return person.timestamp;
            if (type == EventType.AUCTION) return auction.timestamp;
            return bid.timestamp;
        }
    }

    // -----------------------------------------------------------------------
    // Static data pools (must match Rust arrays exactly)
    // -----------------------------------------------------------------------

    private static final String[] FIRST_NAMES = {
        "Alice", "Bob", "Carol", "Dave", "Eve", "Frank", "Grace", "Hank",
        "Ivy", "Jack", "Karen", "Leo", "Mia", "Noah", "Olivia", "Paul",
        "Quinn", "Rose", "Sam", "Tina"
    };

    private static final String[] LAST_NAMES = {
        "Smith", "Jones", "Williams", "Brown", "Davis", "Miller", "Wilson",
        "Moore", "Taylor", "Anderson", "Thomas", "Jackson", "White", "Harris",
        "Martin", "Thompson", "Garcia", "Martinez", "Robinson", "Clark"
    };

    private static final String[] CITIES = {
        "Springfield", "Portland", "Austin", "Denver", "Phoenix", "Seattle",
        "Boston", "Atlanta", "Chicago", "Dallas", "Houston", "Miami",
        "Nashville", "Orlando", "Detroit"
    };

    private static final String[] STATES = {
        "CA", "OR", "TX", "CO", "AZ", "WA", "MA", "GA", "IL", "NY",
        "FL", "TN", "MI", "OH", "PA"
    };

    private static final String[] CHANNELS = { "web", "mobile", "app", "api", "partner" };

    private static final long NUM_CATEGORIES = 20L;

    // -----------------------------------------------------------------------
    // Config
    // -----------------------------------------------------------------------

    public static class Config {
        public long eventsPerSec    = 0;
        public long totalEvents     = 0;
        public int  personRatio     = 1;
        public int  auctionRatio    = 3;
        public int  bidRatio        = 46;
        public long maxOutOfOrderMs = 0;
        public long numActivePersons  = 1_000L;
        public long numActiveAuctions = 10_000L;
        public long seed            = 12_345L;
    }

    // -----------------------------------------------------------------------
    // LCG RNG — matches Rust SimpleRng exactly
    // -----------------------------------------------------------------------

    private long rngState;

    /** Advance LCG and return new state (unsigned 64-bit, same as Rust u64). */
    private long nextU64() {
        // Java long wraps on overflow, matching Rust wrapping_mul / wrapping_add.
        rngState = rngState * 6364136223846793005L + 1442695040888963407L;
        return rngState;
    }

    /**
     * Return a value in [0, max). Matches Rust: self.next_u64() % max.
     * Uses Long.divideUnsigned to handle large unsigned values correctly.
     */
    private long nextRange(long max) {
        if (max == 0) return 0;
        // nextU64() treated as unsigned; Java remainder on signed longs works
        // correctly here because the Rust code also treats it as a modulo of
        // two u64 values. Long.remainderUnsigned ensures identical results.
        return Long.remainderUnsigned(nextU64(), max);
    }

    // -----------------------------------------------------------------------
    // Generator state
    // -----------------------------------------------------------------------

    private final Config config;
    private long eventsGenerated;
    private long currentTimestamp;
    private long nextPersonId;
    private long nextAuctionId;
    private final long totalRatio;

    public NexmarkGenerator(Config config) {
        this.config = config;
        this.rngState = config.seed;
        this.eventsGenerated = 0;
        this.currentTimestamp = 0;
        this.nextPersonId = 1;
        this.nextAuctionId = 1;
        this.totalRatio = config.personRatio + config.auctionRatio + config.bidRatio;
    }

    // -----------------------------------------------------------------------
    // Private helpers
    // -----------------------------------------------------------------------

    private String pickName() {
        String first = FIRST_NAMES[(int) nextRange(FIRST_NAMES.length)];
        String last  = LAST_NAMES[(int) nextRange(LAST_NAMES.length)];
        return first + " " + last;
    }

    private String pickCity() {
        return CITIES[(int) nextRange(CITIES.length)];
    }

    private String pickState() {
        return STATES[(int) nextRange(STATES.length)];
    }

    private String pickChannel() {
        return CHANNELS[(int) nextRange(CHANNELS.length)];
    }

    private String pickEmail(String name) {
        long domainIdx = nextRange(4);
        String domain;
        if      (domainIdx == 0) domain = "gmail.com";
        else if (domainIdx == 1) domain = "yahoo.com";
        else if (domainIdx == 2) domain = "outlook.com";
        else                     domain = "example.com";
        String slug = name.toLowerCase().replace(' ', '.');
        return slug + "@" + domain;
    }

    private String pickCreditCard() {
        long a = nextRange(9000) + 1000;
        long b = nextRange(9000) + 1000;
        long c = nextRange(9000) + 1000;
        long d = nextRange(9000) + 1000;
        return String.format("%04d-%04d-%04d-%04d", a, b, c, d);
    }

    /** Advance base timestamp by 1ms per event, add optional jitter. */
    private long nextTimestamp() {
        currentTimestamp += 1;
        long jitter = 0;
        if (config.maxOutOfOrderMs > 0) {
            long delay = nextRange(config.maxOutOfOrderMs + 1);
            jitter = delay - (config.maxOutOfOrderMs / 2);
        }
        return currentTimestamp + jitter;
    }

    private NexmarkEvent generatePerson() {
        long id = nextPersonId++;
        String name       = pickName();
        String email      = pickEmail(name);
        String creditCard = pickCreditCard();
        String city       = pickCity();
        String state      = pickState();
        long   timestamp  = nextTimestamp();
        return NexmarkEvent.ofPerson(new Person(id, name, email, creditCard, city, state, timestamp));
    }

    private NexmarkEvent generateAuction() {
        long id = nextAuctionId++;
        long sellerPool = Math.max(nextPersonId, 1);
        long seller = nextRange(Math.min(sellerPool, config.numActivePersons)) + 1;
        seller = Math.min(seller, sellerPool - 1);
        seller = Math.max(seller, 1);
        long category   = nextRange(NUM_CATEGORIES) + 1;
        long initialBid = nextRange(990) + 10;
        long timestamp  = nextTimestamp();
        long expires    = timestamp + (nextRange(10_000) + 1_000);
        return NexmarkEvent.ofAuction(new Auction(id, seller, category, initialBid, expires, timestamp));
    }

    private NexmarkEvent generateBid() {
        long auctionPool = Math.max(nextAuctionId, 1);
        long auction = nextRange(Math.min(auctionPool, config.numActiveAuctions)) + 1;
        auction = Math.min(auction, auctionPool - 1);
        auction = Math.max(auction, 1);
        long personPool = Math.max(nextPersonId, 1);
        long bidder = nextRange(Math.min(personPool, config.numActivePersons)) + 1;
        bidder = Math.min(bidder, personPool - 1);
        bidder = Math.max(bidder, 1);
        long price     = nextRange(9_900) + 100;
        String channel = pickChannel();
        long timestamp = nextTimestamp();
        return NexmarkEvent.ofBid(new Bid(auction, bidder, price, channel, timestamp));
    }

    private EventType eventTypeFor(long seq) {
        long bucket = seq % totalRatio;
        if (bucket < config.personRatio) return EventType.PERSON;
        if (bucket < config.personRatio + config.auctionRatio) return EventType.AUCTION;
        return EventType.BID;
    }

    // -----------------------------------------------------------------------
    // Public API
    // -----------------------------------------------------------------------

    /** Returns the next event, or null when totalEvents is reached. */
    public NexmarkEvent next() {
        if (config.totalEvents > 0 && eventsGenerated >= config.totalEvents) {
            return null;
        }
        long seq = eventsGenerated;
        NexmarkEvent event;
        switch (eventTypeFor(seq)) {
            case PERSON:  event = generatePerson();  break;
            case AUCTION: event = generateAuction(); break;
            default:      event = generateBid();     break;
        }
        eventsGenerated++;
        return event;
    }

    /** Convenience: generate all events into an array (requires totalEvents > 0). */
    public NexmarkEvent[] generateAll() {
        if (config.totalEvents <= 0) throw new IllegalStateException("totalEvents must be > 0");
        NexmarkEvent[] events = new NexmarkEvent[(int) config.totalEvents];
        for (int i = 0; i < events.length; i++) {
            events[i] = next();
        }
        return events;
    }
}
