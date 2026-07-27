# Redis Caching Patterns

Two production-oriented Redis patterns in ~200 lines of Java, with concurrency tests that make the invariants observable.

> Companion annotated learning repo (with phase-by-phase evolution + experiment logs): [redis-playground-annotated](https://github.com/VengadeshVK/Redis-playground)

---

## Patterns

### 1. Cached view counts — soft TTL + compute lock
`src/main/java/ViewCountCache.java`

Cache-aside for expensive `COUNT(*)` queries, with two protections:

- **Soft TTL** — a sidecar `expiryTime` key marks freshness. Stale values are still served while a background recompute runs, avoiding hard cache misses.
- **Compute lock** — `SET NX PX` collapses N concurrent stale-hits into a single recompute. Losers serve the stale value immediately (~7 ms) instead of racing to the database.

Concurrency test: 10 threads race on a pre-populated stale key.

```
[pool-1-thread-2] count=-999 took=5ms   ← loser, served stale
[pool-1-thread-6] count=-999 took=5ms   ← loser
... (9 losers total)
[pool-1-thread-8] count=-43  took=511ms ← winner, recomputed
Total: 513 ms
```

**One recompute, not ten.** Downstream DB load is 10× lower under a stampede.

### 2. Rolling-window rate limiter — atomic via Lua
`src/main/java/RateLimiter.java`

Enforces *"at most N events per window"* backed by a Redis sorted set. The naïve version (four separate Redis calls) breaks under concurrency because the check-then-add pattern isn't atomic. The Lua wrapper fixes it.

Concurrency test: 20 threads, threshold = 5.

```
Threshold: 5
Allowed:   5
Blocked:   15
```

Exactly 5 pass, 15 rejected — regardless of how tightly the threads race.

See [docs/PATTERNS.md](docs/PATTERNS.md) for the full technical write-up.

---

## Prerequisites

- Java 17+
- Redis on `localhost:6379`

```bash
# macOS
brew install redis
brew services start redis
redis-cli PING
```

## Running

```bash
# Cached view counts
sed -i '' "s/mainClass = 'RateLimiter'/mainClass = 'ViewCountCache'/" build.gradle
./gradlew run

# Rate limiter
sed -i '' "s/mainClass = 'ViewCountCache'/mainClass = 'RateLimiter'/" build.gradle
./gradlew run
```

Or edit `build.gradle` manually — the two `mainClass` values are `'ViewCountCache'` and `'RateLimiter'`.

## Structure

```
redis-patterns-showcase/
├── build.gradle
├── src/main/java/
│   ├── ViewCountCache.java   # soft TTL + compute lock
│   └── RateLimiter.java      # rolling window + Lua atomic
├── docs/
│   └── PATTERNS.md           # technical explainer
└── README.md
```

## Key concepts demonstrated

- Cache-aside with sidecar freshness timestamp
- `SET NX PX` distributed lock with auto-release on holder crash
- Sorted-set rolling window
- Atomicity via Lua `EVAL` for multi-step operations
- Connection pooling with try-with-resources discipline
- Java concurrency primitives (`ExecutorService`, `CountDownLatch`, `AtomicInteger`)

## Credits

Patterns modeled after those in the Zoho Desk backend (`com.zoho.desk.cache`, `com.zoho.desk.redis`).
