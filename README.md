# Redis Caching Patterns

Three production-oriented Redis patterns in ~250 lines of Java, with concurrency tests that make the invariants observable.

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

### 3. Cross-server cache invalidation — Redis Pub/Sub
`src/main/java/CacheInvalidator.java`

When any app server changes user data, it PUBLISHes an event on a shared channel. Every other server SUBSCRIBEs and drops its local cache entry — no direct server-to-server calls, no polling.

Demo: 2 subscribers listening, 1 publisher fires an invalidation.

```
Publisher:   Published '12345_678_view_5001' → 2 subscribers received
Server-B:    received: 12345_678_view_5001  → deleted 2 cache keys
Server-C:    received: 12345_678_view_5001  → deleted 0 cache keys  (B was first)
```

Both servers converge on the same invalidated state. Fire-and-forget delivery — no persistence, no replay. Ideal for cache-coherence signals where losing an occasional message is acceptable (soft TTL fallback catches drift).

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

Edit `build.gradle` — set `mainClass` to one of `'ViewCountCache'`, `'RateLimiter'`, or `'CacheInvalidator'`.

```bash
./gradlew run
```

For the invalidator demo, run 2+ subscribers in separate terminals, then publish from a fourth:

```bash
# Terminal 1
./gradlew run --args="server-B"

# Terminal 2
./gradlew run --args="server-C"

# Terminal 3 (publisher, sends one message and exits)
./gradlew run --args="publish 12345_678_view_5001"
```

## Structure

```
redis-patterns-showcase/
├── build.gradle
├── src/main/java/
│   ├── ViewCountCache.java     # soft TTL + compute lock
│   ├── RateLimiter.java        # rolling window + Lua atomic
│   └── CacheInvalidator.java   # cross-server invalidation via Pub/Sub
├── docs/
│   └── PATTERNS.md             # technical explainer
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
