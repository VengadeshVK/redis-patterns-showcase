# Patterns Explained

## 1. Soft TTL + compute lock (`ViewCountCache.java`)

### The problem
Caching an expensive query with a hard TTL creates a **thundering-herd** failure: at the moment the TTL expires, every concurrent reader sees a cache miss, and all of them race to the database with identical queries.

### The design

**Two keys per cached value** (three under load):

| Key | Purpose |
|---|---|
| `..._count` | The cached value itself. Never auto-expired. |
| `..._expiryTime` | Millisecond timestamp indicating when the value goes stale. |
| `..._lock` | Compute lock. `SET NX PX 30000` — auto-released on holder crash. |

**Read logic:**
1. `GET` count and expiryTime.
2. If `now < expiryTime` → return count. Fresh.
3. Otherwise try to claim the lock via `SET NX PX`.
4. If we won → recompute → write count + refresh expiryTime → `DEL` lock in `finally`.
5. If we lost → return the stale count. Someone else is recomputing.

### Why it works

- **No hard misses.** Because `_count` never auto-expires, there's always a value to serve — fresh or stale. Users never see a spinner mid-recompute.
- **Stampede impossible.** `SET NX` is atomic. Only one caller gets `"OK"`; every other caller must serve stale.
- **Crash-safe.** The lock's `PX 30000` guarantees a crashed winner can't hold the lock forever. Worst case: 30 seconds of extra staleness, then automatic recovery.

### Cost profile under load
- 10 concurrent stale-readers → 1 database query, not 10.
- Losers return in ~5–10 ms (just the two GETs + failed SETNX).
- Winner pays the full compute cost (~500 ms in the demo).

---

## 2. Rolling-window rate limiter (`RateLimiter.java`)

### The problem
Enforce *"at most N events per window seconds"* per user, correctly, when N app servers are all handling requests. In-memory counters on each server don't work — each server would allow N and the aggregate becomes N×servers.

### The design

**One sorted set per user**: `ratelimit_<userId>`. Each request is one member; the score is the request's timestamp.

**Check logic (in one Lua script):**
1. `ZREMRANGEBYSCORE key 0 (now - window)` — prune entries outside the current window.
2. `ZCARD key` — count remaining.
3. If `count >= threshold` → return 0 (BLOCKED).
4. Otherwise `ZADD key now <unique_member>` + `EXPIRE key 60` → return 1 (ALLOWED).

### Why it must be atomic

Doing the four steps as four separate Redis calls looks correct but breaks under concurrency:

```
Thread A: ZREMRANGE  ZCARD→4  check  ZADD
Thread B:            ZREMRANGE  ZCARD→4  check  ZADD
                                ↑
                       both see count=4 → both add → count is now 6
```

Two callers both observe `count=4` before either adds. Both decide "under threshold." Threshold silently violated.

Wrapping the four steps in a Lua script fixes this. Redis runs each `EVAL` to completion — no other command can interleave.

### Verifiable outcome
20 threads, threshold=5:
- Naïve version: **20 allowed** (race condition).
- Lua version: **exactly 5 allowed, 15 blocked**.

Same test, same threading, one behaves correctly and the other doesn't — makes the atomicity argument concrete.

### Companion `_COUNT` key (not in this demo)

Production versions often add a companion integer key (`ratelimit_userA_COUNT`) tracking the running total, updated inside the same Lua script. That way threshold checks are O(1) instead of paying `ZCARD` cost each time. The Zoho Desk `RollingRateLimiter` uses this pattern.

---

## Cross-cutting themes

- **Redis is a shared side store.** The source of truth stays in the database.
- **Atomicity is a property of a single Redis command OR a Lua script** — never of a client-side sequence of separate calls.
- **Every borrowed connection must be returned.** `try-with-resources` (Jedis) or `try/finally` around `pool.getResource()`.
- **Every lock must have a TTL.** A crash without cleanup is not a fringe case — treat it as normal.
