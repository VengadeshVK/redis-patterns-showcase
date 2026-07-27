import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Rolling-window rate limiter backed by a Redis sorted set.
 *
 * The 4 steps (prune old, count, threshold check, add new) are wrapped in a single
 * atomic Lua script — otherwise concurrent callers can interleave between the
 * count and the add, and the threshold gets silently violated.
 */
public class RateLimiter {

    private static final int REDIS_DB = 2;

    static final JedisPool pool;
    static {
        JedisPoolConfig config = new JedisPoolConfig();
        config.setMaxTotal(30);
        pool = new JedisPool(config, "localhost", 6379);
    }

    private static final String RATE_LIMIT_SCRIPT =
        "local key = KEYS[1] " +
        "local now = tonumber(ARGV[1]) " +
        "local windowStart = tonumber(ARGV[2]) " +
        "local threshold = tonumber(ARGV[3]) " +
        "local member = ARGV[4] " +

        "redis.call('ZREMRANGEBYSCORE', key, 0, windowStart) " +
        "local count = redis.call('ZCARD', key) " +

        "if count >= threshold then " +
        "  return 0 " +
        "end " +

        "redis.call('ZADD', key, now, member) " +
        "redis.call('EXPIRE', key, 60) " +
        "return 1";

    /** @return true if allowed, false if blocked. */
    static boolean allow(String userId, int threshold, long windowMillis) {
        String key = "ratelimit_" + userId;

        try (Jedis jedis = pool.getResource()) {
            jedis.select(REDIS_DB);

            long now = System.currentTimeMillis();
            long windowStart = now - windowMillis;
            String member = now + ":" + Math.random();

            Object result = jedis.eval(
                RATE_LIMIT_SCRIPT,
                1,
                key,
                String.valueOf(now),
                String.valueOf(windowStart),
                String.valueOf(threshold),
                member
            );

            return ((Long) result) == 1L;
        }
    }

    /** 20 threads race for a threshold of 5. Exactly 5 should be allowed. */
    public static void main(String[] args) throws Exception {
        try (Jedis j = pool.getResource()) {
            j.select(REDIS_DB);
            j.del("ratelimit_user_A");
        }

        int threshold = 5;
        long window = 10_000;
        int threadCount = 20;

        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        CountDownLatch startGate = new CountDownLatch(1);
        CountDownLatch doneGate  = new CountDownLatch(threadCount);
        AtomicInteger allowedCount = new AtomicInteger(0);
        AtomicInteger blockedCount = new AtomicInteger(0);

        for (int i = 0; i < threadCount; i++) {
            executor.submit(() -> {
                try {
                    startGate.await();
                    boolean allowed = allow("user_A", threshold, window);
                    (allowed ? allowedCount : blockedCount).incrementAndGet();
                    System.out.println("[" + Thread.currentThread().getName() + "] "
                            + (allowed ? "ALLOWED" : "BLOCKED"));
                } catch (Exception e) {
                    e.printStackTrace();
                } finally {
                    doneGate.countDown();
                }
            });
        }

        startGate.countDown();
        doneGate.await();
        executor.shutdown();

        System.out.println("---");
        System.out.println("Threshold: " + threshold);
        System.out.println("Allowed:   " + allowedCount.get());
        System.out.println("Blocked:   " + blockedCount.get());
    }
}
