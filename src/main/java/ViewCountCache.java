import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.params.SetParams;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Cached view counts with soft TTL and stampede protection.
 *
 * Pattern: cache-aside + sidecar expiryTime + SET-NX-PX compute lock.
 * Redis keys per view:
 *   {zgId}_{depId}_{viewId}_count        — cached count value
 *   {zgId}_{depId}_{viewId}_expiryTime   — "best by" timestamp in ms
 *   {zgId}_{depId}_{viewId}_lock         — compute lock (SET NX PX 30000)
 */
public class ViewCountCache {

    private static final int  REDIS_DB           = 2;
    private static final long FRESHNESS_WINDOW_MS = 60_000;   // soft-TTL window
    private static final long LOCK_TTL_MS         = 30_000;   // auto-release if the holder crashes

    static final JedisPool pool;
    static {
        JedisPoolConfig config = new JedisPoolConfig();
        config.setMaxTotal(20);
        pool = new JedisPool(config, "localhost", 6379);
    }

    /** Stand-in for an expensive DB query (500 ms). */
    static long computeViewCount(long zgId, long depId, String viewId) {
        try { Thread.sleep(500); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
        return (zgId + depId + viewId.hashCode()) % 100;
    }

    static long getViewCount(long zgId, long depId, String viewId) {
        String countKey  = zgId + "_" + depId + "_" + viewId + "_count";
        String expiryKey = zgId + "_" + depId + "_" + viewId + "_expiryTime";
        String lockKey   = zgId + "_" + depId + "_" + viewId + "_lock";

        try (Jedis jedis = pool.getResource()) {
            jedis.select(REDIS_DB);

            String cached = jedis.get(countKey);
            String expiry = jedis.get(expiryKey);
            long now = System.currentTimeMillis();

            // Fresh: value present and within the soft-TTL window.
            if (cached != null && expiry != null && now < Long.parseLong(expiry)) {
                return Long.parseLong(cached);
            }

            // Stale or missing — race for the compute lock. Only the winner recomputes.
            SetParams lockParams = SetParams.setParams().nx().px(LOCK_TTL_MS);
            String lockAcquired  = jedis.set(lockKey, Thread.currentThread().getName(), lockParams);

            if ("OK".equals(lockAcquired)) {
                try {
                    long count = computeViewCount(zgId, depId, viewId);
                    jedis.set(countKey, String.valueOf(count));
                    jedis.set(expiryKey, String.valueOf(now + FRESHNESS_WINDOW_MS));
                    return count;
                } finally {
                    jedis.del(lockKey);   // always release; TTL is the crash safety net
                }
            }

            // Lost the race — serve the stale value while the winner recomputes.
            if (cached != null) {
                return Long.parseLong(cached);
            }

            // Cold-start edge: no cached value AND lost the lock. Fall back to compute.
            return computeViewCount(zgId, depId, viewId);
        }
    }

    /** Concurrent stampede demo: 10 threads race on a stale key; exactly 1 recomputes. */
    public static void main(String[] args) throws Exception {
        try (Jedis j = pool.getResource()) {
            j.select(REDIS_DB);
            j.set("12345_678_view_5001_count", "-999");    // pre-populate stale
            j.set("12345_678_view_5001_expiryTime", "1");   // expired long ago
            j.del("12345_678_view_5001_lock");
        }

        int threadCount = 10;
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        CountDownLatch startGate = new CountDownLatch(1);
        CountDownLatch doneGate  = new CountDownLatch(threadCount);

        long start = System.currentTimeMillis();

        for (int i = 0; i < threadCount; i++) {
            executor.submit(() -> {
                try {
                    startGate.await();
                    long threadStart = System.currentTimeMillis();
                    long count = getViewCount(12345, 678, "view_5001");
                    long elapsed = System.currentTimeMillis() - threadStart;
                    System.out.println("[" + Thread.currentThread().getName()
                            + "] count=" + count + " took=" + elapsed + "ms");
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

        System.out.println("Total: " + (System.currentTimeMillis() - start) + " ms");
    }
}
