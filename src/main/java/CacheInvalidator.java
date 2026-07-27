import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.JedisPubSub;

/**
 * Cross-server cache invalidation via Redis Pub/Sub.
 *
 * When any server changes user data, it PUBLISHes an event on a shared channel.
 * Every other server SUBSCRIBEs to the channel and drops its local cache entry
 * for the affected user — no direct server-to-server calls needed.
 */
public class CacheInvalidator {

    private static final int REDIS_DB = 2;
    private static final String CHANNEL  = "cache_invalidations";

    static final JedisPool pool;
    static {
        JedisPoolConfig config = new JedisPoolConfig();
        config.setMaxTotal(10);
        pool = new JedisPool(config, "localhost", 6379);
    }

    static void subscribe(String subscriberName){
        try(Jedis jedis = pool.getResource()){
            jedis.select(REDIS_DB);

             System.out.println("[" + subscriberName + "] listening on '" + CHANNEL + "'...");
             jedis.subscribe(new JedisPubSub(){
                @Override
                public void onMessage(String channel,String message){
                     System.out.println("[" + subscriberName + "] received: " + message);
                     invalidateCache(message);
                }
             },CHANNEL);
        }
    }

    static long publish(String message){
        try(Jedis jedis = pool.getResource()){
            jedis.select(REDIS_DB);
            return jedis.publish(CHANNEL,message);
        }
    }

    static void invalidateCache(String viewKey){
        try(Jedis jedis = pool.getResource()){
            jedis.select(REDIS_DB);
            long deleted = jedis.del(
                viewKey + "_count",
                viewKey + "_expiryTime",
                viewKey + "_lock"
            );
            System.out.println("   → deleted " + deleted + " cache keys for " + viewKey);
        }
    }

    public static void main(String[] args) throws Exception {
        // TODO — filled in step by step
       if(args.length >= 2 && args[0].equals("publish")){
        long count = publish(args[1]);
        System.out.println("Published '" + args[1] + "' → " + count + " subscribers received");
        return;
       }
       String name = args.length > 0 ? args[0] : "subscribe-1";
       subscribe(name);
    }
}
