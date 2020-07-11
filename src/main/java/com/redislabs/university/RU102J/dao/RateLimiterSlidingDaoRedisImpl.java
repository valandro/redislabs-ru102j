package com.redislabs.university.RU102J.dao;

import com.redislabs.university.RU102J.core.KeyHelper;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Response;

import java.time.ZonedDateTime;
import java.util.Random;

public class RateLimiterSlidingDaoRedisImpl implements RateLimiter {

    private final JedisPool jedisPool;
    private final long windowSizeMS;
    private final long maxHits;

    public RateLimiterSlidingDaoRedisImpl(JedisPool pool, long windowSizeMS,
                                          long maxHits) {
        this.jedisPool = pool;
        this.windowSizeMS = windowSizeMS;
        this.maxHits = maxHits;
    }

    // Challenge #7
    @Override
    public void hit(String name) throws RateLimitExceededException {
        // START CHALLENGE #7
        try (Jedis jedis = jedisPool.getResource()) {
            String key = getKey(name);
            Pipeline pipeline = jedis.pipelined();
            Long timeStamp = System.currentTimeMillis();
            String member = timeStamp + "-" + new Random().nextLong();
            pipeline.zadd(key, timeStamp, member);
            pipeline.zremrangeByScore(key, 0, timeStamp - windowSizeMS);
            Response<Long> hits= pipeline.zcard(key);
            pipeline.sync();

            if (hits.get() > maxHits) {
                throw new RateLimitExceededException();
            }
        }

        // END CHALLENGE #7
    }

    // [limiter]:[windowSize]:[name]:[maxHits]
    private String getKey(String name) {
        return KeyHelper.getKey("limiter:" +
                windowSizeMS + ":" +
                String.valueOf(name) + ":" +
                String.valueOf(maxHits));
    }
}
