package com.redislabs.university.RU102J.dao;

import javafx.beans.binding.ObjectBinding;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.Response;
import redis.clients.jedis.Transaction;

import java.util.List;
import java.util.UUID;

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

        try(Jedis jedis = jedisPool.getResource()) {

            String key = RedisSchema.getSlidingRateLimiterKey(name, windowSizeMS, maxHits);

            Transaction t = jedis.multi();

            t.zadd(key, System.currentTimeMillis(), UUID.randomUUID().toString());
            t.zremrangeByScore(key,0, System.currentTimeMillis() - windowSizeMS);
            Response<Long> count = t.zcard(key);

            t.exec();

            if(count.get() > maxHits) {
                throw new RateLimitExceededException();
            }
        }
    }
}
