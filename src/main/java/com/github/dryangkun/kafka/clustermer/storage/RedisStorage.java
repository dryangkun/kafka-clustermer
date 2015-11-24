package com.github.dryangkun.kafka.clustermer.storage;

import com.github.dryangkun.kafka.clustermer.Fetcher;
import com.github.dryangkun.kafka.clustermer.Partition;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.exceptions.JedisConnectionException;

public class RedisStorage implements Storage {

    private final JedisPool jedisPool;
    private final String key;

    public RedisStorage(String key, JedisPool jedisPool) {
        this.key = key;
        this.jedisPool = jedisPool;
    }

    public JedisPool getJedisPool() {
        return jedisPool;
    }

    public String getKey() {
        return key;
    }

    private String partToKey(Partition partition) {
        return "t:" + partition.getTopic() + ",p:" + partition.getId();
    }

    public long get(Partition partition) throws Exception {
        Jedis jedis = null;
        long offset = Fetcher.EMPTY_OFFSET;
        try {
            jedis = jedisPool.getResource();
            String value = jedis.hget(key, partToKey(partition));
            if (value != null && value.length() > 0) {
                try {
                    offset = Long.parseLong(value);
                } catch (NumberFormatException e) {}
            }
        } catch (JedisConnectionException e) {
            if (jedis != null) {
                jedisPool.returnBrokenResource(jedis);
                jedis = null;
            }
            throw e;
        } finally {
            if (jedis != null) {
                jedisPool.returnResource(jedis);
            }
        }
        return offset;
    }

    public void put(Partition partition, long offset) throws Exception {
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            jedis.hset(key, partToKey(partition), "" + offset);
        } catch (JedisConnectionException e) {
            if (jedis != null) {
                jedisPool.returnBrokenResource(jedis);
                jedis = null;
            }
            throw e;
        } finally {
            if (jedis != null) {
                jedisPool.returnResource(jedis);
            }
        }
    }

    public void close() {
        jedisPool.close();
    }
}
