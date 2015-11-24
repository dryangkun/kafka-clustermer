package com.github.dryangkun.kafka.clustermer.storage;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.Protocol;

import java.io.Serializable;

public class RedisStorageBuilder extends StorageBuilder<RedisStorage> implements Serializable {

    private int port = 6379;
    private String host;
    private int timeout = Protocol.DEFAULT_TIMEOUT;
    private String auth = null;

    private String key;

    public RedisStorageBuilder setPort(int port) {
        this.port = port;
        return this;
    }

    public RedisStorageBuilder setHost(String host) {
        this.host = host;
        return this;
    }

    public RedisStorageBuilder setTimeout(int timeout) {
        this.timeout = timeout;
        return this;
    }

    public RedisStorageBuilder setAuth(String auth) {
        this.auth = auth;
        return this;
    }

    public RedisStorageBuilder setKey(String key) {
        this.key = key;
        return this;
    }

    @Override
    public RedisStorage newStorage() throws Exception {
        int concurrency = clusterConfig.getConcurrency();

        GenericObjectPoolConfig config = new GenericObjectPoolConfig();
        config.setMaxIdle(concurrency);
        config.setMaxTotal(concurrency);
        config.setMinIdle(0);

        JedisPool jedisPool = new JedisPool(config,
                host, port, timeout, auth);
        return new RedisStorage(key, jedisPool);
    }
}
