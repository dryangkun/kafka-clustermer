package com.github.dryangkun.kafka.clustermer;

import com.github.dryangkun.kafka.clustermer.storage.Storage;

public class FetcherConfig {

    protected final Storage storage;

    protected final int maxWait;
    protected final int minBytes;
    protected final int fetchSize;
    protected final FetcherMode mode;

    public FetcherConfig(Storage storage, ClusterConfig clusterConfig) {
        this.storage = storage;
        maxWait = clusterConfig.getMaxWait();
        minBytes = clusterConfig.getMinBytes();
        fetchSize = clusterConfig.getFetchSize();
        mode = clusterConfig.getFetcherMode();
    }
}
