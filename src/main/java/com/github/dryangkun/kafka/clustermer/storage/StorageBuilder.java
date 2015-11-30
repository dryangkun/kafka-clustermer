package com.github.dryangkun.kafka.clustermer.storage;

import com.github.dryangkun.kafka.clustermer.ClusterConfig;

import java.io.Serializable;

public abstract class StorageBuilder<A extends Storage> implements Serializable {

    protected final ClusterConfig clusterConfig;

    public StorageBuilder(ClusterConfig clusterConfig) {
        this.clusterConfig = clusterConfig;
    }

    public abstract A newStorage() throws Exception;
}
