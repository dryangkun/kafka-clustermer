package com.github.dryangkun.kafka.clustermer.storage;

import com.github.dryangkun.kafka.clustermer.ClusterConfig;

import java.io.Serializable;

public abstract class StorageBuilder<A extends Storage> implements Serializable {

    public static StorageBuilder newDefaultBuilder() {
        return new ChronicleStorageBuilder();
    }

    protected ClusterConfig clusterConfig;

    public abstract A newStorage() throws Exception;

    public void setClusterConfig(ClusterConfig clusterConfig) {
        this.clusterConfig = clusterConfig;
    }
}
