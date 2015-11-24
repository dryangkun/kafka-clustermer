package com.github.dryangkun.kafka.clustermer.storage;

import com.github.dryangkun.kafka.clustermer.ClusterConfig;

public abstract class StorageBuilder<A extends Storage> {

    public static StorageBuilder newDefaultBuilder() {
        return new ChronicleStorageBuilder();
    }

    protected ClusterConfig clusterConfig;

    public abstract A newStorage() throws Exception;

    public void setClusterConfig(ClusterConfig clusterConfig) {
        this.clusterConfig = clusterConfig;
    }
}
