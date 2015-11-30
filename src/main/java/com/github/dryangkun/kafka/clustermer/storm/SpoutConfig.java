package com.github.dryangkun.kafka.clustermer.storm;

import backtype.storm.spout.MultiScheme;
import backtype.storm.spout.RawMultiScheme;
import com.github.dryangkun.kafka.clustermer.ClusterConfig;
import com.github.dryangkun.kafka.clustermer.coordinator.Coordinator;
import com.github.dryangkun.kafka.clustermer.storage.StorageBuilder;

import java.io.Serializable;
import java.util.List;

public class SpoutConfig implements Serializable {
    /**
     * deserialize the message from kafka to some storm tuples
     */
    public MultiScheme scheme = new RawMultiScheme();
    /**
     * ClusterConfig
     */
    public ClusterConfig clusterConfig;
    /**
     * the topics needing to consume
     */
    public List<String> topics;
    /**
     * the consumer's group id to store offset to zk
     */
    public String groupId;
    /**
     * the root path of zk to store offset
     */
    public String zkRoot;
    /**
     * if not use zk to store offset, you need use storageBuilder
     */
    public StorageBuilder storageBuilder;

    /**
     * how many tuples emited then store the offset
     */
    public int commitPerEmitCount = 2048;
    /**
     * the ms interval to store the offset
     */
    public int commitPerIntervalMs = 3000;
    /**
     * the max number of once filling the pending list
     *
     */
    public int pendingMaxSize = 8192;
}
