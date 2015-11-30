package com.github.dryangkun.kafka.clustermer.storm;

import backtype.storm.spout.MultiScheme;
import backtype.storm.spout.RawMultiScheme;
import com.github.dryangkun.kafka.clustermer.ClusterConfig;
import com.github.dryangkun.kafka.clustermer.coordinator.Coordinator;
import com.github.dryangkun.kafka.clustermer.storage.StorageBuilder;

import java.io.Serializable;
import java.util.List;

public class SpoutConfig implements Serializable {
    public MultiScheme scheme = new RawMultiScheme();

    public ClusterConfig clusterConfig;

    public List<String> topics;
    public String groupId;
    public String zkRoot;
    public StorageBuilder storageBuilder;

    public int commitPerEmitCount = 2048;
    public int commitPerIntervalMs = 3000;
    public int pendingMaxSize = 8192;
}
