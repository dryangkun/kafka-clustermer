package com.github.dryangkun.kafka.clustermer.storm;

import backtype.storm.spout.MultiScheme;
import backtype.storm.spout.RawMultiScheme;
import com.github.dryangkun.kafka.clustermer.ClusterConfig;

import java.io.Serializable;
import java.util.List;

public class SpoutConfig implements Serializable {
    public MultiScheme scheme = new RawMultiScheme();

    public List<String> topics;
    public ClusterConfig clusterConfig;

    public SpoutConfig(ClusterConfig clusterConfig) {
        this.clusterConfig = clusterConfig;
    }
}
