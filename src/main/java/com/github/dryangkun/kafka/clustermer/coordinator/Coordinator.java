package com.github.dryangkun.kafka.clustermer.coordinator;

import com.github.dryangkun.kafka.clustermer.ClusterConsumer;
import com.github.dryangkun.kafka.clustermer.Fetcher;

import java.util.List;

public interface Coordinator {

    List<Fetcher> coordinate(ClusterConsumer clusterConsumer) throws Exception;
}
