package com.github.dryangkun.kafka.clustermer.coordinator;

import com.github.dryangkun.kafka.clustermer.Broker;
import com.github.dryangkun.kafka.clustermer.ClusterConsumer;
import com.github.dryangkun.kafka.clustermer.Fetcher;
import com.github.dryangkun.kafka.clustermer.Partition;

import java.util.*;

public class StaticCoordinator implements Coordinator {

    private final Map<String, Set<Partition>> topicParts = new TreeMap<String, Set<Partition>>();

    /**
     * add a topic and its partitions for consumer
     * @param topic
     * @param partIds
     * @return
     */
    public StaticCoordinator addTopicParts(String topic, int... partIds) {
        for (int partition : partIds) {
            Partition part = new Partition(topic, partition);

            Set<Partition> parts = topicParts.get(topic);
            if (parts == null) {
                parts = new TreeSet<Partition>();
                topicParts.put(topic, parts);
            }
            parts.add(part);
        }
        return this;
    }

    /**
     * add partitions for consumer
     * @param parts
     * @return
     */
    public StaticCoordinator addParts(Partition... parts) {
        for (Partition partition : parts) {
            Set<Partition> _parts = topicParts.get(partition.getTopic());
            if (_parts == null) {
                _parts = new TreeSet<Partition>();
                topicParts.put(partition.getTopic(), _parts);
            }
            _parts.add(partition);
        }
        return this;
    }

    public List<Fetcher> coordinate(ClusterConsumer clusterConsumer) throws Exception {
        if (topicParts.isEmpty()) {
            throw new IllegalArgumentException("addTopicParts or addParts should call before");
        }

        List<Fetcher> fetchers = new ArrayList<Fetcher>();
        Map<Partition, Broker> partBrokers =
                clusterConsumer.findPartBrokers(new ArrayList<String>(topicParts.keySet()));

        for (Set<Partition> parts : topicParts.values()) {
            for (Partition part : parts) {
                Broker broker = partBrokers.get(part);
                if (broker == null) {
                    //todo log warn
                } else {
                    fetchers.add(clusterConsumer.newFetcher(broker, part));
                }
            }
        }
        return fetchers;
    }
}
