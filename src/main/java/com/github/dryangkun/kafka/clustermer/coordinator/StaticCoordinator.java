package com.github.dryangkun.kafka.clustermer.coordinator;

import com.github.dryangkun.kafka.clustermer.*;

import java.io.Serializable;
import java.util.*;

public class StaticCoordinator implements Coordinator, Serializable {

    private final Map<String, Set<Partition>> topicParts = new TreeMap<String, Set<Partition>>();

    /**
     * @param topic topic
     * @param partIds partIds for consumer
     * @return this
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
     * @param parts partitions for consumer
     * @return this
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

    public List<String> getTopics() {
        return new ArrayList<String>(topicParts.keySet());
    }

    public LinkedHashMap<Partition, Broker> coordinate(Map<Partition, Broker> partBrokers) throws Exception {
        if (topicParts.isEmpty()) {
            throw new IllegalArgumentException("addTopicParts or addParts should call before");
        }

        LinkedHashMap<Partition, Broker> result = new LinkedHashMap<Partition, Broker>();
        for (Set<Partition> parts : topicParts.values()) {
            for (Partition part : parts) {
                Broker broker = partBrokers.get(part);
                if (broker == null) {
                    throw new Exception("partition=" + part + " find no broker");
                } else {
                    result.put(part, broker);
                }
            }
        }
        return result;
    }
}
