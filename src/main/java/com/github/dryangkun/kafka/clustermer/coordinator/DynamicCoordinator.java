package com.github.dryangkun.kafka.clustermer.coordinator;

import com.github.dryangkun.kafka.clustermer.*;
import org.apache.kafka.common.utils.Crc32;

import java.io.Serializable;
import java.util.*;

public class DynamicCoordinator implements Coordinator, Serializable {

    private int total;
    private int index;

    private List<String> topics = new ArrayList<String>();

    /**
     * must
     * @param topics topics for consumer
     */
    public DynamicCoordinator setTopics(String... topics) {
        this.topics = new ArrayList<String>(topics.length);
        Collections.addAll(this.topics, topics);
        return this;
    }

    /**
     * @see DynamicCoordinator#setTopics(String...)
     */
    public DynamicCoordinator setTopics(Collection<String> topics) {
        this.topics.addAll(topics);
        return this;
    }

    /**
     * must
     * @param index current consumers index to all
     * @param total number of all consumers
     */
    public DynamicCoordinator setIndexAndTotal(int index, int total) {
        this.index = index;
        this.total = total;
        return this;
    }

    public List<Fetcher> coordinate(ClusterConsumer clusterConsumer) throws Exception {
        if (topics.isEmpty()) {
            throw new IllegalArgumentException("topics must set");
        }
        if (total == 0) {
            throw new IllegalArgumentException("index and total must set");
        }

        List<Fetcher> fetchers = new ArrayList<Fetcher>();
        Map<Partition, Broker> partBrokers =
                clusterConsumer.findPartBrokers(new ArrayList<String>(topics));

        Map<String,Integer> topicIndexMap = new HashMap<String, Integer>();
        for (String topic : topics) {
            int index = (int) (Crc32.crc32(topic.getBytes()) % total);
            topicIndexMap.put(topic, index);
        }

        for (Partition part : partBrokers.keySet()) {
            int i = topicIndexMap.get(part.getTopic()) + part.getId();
            i = i % total;
            if (i == index) {
                Broker broker = partBrokers.get(part);
                fetchers.add(clusterConsumer.newFetcher(broker, part));
            }
        }
        return fetchers;
    }
}
