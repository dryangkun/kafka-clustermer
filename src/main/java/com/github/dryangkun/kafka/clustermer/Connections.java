package com.github.dryangkun.kafka.clustermer;

import kafka.javaapi.consumer.SimpleConsumer;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * thread-safe
 */
public class Connections {

    private final Map<Broker, Set<Partition>> brokerPartitionsMap = new HashMap<Broker, Set<Partition>>();

    private final int soTimeout;
    private final int bufferSize;
    private final String clientId;

    public Connections(ClusterConfig config) {
        soTimeout = config.getSoTimeout();
        bufferSize = config.getBufferSize();
        clientId = config.getClientId();
    }

    public SimpleConsumer getConsumer(Broker broker) {
        return new SimpleConsumer(
            broker.getHost(), broker.getPort(),
            soTimeout, bufferSize, clientId);
    }

    public void returnConsumer(Broker broker, SimpleConsumer consumer) {
        if (consumer != null) {
            consumer.close();
        }
    }
}
