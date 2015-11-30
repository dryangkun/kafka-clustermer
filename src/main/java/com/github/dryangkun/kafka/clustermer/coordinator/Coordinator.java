package com.github.dryangkun.kafka.clustermer.coordinator;

import com.github.dryangkun.kafka.clustermer.*;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public interface Coordinator {

    List<String> getTopics();

    LinkedHashMap<Partition, Broker> coordinate(Map<Partition, Broker> partBrokers) throws Exception;
}
