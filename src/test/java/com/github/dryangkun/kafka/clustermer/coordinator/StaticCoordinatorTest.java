package com.github.dryangkun.kafka.clustermer.coordinator;

import com.github.dryangkun.kafka.clustermer.Broker;
import com.github.dryangkun.kafka.clustermer.Partition;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

public class StaticCoordinatorTest {

    @Test public void coordinate() throws Exception {
        Map<Partition, Broker> partBrokers = new HashMap<Partition, Broker>();
        partBrokers.put(new Partition("a", 0), new Broker("x1"));
        partBrokers.put(new Partition("a", 1), new Broker("x2"));
        partBrokers.put(new Partition("b", 0), new Broker("x1"));
        partBrokers.put(new Partition("b", 1), new Broker("x1"));
        partBrokers.put(new Partition("b", 2), new Broker("x2"));
        partBrokers.put(new Partition("c", 0), new Broker("x2"));

        StaticCoordinator coordinator = new StaticCoordinator()
                .addTopicParts("a", 1)
                .addTopicParts("b", 2)
                .addTopicParts("c", 0);

        LinkedHashMap<Partition, Broker> result = coordinator.coordinate(partBrokers);

        Assert.assertEquals(result.get(new Partition("a", 1)), new Broker("x2"));
        Assert.assertEquals(result.get(new Partition("b", 2)), new Broker("x2"));
        Assert.assertEquals(result.get(new Partition("c", 0)), new Broker("x2"));
    }
}
