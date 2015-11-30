package com.github.dryangkun.kafka.clustermer.coordinator;

import com.github.dryangkun.kafka.clustermer.*;
import org.junit.Assert;
import org.junit.Test;

import java.util.*;

public class DynamicCoordinatorTest {

    @Test public void coordinate() throws Exception {
        Map<Partition, Broker> partBrokers = new HashMap<Partition, Broker>();
        partBrokers.put(new Partition("a", 0), new Broker("x1"));
        partBrokers.put(new Partition("a", 1), new Broker("x2"));
        partBrokers.put(new Partition("b", 0), new Broker("x1"));
        partBrokers.put(new Partition("b", 1), new Broker("x1"));
        partBrokers.put(new Partition("b", 2), new Broker("x2"));
        partBrokers.put(new Partition("c", 0), new Broker("x2"));

        LinkedHashMap<Partition, Broker> result1, result2;
        {
            DynamicCoordinator coordinator = new DynamicCoordinator()
                    .setIndexAndTotal(0, 2)
                    .setTopics("a", "b", "c");

            result1 = coordinator.coordinate(partBrokers);
        }

        {
            DynamicCoordinator coordinator = new DynamicCoordinator()
                    .setIndexAndTotal(1, 2)
                    .setTopics("a", "b", "c");

            result2 = coordinator.coordinate(partBrokers);
        }

        System.out.println(result1);
        System.out.println(result2);

        Set<Partition> set1 = new HashSet<Partition>(result1.keySet());
        set1.retainAll(result2.keySet());
        Assert.assertTrue(set1.isEmpty());

        result1.putAll(result2);
        System.out.println(result1);
        System.out.println(partBrokers);

        Assert.assertTrue(result1.size() == partBrokers.size());
        for (Partition k : partBrokers.keySet()) {
            Assert.assertTrue(result1.containsKey(k));
            Assert.assertEquals(partBrokers.get(k), result1.get(k));
        }
    }
}
