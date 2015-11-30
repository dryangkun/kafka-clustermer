package com.github.dryangkun.kafka.clustermer;

import kafka.javaapi.consumer.SimpleConsumer;
import org.apache.log4j.Logger;

import java.util.Collection;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * non thread-safe
 */
public class FetcherContainer {

    private final static Logger LOG = Logger.getLogger(FetcherContainer.class);

    private final Map<Partition, Fetcher> partFetchers = new TreeMap<Partition, Fetcher>();
    private final LinkedBlockingQueue<Fetcher> queue = new LinkedBlockingQueue<Fetcher>(1024);
    private final int id;
    private final ClusterConsumer clusterConsumer;

    public FetcherContainer(int id, ClusterConsumer clusterConsumer) {
        this.id = id;
        this.clusterConsumer = clusterConsumer;
    }

    public int getId() {
        return id;
    }

    public void send(Fetcher fetcher) {
        try {
            queue.put(fetcher);
        } catch (InterruptedException e) {
            //do nothing
        }
    }

    public Collection<Fetcher> getFetchers() {
        Fetcher fetcher;
        while ((fetcher = queue.poll()) != null) {
            if (fetcher.isClosing()) {
                partFetchers.remove(fetcher.getPart());
                clusterConsumer.connections.returnConsumer(fetcher.getBroker(), fetcher.getConsumer());
            } else {
                SimpleConsumer consumer = clusterConsumer.connections.getConsumer(fetcher.getBroker());
                fetcher.setConsumer(consumer);
                partFetchers.put(fetcher.getPart(), fetcher);
            }
        }
        return partFetchers.values();
    }

    public Fetcher getFetcher(Partition part) {
        return partFetchers.get(part);
    }

    public void closeFetcher(Fetcher fetcher) {
        partFetchers.remove(fetcher.getPart());
        clusterConsumer.connections.returnConsumer(fetcher.getBroker(), fetcher.getConsumer());
    }

    public void closeAllFetchers() {
        Collection<Fetcher> fetchers = getFetchers();
        for (Fetcher fetcher : fetchers) {
            clusterConsumer.connections.returnConsumer(fetcher.getBroker(), fetcher.getConsumer());
        }
        fetchers.clear();
    }

    public void refreshFetcher(Fetcher fetcher) {
        try {
            closeFetcher(fetcher);
            clusterConsumer.getMetaRefresh().refresh(fetcher);
        } catch (Exception e) {
            LOG.error("refresh fetcher fail", e);
        }
    }
}
