package com.github.dryangkun.kafka.clustermer;

import kafka.javaapi.consumer.SimpleConsumer;

import java.util.Collection;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * thread-safe
 */
public class FetcherContainer implements Comparable<FetcherContainer> {

    private final Set<Fetcher> fetchers = new TreeSet<Fetcher>();
    private final LinkedBlockingQueue<Fetcher> queue = new LinkedBlockingQueue<Fetcher>(1024);
    private final int id;
    private final ClusterConsumer clusterConsumer;
    private final Connections connections;

    public FetcherContainer(int id, ClusterConsumer clusterConsumer) {
        this.id = id;
        this.clusterConsumer = clusterConsumer;
        this.connections = clusterConsumer.getConnections();
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

    public synchronized Collection<Fetcher> getFetchers() {
        Fetcher fetcher;
        while ((fetcher = queue.poll()) != null) {
            if (fetcher.isClosing()) {
                fetchers.remove(fetcher);
                connections.returnConsumer(fetcher.getBroker(), fetcher.getConsumer());
            } else {
                SimpleConsumer consumer = connections.getConsumer(fetcher.getBroker());
                fetcher.setConsumer(consumer);
                fetchers.add(fetcher);
            }
        }
        return fetchers;
    }

    public synchronized void closeFetcher(Fetcher fetcher) {
        fetchers.remove(fetcher);
        connections.returnConsumer(fetcher.getBroker(), fetcher.getConsumer());
    }

    public synchronized void closeAllFetchers() {
        Collection<Fetcher> fetchers = getFetchers();
        for (Fetcher fetcher : fetchers) {
            connections.returnConsumer(fetcher.getBroker(), fetcher.getConsumer());
        }
        fetchers.clear();
    }

    public int compareTo(FetcherContainer container) {
        return fetchers.size() - container.fetchers.size();
    }
}
