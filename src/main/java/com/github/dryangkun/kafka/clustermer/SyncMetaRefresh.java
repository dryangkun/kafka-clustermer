package com.github.dryangkun.kafka.clustermer;

import org.apache.log4j.Logger;

import java.util.*;

public class SyncMetaRefresh extends MetaRefresh {

    private static final Logger LOG = Logger.getLogger(SyncMetaRefresh.class);

    private int next = 0;
    private final Map<Partition, Fetcher> partFetchers = new TreeMap<Partition, Fetcher>();

    private long lastRefreshTime = -1;

    public SyncMetaRefresh(int metaInterval, Set<Broker> metaBrokers, ClusterConsumer clusterConsumer) {
        super(metaInterval, metaBrokers, clusterConsumer);
    }

    @Override
    public synchronized void start() throws Exception {
        refresh();
    }

    private synchronized FetcherContainer getNextFetchSet() {
        FetcherContainer fetcherSet = clusterConsumer.fetcherContainers.get(next);
        if ((++next) == clusterConsumer.fetcherContainers.size()) {
            next = 0;
        }
        return fetcherSet;
    }

    @Override
    public synchronized boolean refresh() throws Exception {
        long time = System.currentTimeMillis();
        if (lastRefreshTime == -1 || time - lastRefreshTime >= metaInterval) {
            doRefresh();
            lastRefreshTime = time;
            return true;
        }
        return false;
    }

    protected synchronized void doRefresh() throws Exception {
        List<String> topics = clusterConsumer.coordinator.getTopics();
        LinkedHashMap<Partition, Broker> partBrokers = clusterConsumer.coordinator.coordinate(
                findPartBrokers(topics));

        for (Partition part : partBrokers.keySet()) {
            Broker broker = partBrokers.get(part);
            Fetcher fetcher = new Fetcher(broker, part, clusterConsumer.fetcherConfig);

            if (!partFetchers.containsKey(fetcher.getPart())) {
                FetcherContainer container = getNextFetchSet();
                fetcher.setContainer(container);

                if (LOG.isDebugEnabled()) {
                    LOG.debug("new fetcher=" + fetcher + " for container-" + container.getId());
                }
                container.send(fetcher);
                partFetchers.put(fetcher.getPart(), fetcher);
            } else {
                Fetcher oldFetcher = partFetchers.get(fetcher.getPart());
                FetcherContainer container = oldFetcher.getContainer();

                if (!fetcher.equals(oldFetcher)) {
                    LOG.debug("new fetcher=" + fetcher + " replace old=" + oldFetcher);

                    oldFetcher.closing();
                    container.send(oldFetcher);

                    fetcher.setContainer(container);
                    container.send(fetcher);
                    partFetchers.put(fetcher.getPart(), fetcher);
                }
            }
        }
    }

    @Override
    public synchronized void refresh(Fetcher oldFetcher) throws Exception {
        Partition part = oldFetcher.getPart();
        FetcherContainer container = oldFetcher.getContainer();
        Map<Partition, Broker> partBrokers = findPartBrokers(part.getTopic());

        if (LOG.isDebugEnabled()) {
            LOG.debug("refresh closing old fetcher=" + oldFetcher);
        }
        oldFetcher.closing();
        container.send(oldFetcher);

        if (partBrokers.containsKey(part)) {
            Broker broker = partBrokers.get(part);
            Fetcher fetcher = new Fetcher(broker, part, clusterConsumer.fetcherConfig);
            if (LOG.isDebugEnabled()) {
                LOG.debug("refresh open new fetcher=" + fetcher);
            }

            fetcher.setContainer(container);
            container.send(fetcher);
            partFetchers.put(part, fetcher);
        } else {
            partFetchers.remove(part);
        }
    }

    @Override
    public void close() {
        next = 0;
        lastRefreshTime = -1;
        partFetchers.clear();
    }

    public static class Factory implements MetaRefresh.Factory<SyncMetaRefresh> {
        public SyncMetaRefresh newRefresher(int metaInterval, Set<Broker> metaBrokers, ClusterConsumer clusterConsumer) {
            return new SyncMetaRefresh(metaInterval, metaBrokers, clusterConsumer);
        }
    }
}
