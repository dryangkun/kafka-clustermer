package com.github.dryangkun.kafka.clustermer;

import com.github.dryangkun.kafka.clustermer.coordinator.Coordinator;
import com.github.dryangkun.kafka.clustermer.storage.Storage;
import kafka.javaapi.PartitionMetadata;
import kafka.javaapi.TopicMetadata;
import kafka.javaapi.TopicMetadataRequest;
import kafka.javaapi.TopicMetadataResponse;
import kafka.javaapi.consumer.SimpleConsumer;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class ClusterConsumer {

    private static final Logger LOG = Logger.getLogger(ClusterConsumer.class);

    private final Connections connections;
    private final Set<Broker> metaBrokers;

    private final Storage storage;
    private final Coordinator coordinator;
    private final int metaInterval;

    private final List<FetcherContainer> fetcherContainers;
    private final FetcherConfig fetcherConfig;

    private boolean closed = false;
    private final Refresher refresher = new Refresher();
    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);


    public ClusterConsumer(ClusterConfig config) throws Exception {
        connections = new Connections(config);
        metaBrokers = new HashSet<Broker>(config.getMetaBrokers());

        storage = config.getStorageBuilder().newStorage();
        coordinator = config.getCoordinator();
        metaInterval = config.getMetaInterval();

        fetcherContainers = new ArrayList<FetcherContainer>(config.getConcurrency());
        for (int i = 0; i < config.getConcurrency(); i++) {
            fetcherContainers.add(new FetcherContainer(i, this));
        }
        fetcherConfig = new FetcherConfig(storage, config);
    }

    public synchronized void start() throws Exception {
        refresher.dispatch();
        scheduler.schedule(new Runnable() {
            public void run() {
                try {
                    refresher.dispatch();
                } catch (Exception e) {
                    LOG.error("dispatch fetcher fail", e);
                }
            }
        }, metaInterval, TimeUnit.SECONDS);
    }

    public void refresh(final Fetcher fetcher) {
        scheduler.submit(new Runnable() {
            public void run() {
                try {
                    refresher.refresh(fetcher);
                } catch (Exception e) {
                    LOG.error("refresh fetcher fail=" + fetcher, e);
                }
            }
        });
    }

    private class Refresher {

        private int next = 0;
        private final Map<Partition, Fetcher> partFetchers = new TreeMap<Partition, Fetcher>();

        private synchronized FetcherContainer getNextFetchSet() {
            FetcherContainer fetcherSet = fetcherContainers.get(next);
            if ((++next) == fetcherContainers.size()) {
                next = 0;
            }
            return fetcherSet;
        }

        private synchronized void refresh(Fetcher oldFetcher) throws Exception {
            if (closed) {
                return;
            }

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
                Fetcher fetcher = newFetcher(broker, part);
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

        private synchronized void dispatch() throws Exception {
            if (closed) {
                return;
            }

            List<Fetcher> fetchers = coordinator.coordinate(ClusterConsumer.this);
            for (Fetcher fetcher : fetchers) {
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
    }

    public FetcherConfig getFetcherConfig() {
        return fetcherConfig;
    }

    public Connections getConnections() {
        return connections;
    }

    public Fetcher newFetcher(Broker broker, Partition partition) {
        return new Fetcher(broker, partition, fetcherConfig);
    }

    public Map<Partition, Broker> findPartBrokers(List<String> topics) throws IOException {
        Set<Broker> newMetaBrokers = new HashSet<Broker>();
        Map<Partition, Broker> partBrokers = null;
        String error = null;

        for (Broker metaBroker : metaBrokers) {
            SimpleConsumer consumer = null;
            try {
                consumer = connections.getConsumer(metaBroker);
                TopicMetadataRequest request = new TopicMetadataRequest(topics);
                TopicMetadataResponse response = consumer.send(request);

                List<TopicMetadata> metadatas = response.topicsMetadata();
                Map<Partition, Broker> _partBrokers = new TreeMap<Partition, Broker>();

                for (TopicMetadata metadata : metadatas) {
                    for (PartitionMetadata partitionMetadata : metadata.partitionsMetadata()) {
                        Partition partition = new Partition(
                                metadata.topic(), partitionMetadata.partitionId());
                        Broker broker = new Broker(partitionMetadata.leader());
                        _partBrokers.put(partition, broker);
                        if (LOG.isDebugEnabled()) {
                            LOG.debug("find partition=" + partition + " at broker=" + broker);
                        }

                        if (!metaBrokers.contains(broker)) {
                            LOG.info("find new meta broker=" + broker);
                            newMetaBrokers.add(broker);
                        }
                    }
                }
                partBrokers = _partBrokers;
                break;
            } catch (Exception e) {
                LOG.warn("topic meta data request fail=" + topics + " at broker=" + metaBroker, e);
                error = e.getMessage();
            } finally {
                connections.returnConsumer(metaBroker, consumer);
            }
        }

        metaBrokers.addAll(newMetaBrokers);
        if (partBrokers != null) {
            return partBrokers;
        } else {
            throw new IOException("meta request fail=" + error + " for topics=" + topics);
        }
    }

    protected Map<Partition, Broker> findPartBrokers(String... topics) throws IOException {
        return findPartBrokers(Arrays.asList(topics));
    }

    public synchronized void close() {
        scheduler.shutdown();
        synchronized (refresher) {
            closed = true;
        }
        storage.close();
    }

    public synchronized Collection<FetcherContainer> getFetcherContainers() {
        return fetcherContainers;
    }
}
