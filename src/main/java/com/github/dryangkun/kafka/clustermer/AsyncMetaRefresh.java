package com.github.dryangkun.kafka.clustermer;

import org.apache.log4j.Logger;

import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class AsyncMetaRefresh extends MetaRefresh {

    private static final Logger LOG = Logger.getLogger(AsyncMetaRefresh.class);

    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
    private volatile boolean closed = false;

    private final SyncMetaRefresh syncMetaRefresh;

    public AsyncMetaRefresh(int metaInterval, Set<Broker> metaBrokers, ClusterConsumer clusterConsumer) {
        super(metaInterval, metaBrokers, clusterConsumer);
        syncMetaRefresh = new SyncMetaRefresh.Factory().newRefresher(metaInterval, metaBrokers, clusterConsumer);
    }

    @Override
    public synchronized void start() throws Exception {
        syncMetaRefresh.start();
        scheduler.schedule(new Runnable() {
            public void run() {
                LOG.debug("schedule refresh meta");
                if (!closed) {
                    try {
                        syncMetaRefresh.doRefresh();
                    } catch (Exception e) {
                        LOG.error("start sync do refresh fail", e);
                    }
                }
            }
        }, metaInterval, TimeUnit.SECONDS);
    }

    @Override
    public synchronized boolean refresh() throws Exception {
        if (!closed) {
            scheduler.submit(new Runnable() {
                public void run() {
                    if (!closed) {
                        try {
                            syncMetaRefresh.doRefresh();
                        } catch (Exception e) {
                            LOG.error("refresh sync do refresh fail", e);
                        }
                    }
                }
            });
            return true;
        }
        return false;
    }

    @Override
    public void refresh(final Fetcher oldFetcher) throws Exception {
        if (!closed) {
            scheduler.submit(new Runnable() {
                public void run() {
                    if (!closed) {
                        try {
                            syncMetaRefresh.refresh(oldFetcher);
                        } catch (Exception e) {
                            LOG.error("refresh fetcher sync do refresh fail", e);
                        }
                    }
                }
            });
        }
    }

    @Override
    public void close() {
        LOG.info("closing");
        closed = true;
        scheduler.shutdownNow();
        syncMetaRefresh.close();
        try {
            while (!scheduler.awaitTermination(500L, TimeUnit.MILLISECONDS));
        } catch (InterruptedException e) {}
        LOG.info("closed");
    }

    public static class Factory implements MetaRefresh.Factory<AsyncMetaRefresh> {
        public AsyncMetaRefresh newRefresher(int metaInterval, Set<Broker> metaBrokers, ClusterConsumer clusterConsumer) {
            return new AsyncMetaRefresh(metaInterval, metaBrokers, clusterConsumer);
        }
    }
}
