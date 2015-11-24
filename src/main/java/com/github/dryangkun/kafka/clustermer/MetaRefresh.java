package com.github.dryangkun.kafka.clustermer;

import java.io.Serializable;

public abstract class MetaRefresh {

    protected final int metaInterval;
    protected final ClusterConsumer clusterConsumer;

    public MetaRefresh(int metaInterval, ClusterConsumer clusterConsumer) {
        this.metaInterval = metaInterval;
        this.clusterConsumer = clusterConsumer;
    }

    public abstract void start() throws Exception;

    public abstract boolean refresh() throws Exception;

    public abstract void refresh(Fetcher oldFetcher) throws Exception;

    public abstract void close();

    public interface Factory<A extends MetaRefresh> extends Serializable {
        A newRefresher(int metaInterval, ClusterConsumer clusterConsumer);
    }
}
