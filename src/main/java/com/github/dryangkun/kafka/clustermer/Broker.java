package com.github.dryangkun.kafka.clustermer;

import java.io.Serializable;

public class Broker implements Comparable<Broker>, Serializable {

    private final String host;
    private final int port;

    public Broker(kafka.cluster.Broker broker) {
        this(broker.host(), broker.port());
    }

    public Broker(String host, int port) {
        this.host = host;
        this.port = port;
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }

    @Override
    public int hashCode() {
        return toString().hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        return !(obj == null || getClass() != obj.getClass()) && compareTo((Broker) obj) == 0;
    }

    @Override
    public String toString() {
        return host + ":" + port;
    }

    public int compareTo(Broker o) {
        int i = host.compareTo(o.host);
        if (i == 0) {
            return port - o.port;
        } else {
            return i;
        }
    }
}
