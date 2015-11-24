package com.github.dryangkun.kafka.clustermer;

import java.io.Serializable;

public class Partition implements Comparable<Partition>, Serializable {

    private final String topic;
    private final int id;

    public Partition(String topic, int id) {
        this.topic = topic;
        this.id = id;
    }

    public String getTopic() {
        return topic;
    }

    public int getId() {
        return id;
    }

    @Override
    public int hashCode() {
        //todo use apache commons hash code method
        return toString().hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }
        if (obj == null || obj.getClass() != getClass()) {
            return false;
        }
        return compareTo((Partition) obj) == 0;
    }

    @Override
    public String toString() {
        return topic + "-" + id;
    }

    public int compareTo(Partition o) {
        int i = topic.compareTo(o.topic);
        if (i == 0) {
            return id - o.id;
        } else {
            return i;
        }
    }
}
