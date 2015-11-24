package com.github.dryangkun.kafka.clustermer.storage;

import com.github.dryangkun.kafka.clustermer.Fetcher;
import com.github.dryangkun.kafka.clustermer.Partition;
import net.openhft.chronicle.map.ChronicleMap;

public class ChronicleStorage implements Storage {

    private final ChronicleMap<String, Long> map;

    public ChronicleStorage(ChronicleMap<String, Long> map) {
        this.map = map;
    }

    public ChronicleMap<String, Long> getMap() {
        return map;
    }

    private String partToKey(Partition partition) {
        return "t:" + partition.getTopic() + ",p:" + partition.getId();
    }

    public long get(Partition partition) throws Exception {
        Long offset = map.get(partToKey(partition));
        return offset != null ? offset : Fetcher.EMPTY_OFFSET;
    }

    public void put(Partition partitioh, long offset) throws Exception {
        map.put(partToKey(partitioh), offset);
    }

    public void close() {
        map.close();
    }
}
