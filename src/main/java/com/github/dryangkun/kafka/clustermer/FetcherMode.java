package com.github.dryangkun.kafka.clustermer;

public enum FetcherMode {
    /**
     * if get offset from storage else earliest
     */
    STORAGE_OR_EARLIEST(1 | 1 << 1),
    /**
     * if get offset from storage else latest
     */
    STORAGE_OR_LATEST(1 | 1 << 2),
    /**
     * directly from earliest
     */
    EARLIEST(1 << 1),
    /**
     * directly from latest
     */
    LATEST(1 << 2);

    private int value;

    FetcherMode(int value) {
        this.value = value;
    }

    public boolean isStorage() {
        return (value & (1)) != 0;
    }

    public boolean isEarliest() {
        return (value & (1 << 1)) != 0;
    }

    public boolean isLatest() {
        return (value & (1 << 2)) != 0;
    }
}
