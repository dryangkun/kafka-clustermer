package com.github.dryangkun.kafka.clustermer;

import kafka.common.ErrorMapping;

public class KafkaException extends Exception {

    private final Broker broker;
    private final Partition part;
    private final short errorCode;

    public KafkaException(String message, Broker broker, Partition part, short errorCode) {
        super(message + ", broker=" + broker + ", part=" + part,
                ErrorMapping.exceptionFor(errorCode));
        this.errorCode = errorCode;
        this.broker = broker;
        this.part = part;
    }

    public short getErrorCode() {
        return errorCode;
    }

    public Broker getBroker() {
        return broker;
    }

    public Partition getPart() {
        return part;
    }

    public boolean needRefresh() {
        return errorCode != ErrorMapping.OffsetOutOfRangeCode();
    }
}
