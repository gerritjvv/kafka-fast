package kafka_clj.util;

import clojure.lang.IFn;

/**
 * Mutable class to update fetch state from a fetch request for each message.<br/>
 * This is more efficient than creating new instances on every message.
 */
public class FetchState {


    String topic;
    Object status;

    long partition;
    long initOffset;

    long offset;
    long maxOffset;

    IFn delegate;

    long discarded = 0;

    int minByteSize = 512;
    int maxByteSize = 512;

    public FetchState(IFn delegate, String topic, Object status, long partition, long offset, long maxOffset) {
        this.delegate = delegate;
        this.topic = topic;
        this.status = status;
        this.partition = partition;
        this.initOffset = offset;
        this.offset = offset;
        this.maxOffset = maxOffset;
    }

    public void updateMinMax(byte[] bts){
        setMinByteSize(bts.length);
        setMaxByteSize(bts.length);
    }

    public void setMinByteSize(int size) {
        minByteSize = Math.min(minByteSize, size);
    }

    public void setMaxByteSize(int size) {
        maxByteSize = Math.max(maxByteSize, size);
    }

    public int getMinByteSize(){
        return minByteSize;
    }

    public int getMaxByteSize(){
        return maxByteSize;
    }

    public long getInitOffset(){
        return initOffset;
    }

    public long getDiscarded() {
        return discarded;
    }

    public void incDiscarded(){
        discarded++;
    }

    public void setDiscarded(long discarded) {
        this.discarded = discarded;
    }

    public IFn getDelegate() { return delegate; }

    public Object getStatus() {
        return status;
    }

    public void setStatus(Object status) {
        this.status = status;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public long getPartition() {
        return partition;
    }

    public void setPartition(long partition) {
        this.partition = partition;
    }

    public long getOffset() {
        return offset;
    }

    public void setOffset(long offset) {
        this.offset = offset;
    }

    public long getMaxOffset() {
        return maxOffset;
    }

    @Override
    public String toString() {
        return "FetchState{" +
                "topic='" + topic + '\'' +
                ", status=" + status +
                ", partition=" + partition +
                ", initOffset=" + initOffset +
                ", offset=" + offset +
                ", maxOffset=" + maxOffset +
                ", delegate=" + delegate +
                ", discarded=" + discarded +
                ", minByteSize=" + minByteSize +
                ", maxByteSize=" + maxByteSize +
                '}';
    }
}
