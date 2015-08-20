package kafka_clj.util;

import clojure.lang.IFn;
import clojure.lang.ILookup;
import clojure.lang.Keyword;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import java.io.UnsupportedEncodingException;

/**
 * Utility class to create
 */
public class Fetch {

    /**
     * Read the kafka fetch response
     * @param buffer
     * @param state State passed through to onMessage as onMessage.apply(state, message), note that this state is no pure and expected to be mutated by the onMessage.<br/>
     *              It is handled in a thread safe manner inside this class.
     * @param onMessage Function called when a message is found, is called with onMessage.apply(state, new Message(...)), or onMessage.apply(state, new FetchError(...))
     * @return State is returned
     * @throws UnsupportedEncodingException
     */
    public static final Object readFetchResponse(ByteBuf buffer, Object state, IFn onMessage) throws UnsupportedEncodingException{
        int corrId = buffer.readInt();

        readTopicArray(buffer, state, onMessage);
        return state;
    }

    /**
     * Read [topic topic topic]
     * @param buffer
     * @param state
     * @param onMessage Function called when a message is found
     * @throws UnsupportedEncodingException
     */
    public static final void readTopicArray(ByteBuf buffer, Object state, IFn onMessage) throws UnsupportedEncodingException{
        int topicLen = buffer.readInt();
        if(topicLen == 1){
            readTopic(buffer, state, onMessage);
        }else{
            for(int i = 0; i < topicLen; i++)
                readTopic(buffer, state, onMessage);
        }
    }

    /**
     * Read a single topic from the topic array
     * @param buffer
     * @param state
     * @param onMessage
     * @throws UnsupportedEncodingException
     */
    private static final void readTopic(ByteBuf buffer, Object state, IFn onMessage) throws UnsupportedEncodingException{
        String topicName = readShortString(buffer);
        readPartitionArray(topicName, buffer, state, onMessage);
    }

    /**
     * Read a partition array [partition, partition, partition]
     * @param topicName
     * @param buffer
     * @param state
     * @param onMessage
     */
    private static final void readPartitionArray(String topicName, ByteBuf buffer, Object state, IFn onMessage){
        int partitionLen = buffer.readInt();
        if(partitionLen == 1){
            readPartition(topicName, buffer, state, onMessage);
        }else{
            for(int i = 0; i < partitionLen; i++)
                readPartition(topicName, buffer, state, onMessage);
        }
    }

    /**
     * Read a single partition partition-number:int, error-code:short, message-set-size:int...
     * @param topicName
     * @param buffer
     * @param state
     * @param onMessage
     */
    private static final void readPartition(String topicName, ByteBuf buffer, Object state, IFn onMessage){
        int partition = buffer.readInt();
        int errorCode = buffer.readShort();
        buffer.readLong(); //HighwaterMarkOffset
        int messageSetByteSize = buffer.readInt();

        if(errorCode > 0)
            onMessage.invoke(state, FetchError.create(topicName, partition, errorCode));
        else if(buffer.readableBytes() >= messageSetByteSize)
            readMessageSet(topicName, partition, buffer.readSlice(messageSetByteSize), state, onMessage);
    }

    /**
     * Read the contents of a message set, handles half sent messages gracefully
     * @param topicName
     * @param partition
     * @param buffer
     * @param state
     * @param onMessage
     */
    private static final void readMessageSet(String topicName, int partition, ByteBuf buffer, Object state, IFn onMessage) {
        while(buffer.readableBytes() > 12){
            long offset = buffer.readLong();
            int messageByteSize = buffer.readInt();

            if (messageByteSize > 10 && buffer.readableBytes() >= messageByteSize)
                readMessage(topicName, partition, offset, buffer.readSlice(messageByteSize), state, onMessage);
            else
                break;
        }
    }

    /**
     * Read the message thats inside a message set, and any compressed messages are re-read as message sets by calling readMessageSet.
     * @param topicName
     * @param partition
     * @param offset
     * @param buffer
     * @param state
     * @param onMessage
     */
    private static void readMessage(String topicName, int partition, long offset, ByteBuf buffer, Object state, IFn onMessage) {

        int crc = buffer.readInt();  //crc
        byte magic = buffer.readByte(); //magic byte
        int codec = buffer.readByte() & 0x07;

        if(buffer.readableBytes() > 4){
            byte[] key = readBytes(buffer); //key
            byte[] bts = readBytes(buffer); //value

            if(bts != null){
                if(codec > 0){
                    byte[] deCompBts = Util.decompress(codec, bts);
                    readMessageSet(topicName, partition, Unpooled.wrappedBuffer(deCompBts), state, onMessage);
                }else
                    onMessage.invoke(state, Message.create(topicName, partition, offset, bts));
            }
        }
    }

    /**
     * Helper method to ready a byte array the format expected is [len:int, bytes[len] ]
     * @param buffer
     * @return
     */
    private static final byte[] readBytes(ByteBuf buffer){
        int len = buffer.readInt();
        if(len > 0 && buffer.readableBytes() >= len){
            byte[] arr = new byte[len];
            buffer.readBytes(arr);
            return arr;
        }else
            return null;
    }

    /**
     * Read a short string of format [len:short, string[len]]
     * @param buffer
     * @return
     * @throws UnsupportedEncodingException
     */
    private static final String readShortString(ByteBuf buffer) throws UnsupportedEncodingException {
        int len = buffer.readShort();
        byte[] bts = new byte[len];
        buffer.readBytes(bts);
        return new String(bts, "UTF-8");
    }

    /**
     * This is the message passed into the onMessage calling code.<br/>
     * It implements ILookup so can be used like (:topic msg), (:partition msg), (:offset msg), (:bts msg)
     */
    public static final class Message implements ILookup {
        private static final Keyword KW_TOPIC = Keyword.intern("topic");
        private static final Keyword KW_PARTITION = Keyword.intern("partition");
        private static final Keyword KW_OFFSET = Keyword.intern("offset");
        private static final Keyword KW_BTS = Keyword.intern("bts");


        private String topic;
        private int partition;
        private long offset;
        private byte[] bts;

        public Message(String topic, int partition, long offset, byte[] bts) {
            this.topic = topic;
            this.partition = partition;
            this.offset = offset;
            this.bts = bts;
        }

        public String getTopic() {
            return topic;
        }

        public void setTopic(String topic) {
            this.topic = topic;
        }

        public int getPartition() {
            return partition;
        }

        public void setPartition(int partition) {
            this.partition = partition;
        }

        public long getOffset() {
            return offset;
        }

        public void setOffset(long offset) {
            this.offset = offset;
        }

        public byte[] getBts() {
            return bts;
        }

        public void setBts(byte[] bts) {
            this.bts = bts;
        }


        @Override
        public Object valAt(Object key) {
            if(KW_TOPIC.equals(key))
                return topic;
            else if(KW_PARTITION.equals(key))
                return new Integer(partition);
            else if(KW_OFFSET.equals(key))
                return new Long(offset);
            else if(KW_BTS.equals(key))
                return bts;
            else
                return null;
        }

        @Override
        public Object valAt(Object key, Object notFound) {
            Object val = valAt(key);
            return val == null ? notFound : val;
        }

        public String toString(){
            return "Message[" + topic + "," + partition + "," + offset + "]";
        }

        public static final Message create(String topic, int partition, long offset, byte[] bts){
            return new Message(topic, partition, offset, bts);
        }
    }

    /**
     * Represents a fetch errors and implements ILookup so can be used as:<br/>
     * (:topic error), (:partition error), (:error-code error)
     */
    public static final class FetchError implements ILookup{

        private static final Keyword KW_ERROR_CODE = Keyword.intern("error-code");

        private String topic;
        private int partition;
        private int errorCode;

        public FetchError(String topic, int partition, int errorCode) {
            this.topic = topic;
            this.partition = partition;
            this.errorCode = errorCode;
        }

        public String getTopic() {
            return topic;
        }

        public void setTopic(String topic) {
            this.topic = topic;
        }

        public int getPartition() {
            return partition;
        }

        public void setPartition(int partition) {
            this.partition = partition;
        }

        public int getErrorCode() {
            return errorCode;
        }

        public void setErrorCode(int errorCodec) {
            this.errorCode = errorCodec;
        }

        @Override
        public Object valAt(Object key) {
            if(Message.KW_TOPIC.equals(key))
                return topic;
            else if(Message.KW_PARTITION.equals(key))
                return new Integer(partition);
            else if(KW_ERROR_CODE.equals(key))
                return new Integer(errorCode);
            else
                return null;
        }

        @Override
        public Object valAt(Object key, Object notFound) {
            Object val = valAt(key);
            return val == null ? notFound : val;
        }

        public String toString(){
            String topic2 = topic;
            if(topic != null && topic.length() > 100)
                topic2 = topic.substring(0, 100);

            return "FetchError[topic:" + topic2 + ",partition:" + partition + ",error-code:" + errorCode + "]";
        }

        public static final FetchError create(String topic, int partition, int errorCode){
            return new FetchError(topic, partition, errorCode);
        }
    }
}
