package kakfa_clj.core;

import clojure.lang.*;
import clojure.lang.IPersistentMap;

import java.util.Arrays;
import java.util.Map;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Kafka-clj Java API
 * <p/>
 * <pre>
 *     Consumer node = Consumer.createNode(new KafkaConf(), new BrokerConf[]{new BrokerConf("192.168.4.40", 9092)}, new RedisConf("192.168.4.10", 6379, "test-group"), "my-topic");
 *     Message msg = node.readMsg();
 *
 *     String topic = msg.getTopic();
 *     long partition = msg.getPartition();
 *     long offset = msg.getOffset();
 *     byte[] bts = msg.getBytes();
 * </pre>
 */
public class Consumer implements Iterable<Message>{

    static {
        RT.var("clojure.core", "require").invoke(Symbol.create("kafka-clj.consumer.node"));
    }

    private final Object connector;

    private final AtomicBoolean closed = new AtomicBoolean(false);

    private Consumer(Object connector){
        this.connector = connector;
    }

    public static Consumer connect(KafkaConf kafkaConf, BrokerConf[] brokers, RedisConf redisConf, String... topics){
        PersistentArrayMap conf = PersistentArrayMap.createAsIfByAssoc(new Object[]{
                Keyword.intern("bootstrap-brokers"), Producer.toKeywordListMap(brokers),
                Keyword.intern("redis-conf"), redisConf.toMap()
        });

        return new Consumer(
                RT.var("kafka-clj.consumer.node", "create-node!").invoke(
                RT.var("clojure.core", "merge").invoke(kafkaConf.getConf(), conf),
                PersistentVector.create(Arrays.asList(topics))));
    }

    /**
     * This call will block until a message is received or the timeout in millseconds have expired.<br/>
     * On timeout null is returned
     * @param node
     * @param timeout
     * @return
     */
    public Message readMsg(long timeoutMillis){
        return  new Message((Map<?, ?>)RT.var("kafka-clj.consumer.node", "read-msg!").invoke(connector, timeoutMillis));
    }

    /**
     * This call will block until a message is received on the queue
     * @param node
     * @return
     */
    public Message readMsg(){
        return new Message((Map<?, ?>)RT.var("kafka-clj.consumer.node", "read-msg!").invoke(connector));
    }

    /**
     * Adds topics for consumption to the current node
     * @param topics
     */
    public void addTopics(String... topics){
        RT.var("kafka-clj.consumer.node", "add-topics!").invoke(connector, PersistentVector.create(Arrays.asList(topics)));
    }

    /**
     * Remove topics from consumption from the current node
     * @param topics
     */
    public void removeTopics(String... topics){
        RT.var("kafka-clj.consumer.node", "remove-topics!").invoke(connector, PersistentVector.create(Arrays.asList(topics)));
    }

    /**
     * Returns a thread safe iterator that will always return true till this consumer is closed
     * @return
     */
    @Override
    public Iterator<Message> iterator(){
        return new Iterator<Message>(){
            public boolean hasNext(){
                return !closed.get();
            }
            public Message next(){
                return readMsg();
            }
            public void remove(){
                throw new UnsupportedOperationException();
            }
        };
    }

    public void close(){
        closed.set(true);
        RT.var("kafka-clj.consumer.node", "shutdown-node!").invoke(connector);
    }


    /**
     * Example usage
     */
    private void usageConsumer() throws Exception{


        Consumer node = Consumer.connect(new KafkaConf(), new BrokerConf[]{new BrokerConf("192.168.4.40", 9092)}, new RedisConf("192.168.4.10", 6379, "test-group"), "my-topic");
        Message msg = node.readMsg();

        String topic = msg.getTopic();
        long partition = msg.getPartition();
        long offset = msg.getOffset();
        byte[] bts = msg.getBytes();

        for(Message message : node){
            System.out.println(message);
        }

    }
}
