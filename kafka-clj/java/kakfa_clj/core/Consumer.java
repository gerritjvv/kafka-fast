package kakfa_clj.core;

import clojure.lang.*;

import java.util.Arrays;
import java.util.Map;

/**
 * Kafka-clj Java API
 */
public class Consumer {

    static {
        RT.var("clojure.core", "require").invoke(Symbol.create("kafka-clj.consumer.node"));
    }

    private static final IPersistentVector toKeywordListMap(BrokerConf... brokers){
        PersistentVector v = PersistentVector.EMPTY;
        for(BrokerConf conf : brokers){
            v = v.cons(conf.toMap());
        }
        return v;
    }

    private static final IPersistentMap toKeywordMap(Map<?, ?> map){
        ITransientMap m = PersistentArrayMap.EMPTY.asTransient();
        for(Map.Entry<?, ?> entry: map.entrySet()){
            m = m.assoc(Keyword.intern(entry.getKey().toString()), entry.getValue());
        }

        return m.persistent();
    }

    public static Object createNode(KafkaConf kafkaConf, BrokerConf[] brokers, RedisConf redisConf, String... topics){
        PersistentArrayMap conf = PersistentArrayMap.createAsIfByAssoc(new Object[]{
                Keyword.intern("bootstrap-brokers"), Producer.toKeywordListMap(brokers),
                Keyword.intern("redis-conf"), redisConf.toMap()
        });

        return RT.var("kafka-clj.consumer.node", "create-node!").invoke(
                RT.var("clojure.core", "merge").invoke(kafkaConf.getConf(), conf),
                PersistentVector.create(Arrays.asList(topics)));
    }

    /**
     * This call will block until a message is received or the timeout in millseconds have expired.<br/>
     * On timeout null is returned
     * @param node
     * @param timeout
     * @return
     */
    public static Message readMsg(Object node, long timeoutMillis){
        return  new Message((Map<?, ?>)RT.var("kafka-clj.consumer.node", "read-msg!").invoke(node, timeoutMillis));
    }

    /**
     * This call will block until a message is received on the queue
     * @param node
     * @return
     */
    public static Message readMsg(Object node){
        return new Message((Map<?, ?>)RT.var("kafka-clj.consumer.node", "read-msg!").invoke(node));
    }

    /**
     * Adds topics for consumption to the current node
     * @param node
     * @param topics
     */
    public static void addTopics(Object node, String... topics){
        RT.var("kafka-clj.consumer.node", "add-topics!").invoke(node, PersistentVector.create(Arrays.asList(topics)));
    }

    /**
     * Remove topics from consumption from the current node
     * @param node
     * @param topics
     */
    public static void removeTopics(Object node, String... topics){
        RT.var("kafka-clj.consumer.node", "remove-topics!").invoke(node, PersistentVector.create(Arrays.asList(topics)));
    }

    public static void close(Object node){
        RT.var("kafka-clj.consumer.node", "shutdown-node!").invoke(node);
    }


    /**
     * Example usage
     */
    private void usageConsumer() throws Exception{


        Object node = Consumer.createNode(new KafkaConf(), new BrokerConf[]{new BrokerConf("192.168.4.40", 9092)}, new RedisConf("192.168.4.10", 6379, "test-group"), "my-topic");
        Message msg = Consumer.readMsg(node);

        String topic = msg.getTopic();
        long partition = msg.getPartition();
        long offset = msg.getOffset();
        byte[] bts = msg.getBytes();

    }
}
