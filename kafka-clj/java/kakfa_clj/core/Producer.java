package kakfa_clj.core;

import clojure.lang.*;

import java.util.HashMap;
import java.util.Map;

/**
 * Kafka-clj Java API
 */
public class Producer {

    static {
        RT.var("clojure.core", "require").invoke(Symbol.create("kafka-clj.client"));
    }

    protected static final IPersistentVector toKeywordListMap(BrokerConf... brokers){
        PersistentVector v = PersistentVector.EMPTY;
        for(BrokerConf conf : brokers){
            v = v.cons(conf.toMap());
        }
        return v;
    }

    protected static final IPersistentMap toKeywordMap(Map<?, ?> map){
        ITransientMap m = PersistentArrayMap.EMPTY.asTransient();
        for(Map.Entry<?, ?> entry: map.entrySet()){
            m = m.assoc(Keyword.intern(entry.getKey().toString()), entry.getValue());
        }

        return m.persistent();
    }

    public static Object createConnector(BrokerConf... brokers){
        return createConnector(new KafkaConf(), brokers);
    }

    public static Object createConnector (KafkaConf conf, BrokerConf... brokers){
       return RT.var("kafka-clj.client", "create-connector").invoke(toKeywordListMap(brokers), conf.getConf());
    }

    public static void sendMsg(Object connector, String topic, byte[] msg){
        RT.var("kafka-clj.client", "send-msg").invoke(connector, topic, msg);
    }

    public static void close(Object connector){
        RT.var("kafka-clj.client", "close").invoke(connector);
    }

    /**
     * Example usage
     */
    private static void usageProducer() throws Exception{
        {
            Object connector = Producer.createConnector(new BrokerConf("192.168.4.40", 9092));
            Producer.sendMsg(connector, "my-topic", "Hi".getBytes("UTF-8"));
            Producer.close(connector);

        }

    }
}
