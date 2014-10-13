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

    private final Object connector;

    private Producer(Object connector){
        this.connector = connector;
    }

    protected static final IPersistentVector toKeywordListMap(BrokerConf... brokers){
        PersistentVector v = PersistentVector.EMPTY;
        for(BrokerConf conf : brokers){
            v = v.cons(conf.toMap());
        }
        return v;
    }

    public static Producer connect(BrokerConf... brokers){
        return connect(new KafkaConf(), brokers);
    }

    public static Producer connect (KafkaConf conf, BrokerConf... brokers){
       return new Producer(RT.var("kafka-clj.client", "create-connector").invoke(toKeywordListMap(brokers), conf.getConf()));
    }

    public Producer sendMsg(String topic, byte[] msg){
        RT.var("kafka-clj.client", "send-msg").invoke(connector, topic, msg);
        return this;
    }

    public void close(){
        RT.var("kafka-clj.client", "close").invoke(connector);
    }

    /**
     * Example usage
     */
    private static void usageProducer() throws Exception{
        {
            Producer producer = Producer.connect(new BrokerConf("192.168.4.40", 9092));
            producer.sendMsg("my-topic", "Hi".getBytes("UTF-8"));
            producer.close();

        }

    }
}
