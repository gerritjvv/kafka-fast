package kakfa_clj.core;

import clojure.lang.IPersistentMap;
import clojure.lang.Keyword;
import clojure.lang.PersistentArrayMap;

import java.util.HashMap;

/**
 */
public class KafkaConf {

    public enum CODEC { NONE, GZIP, SNAPPY};

    private static final Keyword KW_CODEC = Keyword.intern("codec");
    private static final Keyword KW_ACKS = Keyword.intern("acks");
    private static final Keyword KW_USE_EARLIEST = Keyword.intern("use-earliest");
    private static final Keyword KW_CONSUME_STEP = Keyword.intern("consume-step");
    private static final Keyword KW_FETCH_TIMEOUT = Keyword.intern("fetch-timeout");
    private static final Keyword KW_MIN_BTS = Keyword.intern("min-bytes");
    private static final Keyword KW_MAX_BTS = Keyword.intern("max-bytes");
    private static final Keyword KW_MAX_WAIT_TIME = Keyword.intern("max-wait-time");
    private static final Keyword KW_JAAS = Keyword.intern("jaas");
    private static final Keyword KW_KAFKA_VERSION = Keyword.intern("kafka-version");


    IPersistentMap conf = PersistentArrayMap.EMPTY;

    public void setJaas(String jaas){
        conf = conf.assoc(KW_JAAS, jaas);
    }

    public void setKafkaVersion(String version){
        conf = conf.assoc(KW_KAFKA_VERSION, version);
    }

    public void setMaxWaitTime(long maxWaitTime){
        conf = conf.assoc(KW_MAX_WAIT_TIME, maxWaitTime);
    }

    public void setMaxBytes(long maxBts){
        conf = conf.assoc(KW_MAX_BTS, maxBts);
    }

    public void setMinBytes(long minBts){
        conf = conf.assoc(KW_MIN_BTS, minBts);
    }

    public void setFetchTimeout(long fetchTimeout){
        conf = conf.assoc(KW_FETCH_TIMEOUT, fetchTimeout);
    }

    public void setUseEarliest(boolean useEarliest){
        conf = conf.assoc(KW_USE_EARLIEST, useEarliest);
    }

    public void setConsumeStep(int consumeStep){
        conf = conf.assoc(KW_CONSUME_STEP, consumeStep);
    }

    public void setAcks(int acks){
        conf = conf.assoc(KW_ACKS, acks);
    }

    public void setCodec(CODEC codec){
        switch(codec){
            case GZIP:
                conf = conf.assoc(KW_CODEC, 1);
                break;
            case SNAPPY:
                conf = conf.assoc(KW_CODEC, 2);
                break;
            default:
                conf = conf.assoc(KW_CODEC, 0);
                break;
        };
    }

    public IPersistentMap getConf(){
        return conf;
    }
}
