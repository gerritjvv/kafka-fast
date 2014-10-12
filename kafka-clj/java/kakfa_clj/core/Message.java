package kakfa_clj.core;

import java.util.Map;
import clojure.lang.Keyword;
import clojure.lang.RT;
import clojure.lang.Symbol;

/**
 * Kafka-clj Java API
 */
public class Message {

    static {
        RT.var("clojure.core", "require").invoke(Symbol.create("clojure.core"));
    }

    private static final Keyword KW_PARTITION = Keyword.intern("partition");
    private static final Keyword KW_TOPIC = Keyword.intern("topic");
    private static final Keyword KW_BTS = Keyword.intern("bts");
    private static final Keyword KW_OFFSET = Keyword.intern("offset");
    private static final Keyword KW_ERRORCODE = Keyword.intern("error-code");

    final Map<?, ?> record;

    protected Message(Map<?, ?> record){
        this.record = record;
    }

    public int getPartition(){
        return ((Number)record.get(KW_PARTITION)).intValue();
    }

    public long getOffset(){
        return ((Long)record.get(KW_OFFSET)).longValue();
    }

    public String getTopic(){
        return (String)record.get(KW_TOPIC);
    }

    public byte[] getBytes(){
        return (byte[])record.get(KW_BTS);
    }

    public String toString(){
        return "{:topic " + getTopic() + ", :partition " + getPartition() + ", :offset " + getOffset() + ", :bts " + getBytes() + "}";
    }
}
