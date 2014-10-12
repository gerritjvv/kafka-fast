package kakfa_clj.core;

import clojure.lang.IPersistentMap;
import clojure.lang.Keyword;
import clojure.lang.PersistentArrayMap;

import java.util.HashMap;

/**
 * Kafka-clj Java API
 */
public class RedisConf{

    private final String host;
    private final int port;
    private final String groupName;

    public RedisConf(String host, int port, String groupName) {
        this.host = host;
        this.port = port;
        this.groupName = groupName;
    }

    public IPersistentMap toMap(){
        return PersistentArrayMap.createAsIfByAssoc(
                new Object[]{
                        Keyword.intern("host"), host,
                        Keyword.intern("port"), port,
                        Keyword.intern("group-name"), groupName
                });
    }
}
