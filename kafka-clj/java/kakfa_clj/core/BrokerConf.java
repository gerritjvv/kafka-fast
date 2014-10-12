package kakfa_clj.core;

import clojure.lang.IPersistentMap;
import clojure.lang.Keyword;
import clojure.lang.PersistentArrayMap;

/**
 * Java API for kafka-clj
 */
public class BrokerConf {

    private final String host;
    private final int port;

    public BrokerConf(String host, int port) {
        this.host = host;
        this.port = port;
    }

    public IPersistentMap toMap(){
        return PersistentArrayMap.createAsIfByAssoc(new Object[]{
                Keyword.intern("host"), host,
                Keyword.intern("port"), port
        });
    }
}
