package kafka_clj.util;

import kafka.admin.AdminUtils;
import kafka.admin.RackAwareMode;
import kafka.metrics.KafkaMetricsReporter;
import kafka.server.KafkaConfig;
import kafka.server.KafkaServer;
import kafka.utils.ZkUtils;
import org.I0Itec.zkclient.ZkClient;
import scala.Option$;
import scala.collection.immutable.Seq;
import scala.collection.mutable.ArraySeq;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Used for unit testing against kafka.<br/>
 * Starts N kafka nodes, creates topics.<br/>
 * Use startup and shutdown to manage the cluster.<br/>
 */
public class EmbeddedKafkaCluster {
    private final List<Integer> ports;
    private final String zkConnection;
    private final Properties baseProperties;

    private final List<KafkaServer> brokers;
    private final List<File> logDirs;

    /**
     * Creates an embedded cluster with a single node
     * @param zkConnection
     */
    public EmbeddedKafkaCluster(String zkConnection) {
        this(zkConnection, new Properties());
    }

    /**
     * Creates an embedded cluster with a single node
     * @param zkConnection
     * @param baseProperties
     */
    public EmbeddedKafkaCluster(String zkConnection, Properties baseProperties) {
        this(zkConnection, baseProperties, Collections.singletonList(-1));
    }

    /**
     *Creates an embedded cluster with ports.size() nodes
     * @param zkConnection
     * @param baseProperties
     * @param ports if a port value is -1 a randomly available port is selected
     */
    public EmbeddedKafkaCluster(String zkConnection, Properties baseProperties, List<Integer> ports) {
        this.zkConnection = zkConnection;
        this.ports = resolvePorts(ports);
        this.baseProperties = baseProperties;
        this.brokers = new CopyOnWriteArrayList<>();
        this.logDirs = new CopyOnWriteArrayList<>();
    }


    public ZkUtils getZkUtils(){
        for(KafkaServer server : brokers){
            return server.zkUtils();
        }
        return null;
    }


    public ZkClient getZkClient(){
        for(KafkaServer server : brokers){
            return server.zkUtils().zkClient();
        }
        return null;
    }

    /**
     * Create the topics in the cluster with replication==1, and partition == 2
     * @param topics
     */
    public void createTopics(Collection<String> topics){
        createTopics(topics, 2, 1);
    }

    /**
     * Create topics with partitions=partition and replication-factory=replication.<br/>
     * Note that replication 1 in kafka means no replication at all.
     * @param topics
     */
    public void createTopics(Collection<String> topics, int partition, int replication){
        for(String topic : topics){
            AdminUtils.createTopic(getZkUtils(), topic, partition, replication, new Properties(), new RackAwareMode.Safe$().MODULE$);
        }
    }

    /**
     * True if the topic exists, false it otherwise
     * @param topic
     * @return
     */
    public boolean topicExists(String topic)
    {
        return AdminUtils.topicExists(getZkUtils(), topic);
    }

    /**
     * For each port if -1 a randomly available port is selected.
     * @param ports
     * @return
     */
    private List<Integer> resolvePorts(List<Integer> ports) {
        List<Integer> resolvedPorts = new ArrayList<Integer>();
        for (Integer port : ports) {
            resolvedPorts.add(resolvePort(port));
        }
        return new CopyOnWriteArrayList<>(resolvedPorts);
    }

    /**
     * If port == -1 get the next available port
     * @param port
     * @return
     */
    private int resolvePort(int port) {
        if (port == -1) {
            return TestUtils.getAvailablePort();
        }
        return port;
    }

    private String constructBrokerList(List<Integer> ports) {
        StringBuilder sb = new StringBuilder();
        for (Integer port : ports) {
            if (sb.length() > 0) {
                sb.append(",");
            }
            sb.append("localhost:").append(port);
        }
        return sb.toString();
    }

    /**
     * Startup N KafkaServer(s) to form a single cluster
     */
    public void startup() {
        for (int i = 0; i < ports.size(); i++) {
            _addBroker(i);
        }
    }

    /**
     * Get the properties "metadata.broker.list" and "zookeeper.connect"
     * @return
     */
    public Properties getProps() {
        Properties props = new Properties();
        props.putAll(baseProperties);
        props.put("metadata.broker.list", constructBrokerList(ports));
        props.put("zookeeper.connect", zkConnection);
        return props;
    }

    /**
     * Get the comma separated brokers connection string
     * @return
     */
    public String getBrokerList() {
        return constructBrokerList(ports);
    }

    /**
     * Return all the ports for the kafka cluster
     * @return
     */
    public List<Integer> getPorts() {
        return Collections.unmodifiableList(ports);
    }

    /**
     * Returns the zookeeper connection string
     * @return
     */
    public String getZkConnection() {
        return zkConnection;
    }

    /**
     * Shutdown a random broker, used for testing
     */
    public KafkaServer shutdownRandom(){
        int i = new Random().nextInt(brokers.size());
        KafkaServer broker = brokers.get(i);
        shutdownBroker(broker);
        broker.awaitShutdown();

        return broker;
    }

    /**
     * Add a new broker at the first available port to the
     * current cluster.
     */
    public void addBroker()
    {
        ports.add(resolvePort(-1));
        _addBroker(ports.size()-1);
    }


    private void shutdownBroker(KafkaServer broker){
        try {
            System.out.println("Shutdown broker " + broker);
            broker.shutdown();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * Shutdown all of the kafka brokers for this cluster
     */
    public void shutdown() {
        for (KafkaServer broker : brokers) {
            shutdownBroker(broker);
        }
        for (File logDir : logDirs) {
            try {
                TestUtils.deleteFile(logDir);
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            }
        }
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("EmbeddedKafkaCluster{");
        sb.append("brokerList='").append(constructBrokerList(ports)).append('\'');
        sb.append('}');
        return sb.toString();
    }


    private void _addBroker(int i) {
        Integer port = ports.get(i);
        File logDir = TestUtils.constructTempDir("kafka-local");

        Properties properties = getProperties(i, port, logDir);

        KafkaServer broker = startBroker(properties);

        brokers.add(broker);
        logDirs.add(logDir);
    }

    private Properties getProperties(int i, Integer port, File logDir) {
        Properties properties = new Properties();
        properties.putAll(baseProperties);
        properties.setProperty("zookeeper.connect", zkConnection);
        properties.setProperty("broker.id", String.valueOf(i + 1));
        properties.setProperty("host.name", "localhost");
        properties.setProperty("port", Integer.toString(port));
        properties.setProperty("log.dir", logDir.getAbsolutePath());
        System.out.println("EmbeddedKafkaCluster: local directory: " + logDir.getAbsolutePath());
        properties.setProperty("log.flush.interval.messages", String.valueOf(1));
        return properties;
    }


    private KafkaServer startBroker(Properties props) {
        KafkaServer server = new KafkaServer(new KafkaConfig(props), new SystemTime(), Option$.MODULE$.empty(), new ArraySeq<>(0));
        server.startup();
        return server;
    }

}