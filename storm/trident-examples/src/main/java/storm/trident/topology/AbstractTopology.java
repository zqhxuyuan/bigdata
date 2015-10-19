package storm.trident.topology;

import backtype.storm.generated.StormTopology;
import backtype.storm.spout.SchemeAsMultiScheme;
import storm.kafka.BrokerHosts;
import storm.kafka.StringScheme;
import storm.kafka.ZkHosts;
import storm.kafka.trident.OpaqueTridentKafkaSpout;
import storm.kafka.trident.TridentKafkaConfig;
import storm.trident.util.Configuration;

/**
 *
 * @author mayconbordin
 */
public abstract class AbstractTopology {
    protected String topologyName;
    protected Configuration config;

    public AbstractTopology(String topologyName, Configuration config) {
        this.topologyName = topologyName;
        this.config = config;
    }

    public String getTopologyName() {
        return topologyName;
    }
    
    public abstract void initialize();
    public abstract StormTopology buildTopology();
    
    protected OpaqueTridentKafkaSpout createKafkaSpout(String zkHosts, String topic) {
        BrokerHosts zk = new ZkHosts(zkHosts);
        TridentKafkaConfig spoutConf = new TridentKafkaConfig(zk, topic);
        spoutConf.scheme = new SchemeAsMultiScheme(new StringScheme());
        return new OpaqueTridentKafkaSpout(spoutConf);
    }
}
