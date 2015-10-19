package storm.kafka.demo;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import backtype.storm.utils.Utils;
import storm.kafka.*;

import java.io.FileNotFoundException;
import java.util.ArrayList;

public class MyTopology {

	public static void main(String[] args)
            throws AlreadyAliveException, InvalidTopologyException, InterruptedException, FileNotFoundException {
        String topic = "kafkaToptic";
        String zkRoot = "/kafkastorm";
        String spoutId = "word";  //读取的status会被存在，/kafkastorm/id下面，所以id类似consumer group

        //Kafka配置在zookeeper上的节点
        BrokerHosts brokerHosts = new ZkHosts("localhost");
		SpoutConfig spoutConf = new SpoutConfig(brokerHosts, topic, zkRoot, spoutId);

		spoutConf.scheme = new SchemeAsMultiScheme(new StringScheme());
		//spoutConf.forceStartOffsetTime(-2);
        spoutConf.forceFromStart = false;
        spoutConf.startOffsetTime = kafka.api.OffsetRequest.EarliestTime();
        spoutConf.metricsTimeBucketSizeInSecs = 6;

        //只有在local模式下需要记录读取状态时，才需要设置
		spoutConf.zkServers = new ArrayList<String>() {
			{
				add("localhost");
			}
		};
		spoutConf.zkPort = 2181;

		TopologyBuilder builder = new TopologyBuilder();
        //KafkaSpout是数据源, 会从Kafka消息队列中取出消息,发射出去
		builder.setSpout("word-reader", new KafkaSpout(spoutConf), 3);
		//builder.setBolt("bolt", new PrintBolt()).shuffleGrouping("spout");
		builder.setBolt("word-normalizer", new WordNormalizer()).shuffleGrouping("word-reader");
		builder.setBolt("word-counter", new WordCounter(), 1).fieldsGrouping("word-normalizer", new Fields("word"));

		Config conf = new Config();
		conf.put(Config.NIMBUS_HOST, "localhost");
	    //conf.put(Config.NIMBUS_THRIFT_PORT, 6627);
		conf.setNumWorkers(3);
		//conf.setDebug(true);

		if (args != null && args.length > 0) {
			conf.setNumWorkers(3);
			StormSubmitter.submitTopology("wordcountTopology", conf, builder.createTopology());
		} else {
            System.out.println("-----------------------");
			conf.setMaxTaskParallelism(3);

			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology("local-host", conf, builder.createTopology());

			Thread.sleep(20000);

			//cluster.shutdown();
		}
		Utils.sleep(10000);
	}
}
