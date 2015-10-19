package com.zqh.storm.wiki;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.testing.TestWordSpout;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.utils.Utils;

public class ExclamationTopology {

	public static void main(String[] args) {
		TopologyBuilder builder = new TopologyBuilder();        
		builder.setSpout("words", new TestWordSpout(), 10);        
		builder.setBolt("exclaim1", new ExclamationBolt(), 3)
		        .shuffleGrouping("words");
		builder.setBolt("exclaim2", new ExclamationBolt(), 2)
		        .shuffleGrouping("exclaim1");
		
		Config conf = new Config();
		conf.setDebug(true);
		conf.setNumWorkers(2);

		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("test", conf, builder.createTopology());
		Utils.sleep(10000);
		cluster.killTopology("test");
		cluster.shutdown();
	}
}
