package com.zqh.storm.getstart;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

/**
 * WordReader --> WordNormalizer -->
 * 
 * @author zqhxuyuan
 * 
 */
public class WordCount {

	public static final String TOPOLOGY_NAME = "Getting-Started-Toplogie";
	
	public static void main(String[] args) throws InterruptedException {
		// Topology definition
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("word-reader", new WordReader());
		builder.setBolt("word-normalizer", new WordNormalizer())
				.shuffleGrouping("word-reader");
		builder.setBolt("word-counter", new WordCounter(), 1)
				.fieldsGrouping("word-normalizer", new Fields("word"));

		// Configuration
		Config conf = new Config();
		args = new String[]{"/home/hadoop/data/storm/storm-intro"};
		conf.put("wordsFile", args[0]);
		conf.setDebug(false);
		
		// Topology run
		conf.put(Config.TOPOLOGY_MAX_SPOUT_PENDING, 1);
		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology(TOPOLOGY_NAME, conf, builder.createTopology());
		
		Thread.sleep(10000);
		cluster.killTopology(TOPOLOGY_NAME);
		cluster.shutdown();
	}

}
