package examples.topologies;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.TopologyBuilder;

import org.jpmml.storm.bolts.NaiveBayesIrisBolt;
import examples.spouts.NaiveBayesIrisSpout;

public class NaiveBayesIrisTopology {

	public static void main(String[] args) throws AlreadyAliveException, InvalidTopologyException {

		TopologyBuilder builder = new TopologyBuilder();	
		String testInputFileLoc = "/home/pravesh/workspace/jpmml-storm/data/IrisDataSet.txt";

		builder.setSpout("spout", new NaiveBayesIrisSpout(testInputFileLoc, 1000), 1);
		builder.setBolt("bolt", new NaiveBayesIrisBolt(), 1).shuffleGrouping("spout", "irisDataSetStream");

		Config conf = new Config();
		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("topology", conf, builder.createTopology());

	}

}
