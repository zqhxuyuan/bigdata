package examples.topologies;

import org.jpmml.storm.bolts.SVMIrisBolt;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.TopologyBuilder;
import examples.spouts.SVMIrisSpout;

public class SVMIrisTopology {

	public static void main(String[] args) throws AlreadyAliveException, InvalidTopologyException {

		TopologyBuilder builder = new TopologyBuilder();	
		String testInputFileLoc = "/home/pravesh/workspace/jpmml-storm/data/IrisDataSet.txt";

		builder.setSpout("spout", new SVMIrisSpout(testInputFileLoc, 1000), 1);
		builder.setBolt("bolt", new SVMIrisBolt(), 1).shuffleGrouping("spout", "irisDataSetStream");

		Config conf = new Config();
		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("topology", conf, builder.createTopology());

	}

}