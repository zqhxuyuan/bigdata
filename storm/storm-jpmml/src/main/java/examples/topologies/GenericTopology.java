package examples.topologies;

import org.jpmml.storm.bolts.GenericBolt;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.topology.TopologyBuilder;
import examples.spouts.AlgorithmType;
import examples.spouts.NaiveBayesSpout;
import examples.spouts.SampleDataGeneratorSpout;

/**
 * Storm topology show-casing use of a Generic Bolt.
 *  
 * @author Jayati Tiwari
 */
public class GenericTopology {

	public static void main(String[] args) throws AlreadyAliveException {

		int expectedNumberOfArgs = 2;
		// check input arguments
		if(args.length != expectedNumberOfArgs){
			System.out.println("Usage: numberOfMessagesToEmit pmmlModelFile");
		}
		
		// number of messages the spout should emit
		int numberOfMsgsToEmit = Integer.parseInt(args[0]);
		// location of the pmml model file
		String pmmlModelFile = args[1];
		String testInputFileLoc = args[2];
		int numberOfSpouts = Integer.parseInt(args[3]);
		int numberOfBolts = Integer.parseInt(args[4]);
		
		// Create the topology 
		TopologyBuilder builder = new TopologyBuilder();	
		builder.setSpout("NaiveBayesSpout", new NaiveBayesSpout(testInputFileLoc, numberOfMsgsToEmit), numberOfSpouts);
		builder.setBolt("genericBolt", new GenericBolt(pmmlModelFile), numberOfBolts).shuffleGrouping("NaiveBayesSpout");

		// Run the topology locally
		Config conf = new Config();
		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("GenericTestTopology", conf, builder.createTopology());

	}

}
