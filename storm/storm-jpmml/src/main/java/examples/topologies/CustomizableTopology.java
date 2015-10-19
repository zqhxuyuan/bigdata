package examples.topologies;

import org.jpmml.storm.bolts.CustomizableBolt;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.topology.TopologyBuilder;
import examples.spouts.AlgorithmType;
import examples.spouts.SampleDataGeneratorSpout;

/**
 * Storm topology show-casing use of a Customizable Bolt for the Naive Bayes algorithm using the Iris dataset.
 * 
 * PMML model file for this topology can be found at src/main/resources/NaiveBayesIris.pmml
 * 
 * @author Jayati Tiwari
 */
public class CustomizableTopology {

	public static void main(String[] args) throws AlreadyAliveException{
		int expectedNumberOfArgs = 2;
		// check input arguments
		if(args.length != expectedNumberOfArgs){
			System.out.println("Usage: numberOfMessagesToEmit pmmlModelFile");
		}
		
		// number of messages the spout should emit
		int numberOfMsgsToEmit = Integer.parseInt(args[0]);
		// location of the pmml model file
		String pmmlModelFile = args[1];

		// Implementation of the DataPreProcessingCustomizer interface needed for the Customizable bolt
		TestDataPreProcessor dataPreProcessor = new TestDataPreProcessor(pmmlModelFile);

		// Create the topology 
		TopologyBuilder builder = new TopologyBuilder();	
		builder.setSpout("sampleDataSpout", new SampleDataGeneratorSpout(AlgorithmType.SUPPORTVECTORMACHINEAUDIT, numberOfMsgsToEmit), 1);
		builder.setBolt("customizableBolt", new CustomizableBolt(pmmlModelFile, dataPreProcessor), 1).shuffleGrouping("sampleDataSpout", "sampleDataSetStream");

		// Run the topology locally
		Config conf = new Config();
		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("CustomizableTopology", conf, builder.createTopology());

	}
}
