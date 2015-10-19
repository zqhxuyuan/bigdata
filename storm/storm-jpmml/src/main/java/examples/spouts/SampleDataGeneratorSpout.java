package examples.spouts;

import java.util.Map;
import java.util.UUID;

import org.apache.log4j.Logger;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

/**
 * Spout for generating sample stream of records for various data sets.
 * 
 * You can use this spout for the following pmmls: 
 * 1. DecisionTreeAudit.pmml 
 * 2. GeneralRegressionAudit.pmml 
 * 3. NaiveBayesAudit.pmml 
 * 4. NeuralNetworkAudit.pmml 
 * 5. RandomForestAudit.pmml 
 * 6. SupportVectorMachineAudit.pmml 
 * 7. KMeansIris.pmml 
 * 8. NeuralNetworkIris.pmml
 * 9. DecisionTreeIris.pmml 
 * 10. GeneralRegressionIris.pmml 
 * 11. HierarchicalClusteringIris.pmml 
 * 12. RandomForestIris.pmml 
 * 13. RegressionIris.pmml 
 * 14. SupportVectorMachineIris.pmml 
 * 15. NaiveBayesIris.pmml
 * 16. GeneralRegressionOzone.pmml 
 * 17. NeuralNetworkOzone.pmml 
 * 18. RandomForestOzone.pmml 
 * 19. RegressionOzone.pmml 
 * 20. SupportVectorMachineOzone.pmml 
 * 
 * Specify the algorithm type using the enum SupportedAlgorithms declared in this class, in the constructor of the spout.
 * 
 * The respective pmml files can be found at:
 * https://github.com/jpmml/jpmml-evaluator
 * /tree/master/pmml-rattle/src/test/resources/pmml
 * 
 * @author Jayati Tiwari
 */

public class SampleDataGeneratorSpout extends BaseRichSpout {

	private static final long serialVersionUID = -4618926120626073235L;
	private static transient Logger logger = Logger
			.getLogger(SampleDataGeneratorSpout.class);
	private static SpoutOutputCollector collector;
	private static int counter = 1;
	private static int index = 0;
	private static int numberOfElementsInList = 0;
	private AlgorithmType algorithm;
	private String[] sampleInputRecords;

	/*
	 * Number of records to be emitted
	 */
	private long numberOfMsgsToEmit;

	public SampleDataGeneratorSpout(AlgorithmType algorithm,
			long numberOfMsgsToEmit) {
		super();
		this.algorithm = algorithm;
		this.numberOfMsgsToEmit = numberOfMsgsToEmit;
	}

	/**
	 * (non-Javadoc)
	 * 
	 * @see backtype.storm.spout.ISpout#open(java.util.Map,
	 *      backtype.storm.task.TopologyContext,
	 *      backtype.storm.spout.SpoutOutputCollector)
	 */
	@Override
	public void open(Map conf, TopologyContext context,
			SpoutOutputCollector collector) {
		this.collector = collector;
		sampleInputRecords = algorithm.getDataSetInputRecords();
		numberOfElementsInList = sampleInputRecords.length;
	}

	
	/**
	 * (non-Javadoc)
	 * 
	 * @see backtype.storm.spout.ISpout#nextTuple()
	 */
	@Override
	public void nextTuple() {
		// check if the number of messages requested to be emitted have already
		// been emitted.
		if (counter < numberOfMsgsToEmit) {
			String record = "";
			if (index < numberOfElementsInList - 1) {
				// pick element number "index" from both the lists
				record = sampleInputRecords[index];
				++index;
			} else {
				if (index == numberOfElementsInList - 1) {
					// pick element number "index" from the list
					record = sampleInputRecords[index];
					index = 0;
				}
			}
			++counter;
			// emit the record
			collector.emit("sampleDataSetStream", new Values(record),
					UUID.randomUUID());
		}
	}

	/**
	 * (non-Javadoc)
	 * 
	 * @see backtype.storm.topology.IComponent#declareOutputFields(backtype.storm.topology.OutputFieldsDeclarer)
	 */
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declareStream("sampleDataSetStream", new Fields(
				"sampleMessage"));
	}

}
