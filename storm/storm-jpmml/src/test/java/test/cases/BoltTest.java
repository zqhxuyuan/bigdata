package test.cases;

import org.jpmml.storm.bolts.CustomizableBolt;
import org.jpmml.storm.bolts.GenericBolt;
import org.junit.Test;

import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import examples.spouts.SampleDataGeneratorSpout;
import examples.topologies.TestDataPreProcessor;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

/*
 * Test case for testing the Generic and Customizable in case of invalid input data and implementation of the declareOutputFields() method
 * 
 * @author Jayati Tiwari
 */

public class BoltTest {

	private static final String ANY_NON_EXISTING_LOCATION = "does_not_exist";
	private static final String ANY_EXISTING_LOCATION = "/home/jayati/Documents/iLabsRnD/Spark+JPMML/PMMLFiles/naive1.pmml";

	@Test(expected = IllegalArgumentException.class)  
	public void genericBoltShouldThrowIAEIfPMMLModelLocationDoesNotExist() {
		new GenericBolt(ANY_NON_EXISTING_LOCATION); 
	}
	
	@Test  
	public void genericBoltShouldInitializeNormally() {
		new GenericBolt(ANY_EXISTING_LOCATION); 
	}

	@Test
	public void genericBoltShouldDeclareOutputFields() {
		// given
		OutputFieldsDeclarer declarer = mock(OutputFieldsDeclarer.class);
		GenericBolt bolt = new GenericBolt(ANY_EXISTING_LOCATION);

		// when
		bolt.declareOutputFields(declarer);

		// then
		verify(declarer, times(1)).declareStream(any(String.class), any(Fields.class));
	}
	
	@Test(expected = IllegalArgumentException.class)  
	public void customizableBoltShouldThrowIAEIfPMMLModelLocationDoesNotExist() {
		// given
		TestDataPreProcessor dataPreProcessor = new TestDataPreProcessor(ANY_EXISTING_LOCATION);
		// when
		new CustomizableBolt(ANY_NON_EXISTING_LOCATION, dataPreProcessor); 
	}

	@Test  
	public void customizableBoltShouldInitializeNormally() {
		// given
		TestDataPreProcessor dataPreProcessor = new TestDataPreProcessor(ANY_EXISTING_LOCATION);
		// when
		new CustomizableBolt(ANY_EXISTING_LOCATION, dataPreProcessor); 
	}
	
	@Test  
	public void customizableBoltShouldDeclareOutputFields() {
		// given
		TestDataPreProcessor dataPreProcessor = new TestDataPreProcessor(ANY_EXISTING_LOCATION);
		OutputFieldsDeclarer declarer = mock(OutputFieldsDeclarer.class);
		CustomizableBolt bolt = new CustomizableBolt(ANY_EXISTING_LOCATION, dataPreProcessor);

		// when
		bolt.declareOutputFields(declarer);

		// then
		verify(declarer, times(1)).declareStream(any(String.class), any(Fields.class));
	}
}