package test.cases;

import org.junit.Test;

import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import examples.spouts.AlgorithmType;
import examples.spouts.SampleDataGeneratorSpout;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

/*
 * Test case for testing the Sample spout in case of invalid input data and implementation of the declareOutputFields() method
 * 
 * @author Jayati Tiwari
 */

public class SampleSpoutTest {


	@Test  
	public void spoutShouldInitializeNormally() {
		int numberOfMsgsToEmit = 10;
		AlgorithmType[] types = AlgorithmType.values();
		for (AlgorithmType type: types){
			new SampleDataGeneratorSpout(type, numberOfMsgsToEmit);
		}
	}

	@Test
	public void spoutShouldDeclareOutputFields() {
		// given
		OutputFieldsDeclarer declarer = mock(OutputFieldsDeclarer.class);
		SampleDataGeneratorSpout spout = new SampleDataGeneratorSpout(AlgorithmType.KMEANSIRIS, 2);

		// when
		spout.declareOutputFields(declarer);

		// then
		verify(declarer, times(1)).declareStream(any(String.class), any(Fields.class));
	}
}