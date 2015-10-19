package examples.spouts;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Map;
import java.util.UUID;

import com.google.common.base.Charsets;
import com.google.common.io.Resources;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

/*
 * Spout for generating stream of records to be passed as input to the SVM bolt.
 */

public class SVMIrisSpout extends BaseRichSpout {

	private static SpoutOutputCollector _collector;
	private static int counter = 1;
	private static int index = 0;
	private static int numberOfElementsInList = 0;
	private static BufferedReader br;
	private static ArrayList<String> inputRecordsList = new ArrayList<String>();

	/*
	 * CSV file to be iterated to supply test records for processing
	 */
	String inputFile;

	/*
	 * Number of records to be emitted
	 */
	long numberOfMsgsToEmit;

	public SVMIrisSpout(String inputFile, long numberOfMsgsToEmit) {
		super();
		this.inputFile = inputFile;
		this.numberOfMsgsToEmit = numberOfMsgsToEmit;
	}

	public void open(Map conf, TopologyContext context,
			SpoutOutputCollector collector) {
		_collector = collector;
		try {
			// open a buffered reader on the input file
			br = open(this.inputFile);
			String line = br.readLine();
			// read line by line and form a list of input records
			while (line != null) {
				inputRecordsList.add(line);
				line = br.readLine();
			}

			numberOfElementsInList = inputRecordsList.size();
		} catch (IOException e) {
			e.printStackTrace();
		}

	}

	public void nextTuple() {
		// check if the number of messages requested to be emitted have already
		// been emitted.
		if (counter < numberOfMsgsToEmit) {
			String record = "";
			if (index < numberOfElementsInList - 1) {
				// pick element number "index" from both the lists
				record = inputRecordsList.get(index);
				++index;
			} else {
				if (index == numberOfElementsInList - 1) {
					// pick element number "index" from the list
					record = inputRecordsList.get(index);
					index = 0;
				}
			}
			++counter;

			// entries in the input csv should look like below
			// record = "1,4.4,3.05,4.5";

			System.out.println(record);

			// emit the record
			_collector.emit("irisDataSetStream", new Values(record),
					UUID.randomUUID());
		}
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declareStream("irisDataSetStream", new Fields("iris"));
	}

	/*
	 * Opens a buffered reader object on the given input file to read it line by
	 * line
	 */
	private static BufferedReader open(String inputFile) throws IOException {
		InputStream in;
		try {
			in = Resources.getResource(inputFile).openStream();
		} catch (IllegalArgumentException e) {
			in = new FileInputStream(new File(inputFile));
		}
		return new BufferedReader(new InputStreamReader(in, Charsets.UTF_8));
	}

}
