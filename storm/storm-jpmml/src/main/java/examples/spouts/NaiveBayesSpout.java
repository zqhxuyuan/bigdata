package examples.spouts;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

import com.google.common.base.Charsets;
import com.google.common.io.Resources;

/*
 * Spout for testing Logistic Regression. Trains the model and then emits records to be classified.
 * 
 * @author Jayati
 */

public class NaiveBayesSpout implements IRichSpout {
	SpoutOutputCollector _collector;
	static int counter = 1;
	private static BufferedReader br;
	static FileWriter file;
	static PrintWriter outputFile;
	static ArrayList<String> inputRecordsList = new ArrayList<String>();

	// spout output log location for the cluster
	private static String classificationOutputFile = "/home/pravesh/workspace/jpmml-storm/data/lr_spout_result";

	// local fs location for output file
	// private static String classificationOutputFile =
	// "/home/jayati/Documents/iLabsRnD/LogisticRegression/Storm/lr_spout_result";

	/*
	 * CSV file to be iterated to supply test records for classification
	 */
	String testInputFileLoc;

	/*
	 * Number of records to be emitted
	 */
	long numberOfMsgsToEmit;

	public NaiveBayesSpout(String testInputFileLoc, long numberOfMsgsToEmit) {
		super();
		this.testInputFileLoc = testInputFileLoc;
		this.numberOfMsgsToEmit = numberOfMsgsToEmit;
	}

	@Override
	public void open(Map conf, TopologyContext context,
			SpoutOutputCollector collector) {
		_collector = collector;
		try {
			br = open(this.testInputFileLoc);
			String line = br.readLine();
			while (line != null) {
				// System.out.println(">>>>"+line);
				inputRecordsList.add(line);
				line = br.readLine();
			}
			file = new FileWriter(classificationOutputFile
					+ (int) (Math.random() * 100));
			outputFile = new PrintWriter(file);
			outputFile.println("Spout has been initialized.");
			outputFile.flush();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	@Override
	public void close() {
		// TODO Auto-generated method stub

	}

	@Override
	public void activate() {
		// TODO Auto-generated method stub

	}

	@Override
	public void deactivate() {
		// TODO Auto-generated method stub

	}

	@Override
	public void nextTuple() {
		if (counter <= numberOfMsgsToEmit) {
			++counter;
			Random r = new Random();
			String line = inputRecordsList.get(r.nextInt(inputRecordsList
					.size()));
			_collector.emit(new Values(line),
					UUID.randomUUID());

		}

	}

	@Override
	public void ack(Object msgId) {
		// TODO Auto-generated method stub

	}

	@Override
	public void fail(Object msgId) {
		// TODO Auto-generated method stub

	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("record"));
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}

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
