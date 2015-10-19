package org.jpmml.storm.bolts;

import java.io.EOFException;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import javax.xml.bind.JAXBException;

import org.dmg.pmml.DataField;
import org.dmg.pmml.DataType;
import org.dmg.pmml.Field;
import org.dmg.pmml.FieldName;
import org.dmg.pmml.IOUtil;
import org.dmg.pmml.OutputField;
import org.dmg.pmml.PMML;
import org.jpmml.evaluator.Evaluator;
import org.jpmml.evaluator.EvaluatorUtil;
import org.jpmml.evaluator.ModelEvaluatorFactory;
import org.jpmml.manager.PMMLManager;
import org.xml.sax.SAXException;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

public class NaiveBayesIrisBolt extends BaseRichBolt {

	static Calendar cal;
	static PrintWriter outputFileWriter;
	static FileWriter file;
	static long counter = 0;
	private static String startTime;
	final static SimpleDateFormat sdf = new SimpleDateFormat("HH:mm:ss");


	private String pmmlModelFile = "/home/pravesh/workspace/jpmml-storm/src/main/resources/NaiveBayesIrisEdited.pmml";
	private String outputLogsDirectory = "/home/pravesh/workspace/jpmml-storm/data/output";
	
	private static Evaluator evaluator;
	private static List<FieldName> activeFields; 
	private static List<FieldName> predictedFields;
	private static List<FieldName> resultFields;

	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {

		try {
			file = new FileWriter(outputLogsDirectory + "/bolt_result_" + (int)(Math.random() * 100));
		} catch (IOException e1) {
			System.out.println("Could not create the bolt output file.");
			e1.printStackTrace();
		}
		outputFileWriter = new PrintWriter(file);
		cal = Calendar.getInstance();
		cal.getTime();


		PMML pmml = null;
		try {
			pmml = IOUtil.unmarshal(new File(pmmlModelFile));
		} catch (IOException e) {
			e.printStackTrace();
		} catch (SAXException e) {
			e.printStackTrace();
		} catch (JAXBException e) {
			e.printStackTrace();
		}

		PMMLManager pmmlManager = new PMMLManager(pmml);

		// Load the default model
		evaluator = (Evaluator)pmmlManager.getModelManager(null, ModelEvaluatorFactory.getInstance());

		activeFields = evaluator.getActiveFields();
		predictedFields = evaluator.getPredictedFields();
		resultFields = evaluator.getOutputFields();


		outputFileWriter.println("Bolt initialized.");
		outputFileWriter.flush();

		cal = Calendar.getInstance();
		cal.getTime();
		startTime = sdf.format(cal.getTime());

	}

	public void execute(Tuple input) {
		// collect the input tuple in a variable 
		String inputTuple = input.getString(0);
		// split the input record on ","	
		String[] var = inputTuple.split(",");
		int counter = 0;

		Map<FieldName, Object> arguments = new LinkedHashMap<FieldName, Object>();

		for(FieldName activeField : activeFields){
			Object inputField = var[counter];

			if(inputField == null){
				try {
					throw new EOFException();
				} catch (EOFException e) {
					e.printStackTrace();
				}
			}

			arguments.put(activeField, evaluator.prepare(activeField, inputField));

			counter++;
		}

		Map<FieldName, ?> result = evaluator.evaluate(arguments);


		for(FieldName predictedField : predictedFields){
			DataField dataField = evaluator.getDataField(predictedField);

			Object predictedValue = result.get(predictedField);

			outputFileWriter.println("displayName=" + getDisplayName(dataField, predictedField) + ": " + EvaluatorUtil.decode(predictedValue));
			outputFileWriter.flush(); 
		}

		for(FieldName resultField : resultFields){
			OutputField outputField = evaluator.getOutputField(resultField);

			Object outputValue = result.get(resultField);

			outputFileWriter.println("displayName=" + getDisplayName(outputField, resultField) + ": " + outputValue);
			outputFileWriter.flush(); 
		}

	}

	static
	private String getDisplayName(Field field, FieldName fieldName){
		String result = field.getDisplayName();
		if(result == null){
			result = fieldName.getValue();
		}

		return result;
	}

	static
	private String getDataType(Field field){
		DataType dataType = field.getDataType();

		return dataType.name();
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("test"));

	}

}
