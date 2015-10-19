package org.jpmml.storm.bolts;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import javax.xml.bind.JAXBException;

import org.apache.commons.math3.exception.NullArgumentException;
import org.apache.log4j.Logger;
import org.dmg.pmml.Field;
import org.dmg.pmml.FieldName;
import org.dmg.pmml.IOUtil;
import org.dmg.pmml.OutputField;
import org.dmg.pmml.PMML;
import org.jpmml.evaluator.Evaluator;
import org.jpmml.evaluator.ModelEvaluatorFactory;
import org.jpmml.manager.PMMLManager;
import org.xml.sax.SAXException;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

/**
 * Generic Storm Bolt for running JPMML implementations of algorithms.
 * 
 * Being "generic", it expects the input tuple of data type "String" containing all the input data fields separated by a comma.
 * 
 * For example, if your input data has 4 data fields, namely "Sepal.Length", "Sepal.Width", "Petal.Length" and "Petal.Width" and their respective values are 2.2, 4.2 , 5.5 and 3.5
 *  
 * The expected input tuple would be "2.2,4.2,5.5,3.5".
 * 
 * If your data type isn't this format, use the CustomizableBolt and specify the required data pre-processing.
 * 
 * @author Jayati Tiwari
 */
public class GenericBolt extends BaseRichBolt {

	private static final long serialVersionUID = -6169008967938687152L;
	private static transient Logger logger = Logger.getLogger(GenericBolt.class);
	private OutputCollector collector;
	private Evaluator evaluator;
	private List<FieldName> activeFields; 
	private List<FieldName> resultFields;
	
	static FileWriter file;
	static PrintWriter outputFile;
	
	static int i = 0;
	private static String classificationOutputFile = "/home/pravesh/workspace/jpmml-storm/data/lr_bolt_result";

	/*
	 * Location of the PMML model file
	 */
	private String pmmlModelFile;

	public GenericBolt(String pmmlModelFile) {
		super();
		this.pmmlModelFile = pmmlModelFile;		
		if(this.pmmlModelFile == null){
			throw new NullArgumentException();
		}
		// validate the pmmlModelFile locations for existence 
		File pmmlModelFileObject = new File(pmmlModelFile);
		if(!pmmlModelFileObject.exists()){
			throw new IllegalArgumentException();
		}
	}

	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		this.collector = collector;
		// unmarshal the model file to extract the model object
		PMML pmml = null;
		try {
			pmml = IOUtil.unmarshal(new File(pmmlModelFile));
		} catch (IOException e) {
			logger.error("IOException occured while loading the PMML model.");
		} catch (SAXException e) {
			logger.error("SAXException occured while loading the PMML model.");
		} catch (JAXBException e) {
			logger.error("JAXBException occured while loading the PMML model.");
		} 
		
		// create the PMMLManager object 
		PMMLManager pmmlManager = new PMMLManager(pmml);

		// load the default model and initialize the field details 
		evaluator = (Evaluator)pmmlManager.getModelManager(null, ModelEvaluatorFactory.getInstance());
		activeFields = evaluator.getActiveFields();
		resultFields = evaluator.getOutputFields();
		logger.info("Bolt initialized.");
	}

	/**
	 * (non-Javadoc)
	 * @see backtype.storm.task.IBolt#execute(backtype.storm.tuple.Tuple)
	 */
	@Override 
	public void execute(Tuple input) {
		if(input != null){
			// collect the input tuple in a variable and process it 
			String inputTuple = input.getString(0);
			try {
				file = new FileWriter(classificationOutputFile
						+ (int) (Math.random() * 100));
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			outputFile = new PrintWriter(file);
			outputFile.println(inputTuple);
			outputFile.flush();
			System.out.println("<<<<<<<<<<<<<<<<<<<<  "+ i++ +inputTuple);
			String[] var = inputTuple.split(",");
			int counter = 0;
			Map<FieldName, Object> arguments = new LinkedHashMap<FieldName, Object>();
			Object inputField;

			// populate the map with field names of the active fields and their corresponding values
			for(FieldName activeField : activeFields){
				// check if the input tuple has all fields required, if not catch the thrown ArrayIndexOutOfBoundsException
				try{
					inputField = var[counter];
					} catch (ArrayIndexOutOfBoundsException a) {
						logger.error("Tuple does not contain all the required field values");
						return;
					}
				// if reached here, i.e. no exception is thrown, add the field value with its corresponding active field name to the map
				try{
					arguments.put(activeField, evaluator.prepare(activeField, inputField));
				} catch (org.jpmml.evaluator.InvalidResultException exception){
					logger.error("InvalidResultException occurred: ERROR processing the following record: One or more of your input data field value does not match the expected set of values defined in your PMML model.");
				}
				counter++;
			}
			// evaluate the results using the evaluator
			Map<FieldName, ?> result = evaluator.evaluate(arguments);

			// Write the results to the output log file
			logger.info("Results for input tuple: " + inputTuple);
			for(FieldName resultField : resultFields){
				OutputField outputField = evaluator.getOutputField(resultField);
				Object outputValue = result.get(resultField);
				logger.info("displayName=" + getDisplayName(outputField, resultField) + ": " + outputValue); 
			}
			// emit the result map
			collector.emit(new Values(result));
		}
	}

	/**
	 * Returns the display name of the input field as specified in the pmml model
	 */
	static public String getDisplayName(Field field, FieldName fieldName){
		String result = field.getName().getValue();
		if(result == null){
			result = fieldName.getValue();
		}
		return result;
	}


	/**
	 * (non-Javadoc)
	 * @see backtype.storm.topology.IComponent#declareOutputFields(backtype.storm.topology.OutputFieldsDeclarer)
	 */
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declareStream("evaluatorResultStream", new Fields("ResultMap"));
	}

}
