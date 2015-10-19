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
import org.apache.log4j.spi.LoggerFactory;
import org.dmg.pmml.Field;
import org.dmg.pmml.FieldName;
import org.dmg.pmml.IOUtil;
import org.dmg.pmml.OutputField;
import org.dmg.pmml.PMML;
import org.jpmml.evaluator.Evaluator;
import org.jpmml.evaluator.ModelEvaluatorFactory;
import org.jpmml.manager.PMMLManager;
import org.jpmml.storm.data.preprocessing.DataPreProcessingCustomizer;
import org.xml.sax.SAXException;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

/**
 * Customizable Storm Bolt for running JPMML implementations of algorithms.
 * 
 * Being "customizable", it expects a user input on how to process the tuple for the spout to generate the map of fields and values to be passed to the evaluator.
 * 
 * If your input data stream, has some redundant fields that you need to filter or the fields are separated on a different delimiter other than a comma (as used by GenericBolt), then you need to use this bolt.
 * 
 * You need to create a class implementing the Interface DataPreProcessingCustomizer and as per your specification for pre-processing, implement the preProcessTuple() method. 
 * 
 * An object of this class needs to be passed to the constructor of the CustomizableBolt.
 * 
 * For a working example, refer to test.example.topologies.CustomizableTopology
 *   
 * @author Jayati Tiwari
 */
public class CustomizableBolt extends BaseRichBolt {

	private static final long serialVersionUID = 1680834638402245980L;
	private static transient Logger logger = Logger.getLogger(CustomizableBolt.class);
	private Evaluator evaluator;
	private List<FieldName> resultFields;
	private static OutputCollector collector;

	/*
	 * Location of the PMML model file
	 */
	private String pmmlModelFile;

	/*
	 * Object of a class implementing the DataPreProcessingCustomizer interface. This object's implementation of the "preProcessTuple()" method is used by the bolt
	 */
	private static transient DataPreProcessingCustomizer preProcessorObject;

	public CustomizableBolt(String pmmlModelFile, DataPreProcessingCustomizer preProcessorObject) {
		super();
		this.pmmlModelFile = pmmlModelFile;
		this.preProcessorObject = preProcessorObject;
		if(this.pmmlModelFile == null){
			throw new NullArgumentException();
		}
		// validate the pmmlModelFile locations for existence 
		File pmmlModelFileObject = new File(pmmlModelFile);
		if(!pmmlModelFileObject.exists()){
			throw new IllegalArgumentException();
		}
	}

	/**
	 * (non-Javadoc)
	 * @see backtype.storm.task.IBolt#prepare(java.util.Map, backtype.storm.task.TopologyContext, backtype.storm.task.OutputCollector)
	 */
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
		resultFields = evaluator.getOutputFields();
		logger.info("Bolt initialized.");
	}

	/**
	 * (non-Javadoc)
	 * @see backtype.storm.task.IBolt#execute(backtype.storm.tuple.Tuple)
	 */
	@Override
	public void execute(Tuple input) {
		// invoke the preProcessTuple() method and pass the input tuple to it, which returns the map of field names and values to be passed to the evaluate method.
		Map<FieldName, Object> arguments = new LinkedHashMap<FieldName, Object>();
		try{
			arguments = (Map<FieldName, Object>) preProcessorObject.preProcessTuple(input);
		} catch (org.jpmml.evaluator.InvalidResultException exception){
			logger.error("INVALID INPUT: ERROR processing the following record: One or more of your input data field value does not match the expected set of values defined in your PMML model."); 
		}

		// evaluate the results using the evaluator
		Map<FieldName, ?> result = evaluator.evaluate(arguments);

		// Write the results to the output log file
		logger.info("Results for this tuple: ");
		for(FieldName resultField : resultFields){
			OutputField outputField = evaluator.getOutputField(resultField);
			Object outputValue = result.get(resultField);
			logger.info("displayName=" + getDisplayName(outputField, resultField) + ": " + outputValue); 
		}
		// emit the result map
		collector.emit(new Values(result));
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
