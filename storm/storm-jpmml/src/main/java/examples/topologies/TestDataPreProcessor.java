package examples.topologies;

import java.io.EOFException;
import java.io.File;
import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import javax.xml.bind.JAXBException;

import org.apache.log4j.Logger;
import org.dmg.pmml.FieldName;
import org.dmg.pmml.IOUtil;
import org.dmg.pmml.PMML;
import org.jpmml.evaluator.Evaluator;
import org.jpmml.evaluator.ModelEvaluatorFactory;
import org.jpmml.manager.PMMLManager;
import org.jpmml.storm.data.preprocessing.DataPreProcessingCustomizer;
import org.xml.sax.SAXException;

import backtype.storm.tuple.Tuple;

/**
 * A Sample data pre-processor used in the Customizable Bolt.
 * 
 * @author Jayati Tiwari
 *
 */
public class TestDataPreProcessor implements DataPreProcessingCustomizer{
	
	private static final long serialVersionUID = -4846752171394413751L;
	private final Logger logger = Logger.getLogger(TestDataPreProcessor.class);
	private static Evaluator evaluator;
	private static List<FieldName> activeFields; 
	
	public TestDataPreProcessor(String pmmlModelFile) {
		super();
		// unmarshal the model file
		PMML pmml = null;
		try {
			pmml = IOUtil.unmarshal(new File(pmmlModelFile));
		} catch (IOException e) {
			logger.error("TestDataPreProcessor: IOException occured while loading the PMML model.");
			e.printStackTrace();
		} catch (SAXException e) {
			logger.error("TestDataPreProcessor: SAXException occured while loading the PMML model.");
			e.printStackTrace();
		} catch (JAXBException e) {
			logger.error("TestDataPreProcessor: JAXBException occured while loading the PMML model.");
		} 

		PMMLManager pmmlManager = new PMMLManager(pmml);
		// Load the default model
		evaluator = (Evaluator)pmmlManager.getModelManager(null, ModelEvaluatorFactory.getInstance());
		activeFields = evaluator.getActiveFields();
	}

	/**
	 * (non-Javadoc)
	 * @see org.jpmml.storm.data.preprocessing.DataPreProcessingCustomizer#preProcessTuple(backtype.storm.tuple.Tuple)
	 */
	@Override
	public Map<FieldName, Object> preProcessTuple(Tuple tuple) {
		// collect the input tuple in a variable 
		String inputTuple = tuple.getString(0);
		// split the input record on ","	
		String[] var = inputTuple.split(",");
		int counter = 0;
		
		Map<FieldName, Object> arguments = new LinkedHashMap<FieldName, Object>();
		Object inputField;

		for(FieldName activeField : activeFields){
			inputField = var[counter];

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
		return arguments;
	}
}
