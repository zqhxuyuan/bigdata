package org.jpmml.storm.data.preprocessing;

import java.io.Serializable;
import java.util.Map;

import org.dmg.pmml.FieldName;

import backtype.storm.tuple.Tuple;

/**
 * An interface to be implemented for creating a Data PreProcessor for the bolt
 * 
 * @author Jayati Tiwari
 */
public interface DataPreProcessingCustomizer {
	
	/**
	 * To be overridden by the user when using CustomizableBolt
	 */
	Map<FieldName, Object> preProcessTuple(Tuple tuple);

}
