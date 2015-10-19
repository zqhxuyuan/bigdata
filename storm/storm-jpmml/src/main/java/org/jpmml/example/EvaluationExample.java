/*
 * Copyright (c) 2011 University of Tartu
 */
package org.jpmml.example;

import java.io.*;
import java.util.*;

import org.jpmml.evaluator.*;
import org.jpmml.manager.*;

import org.dmg.pmml.*;

import com.beust.jcommander.Parameter;

public class EvaluationExample extends Example {

	@Parameter (
		names = {"--model"},
		description = "The PMML file",
		required = true
	)
	private File model = null;


	static
	public void main(String... args) throws Exception {
		execute(EvaluationExample.class, args);
	}

	@Override
	public void execute() throws Exception {
		PMML pmml = IOUtil.unmarshal(this.model);

		PMMLManager pmmlManager = new PMMLManager(pmml);

		// Load the default model
		Evaluator evaluator = (Evaluator)pmmlManager.getModelManager(null, ModelEvaluatorFactory.getInstance());

		Map<FieldName, ?> arguments = readArguments(evaluator);

		Map<FieldName, ?> result = evaluator.evaluate(arguments);

		writeResult(evaluator, result);
	}

	static
	public Map<FieldName, ?> readArguments(Evaluator evaluator) throws IOException {
		Map<FieldName, Object> arguments = new LinkedHashMap<FieldName, Object>();

		List<FieldName> activeFields = evaluator.getActiveFields();
		System.out.println("Reading " + activeFields.size() + " argument(s):");

		BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));

		try {
			int line = 1;

			for(FieldName activeField : activeFields){
				DataField dataField = evaluator.getDataField(activeField);

				System.out.print(line + ") displayName=" + getDisplayName(dataField, activeField) + ", dataType=" + getDataType(dataField) + ": ");

				String input = reader.readLine();
				if(input == null){
					throw new EOFException();
				}

				arguments.put(activeField, evaluator.prepare(activeField, input));

				line++;
			}
		} finally {
			reader.close();
		}

		return arguments;
	}

	static
	public void writeResult(Evaluator evaluator, Map<FieldName, ?> result){
		int line = 1;

		System.out.println("Writing " + result.size() + " result(s):");

		List<FieldName> predictedFields = evaluator.getPredictedFields();
		for(FieldName predictedField : predictedFields){
			DataField dataField = evaluator.getDataField(predictedField);

			Object predictedValue = result.get(predictedField);

			System.out.println(line + ") displayName=" + getDisplayName(dataField, predictedField) + ": " + EvaluatorUtil.decode(predictedValue));

			line++;
		}

		List<FieldName> resultFields = evaluator.getOutputFields();
		for(FieldName resultField : resultFields){
			OutputField outputField = evaluator.getOutputField(resultField);

			Object outputValue = result.get(resultField);

			System.out.println(line + ") displayName=" + getDisplayName(outputField, resultField) + ": " + outputValue);

			line++;
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
}