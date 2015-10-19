/*
 * Copyright (c) 2012 University of Tartu
 */
package org.jpmml.example;

import java.io.*;
import java.util.*;

import org.jpmml.evaluator.*;
import org.jpmml.evaluator.FieldValue;
import org.jpmml.manager.*;

import org.dmg.pmml.*;

import com.beust.jcommander.Parameter;

public class CsvEvaluationExample extends Example {

	@Parameter (
		names = {"--model"},
		description = "The PMML file",
		required = true
	)
	private File model = null;

	@Parameter (
		names = {"--input"},
		description = "Input CSV file. If missing, data records will be read from System.in"
	)
	private File input = null;

	@Parameter (
		names = {"--output"},
		description = "Output CSV file. If missing, data records will be written to System.out"
	)
	private File output = null;

	@Parameter (
		names = {"--separator"},
		description = "CSV cell separator character"
	)
	private String separator = null;

	@Parameter (
		names = {"--count"},
		description = "Number of repetitions"
	)
	private int count = 1;


	static
	public void main(String... args) throws Exception {
		execute(CsvEvaluationExample.class, args);
	}

	@Override
	@SuppressWarnings (
		value = {"unused"}
	)
	public void execute() throws Exception {
		PMML pmml = IOUtil.unmarshal(this.model);

		PMMLManager pmmlManager = new PMMLManager(pmml);

		ModelManager<?> modelManager = pmmlManager.getModelManager(null, ModelEvaluatorFactory.getInstance());

		Evaluator evaluator = (Evaluator)modelManager;

		List<FieldName> activeFields = evaluator.getActiveFields();
		List<FieldName> predictedFields = evaluator.getPredictedFields();
		List<FieldName> outputFields = evaluator.getOutputFields();

		CsvUtil.Table inputTable = CsvUtil.readTable(this.input, this.separator);

		List<Map<FieldName, FieldValue>> rows = prepare(inputTable, evaluator);

		CsvUtil.Table outputTable = new CsvUtil.Table();
		outputTable.setSeparator(inputTable.getSeparator());

		// Copy cells from input table to output table if they have the same structure
		boolean copyInputCells = (rows.size() == (inputTable.size() - 1));

		header:
		{
			List<String> headerRow = new ArrayList<String>();

			if(copyInputCells){
				headerRow.addAll(inputTable.get(0));
			}

			for(FieldName predictedField : predictedFields){
				headerRow.add(predictedField.getValue());
			}

			for(FieldName outputField : outputFields){
				headerRow.add(outputField.getValue());
			}

			outputTable.add(headerRow);
		}

		long start = System.currentTimeMillis();

		body:
		for(int i = 0; i < this.count; i++){

			for(int line = 0; line < rows.size(); line++){
				List<String> bodyRow = new ArrayList<String>();

				if(copyInputCells){
					bodyRow.addAll(inputTable.get(line + 1));
				}

				Map<FieldName, FieldValue> arguments = rows.get(line);

				Map<FieldName, ?> result = evaluator.evaluate(arguments);

				for(FieldName predictedField : predictedFields){
					Object predictedValue = EvaluatorUtil.decode(result.get(predictedField));

					bodyRow.add(String.valueOf(predictedValue));
				}

				for(FieldName outputField : outputFields){
					Object outputValue = EvaluatorUtil.decode(result.get(outputField));

					bodyRow.add(String.valueOf(outputValue));
				}

				if(i == 0){
					outputTable.add(bodyRow);
				}
			}
		}

		long end = System.currentTimeMillis();

		System.err.println("Evaluation completed in " + (end - start) + " ms.");

		CsvUtil.writeTable(outputTable, this.output);
	}

	@SuppressWarnings (
		value = {"unused"}
	)
	static
	private List<Map<FieldName, FieldValue>> prepare(List<List<String>> table, Evaluator evaluator){
		List<FieldName> inputFields = new ArrayList<FieldName>();

		List<FieldName> activeFields = evaluator.getActiveFields();
		List<FieldName> groupFields = evaluator.getGroupFields();
		List<FieldName> predictedFields = evaluator.getPredictedFields();
		List<FieldName> outputFields = evaluator.getOutputFields();

		header:
		{
			List<String> headerRow = table.get(0);

			for(int i = 0; i < headerRow.size(); i++){
				String headerCell = headerRow.get(i);

				FieldName inputField = FieldName.create(headerCell);

				// Check that the column is present in PMML data dictionary
				DataField dataField = evaluator.getDataField(inputField);
				if(dataField != null){

					if(!activeFields.contains(inputField) && !groupFields.contains(inputField)){
						System.err.println("Not an input field: " + inputField.getValue());

						if(predictedFields.contains(inputField) || outputFields.contains(inputField)){
							inputField = null;
						}
					}
				} else

				{
					inputField = null;
				}

				inputFields.add(inputField);
			}
		}

		List<Map<FieldName, Object>> stringRows = new ArrayList<Map<FieldName, Object>>();

		body:
		for(int line = 1; line < table.size(); line++){
			List<String> bodyRow = table.get(line);

			Map<FieldName, Object> row = new LinkedHashMap<FieldName, Object>();

			for(int i = 0; i < inputFields.size(); i++){
				String bodyCell = bodyRow.get(i);

				FieldName inputField = inputFields.get(i);
				if(inputField == null){
					continue;
				}

				if(CsvUtil.isMissing(bodyCell)){
					bodyCell = null;
				}

				row.put(inputField, bodyCell);
			}

			stringRows.add(row);
		}

		if(groupFields.size() == 1){
			FieldName groupField = groupFields.get(0);

			stringRows = EvaluatorUtil.groupRows(groupField, stringRows);
		} else

		if(groupFields.size() > 1){
			throw new IllegalArgumentException();
		}

		List<Map<FieldName, FieldValue>> result = new ArrayList<Map<FieldName, FieldValue>>();

		for(int i = 0; i < stringRows.size(); i++){
			Map<FieldName, Object> stringRow = stringRows.get(i);

			Map<FieldName, FieldValue> row = new LinkedHashMap<FieldName, FieldValue>();

			Collection<Map.Entry<FieldName, Object>> entries = stringRow.entrySet();
			for(Map.Entry<FieldName, Object> entry : entries){
				row.put(entry.getKey(), EvaluatorUtil.prepare(evaluator, entry.getKey(), entry.getValue()));
			}

			result.add(row);
		}

		return result;
	}
}