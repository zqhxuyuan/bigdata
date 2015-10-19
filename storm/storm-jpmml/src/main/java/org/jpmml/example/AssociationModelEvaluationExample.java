/*
 * Copyright (c) 2013 University of Tartu
 */
package org.jpmml.example;

import java.io.*;
import java.util.*;

import org.jpmml.evaluator.*;
import org.jpmml.evaluator.FieldValue;

import org.dmg.pmml.*;

import com.beust.jcommander.Parameter;

public class AssociationModelEvaluationExample extends Example {

	@Parameter (
		names = "--model",
		description = "The PMML file with an Association rules model",
		required = true
	)
	private File model = null;

	@Parameter (
		names = "--items",
		description = "List of items in a transaction. If there are multiple items, use commas (',') to separate them",
		required = true
	)
	private List<String> items = null;


	static
	public void main(String... args) throws Exception {
		execute(AssociationModelEvaluationExample.class, args);
	}

	@Override
	public void execute() throws Exception {
		PMML pmml = IOUtil.unmarshal(this.model);

		AssociationModelEvaluator associationModelEvaluator = new AssociationModelEvaluator(pmml);

		List<FieldName> activeFields = associationModelEvaluator.getActiveFields();

		// The Association rules model must contain exactly one MiningField whose type is "active"
		if(activeFields.size() != 1){
			throw new IllegalArgumentException();
		}

		FieldName activeField = activeFields.get(0);

		// Make sure that all user supplied item values conform to the data schema
		FieldValue activeValue = EvaluatorUtil.prepare(associationModelEvaluator, activeField, this.items);

		Map<FieldName, ?> arguments = Collections.singletonMap(activeField, activeValue);

		Map<FieldName, ?> result = associationModelEvaluator.evaluate(arguments);

		// The Association rules model may or may not contain a MiningField whose type is "predicted".
		// If it didn't, then a null value is returned. Having a null value is not a problem here, because java.util.Map implementations allow using null values as keys
		FieldName targetField = associationModelEvaluator.getTargetField();

		// The target object is represented by class org.jpmml.evaluator.Association.
		// However, this is an internal class (hence the package private visibility), which should not be interacted with directly
		Object targetValue = result.get(targetField);

		// The target object can be cast to interface org.jpmml.evaluator.HasRuleValues
		HasRuleValues hasRuleValues = (HasRuleValues)targetValue;

		Map<String, Item> itemRegistry = hasRuleValues.getItemRegistry();
		Map<String, Itemset> itemsetRegistry = hasRuleValues.getItemsetRegistry();

		// The target object computes the final list of fired rules.
		// It is the responsibility of the application developer to sort this list after application specific criteria
		List<AssociationRule> exclusiveRecommendations = hasRuleValues.getRuleValues(OutputField.Algorithm.EXCLUSIVE_RECOMMENDATION);
		for(AssociationRule exclusiveRecommendation : exclusiveRecommendations){
			Itemset antecedent = itemsetRegistry.get(exclusiveRecommendation.getAntecedent());
			Itemset consequent = itemsetRegistry.get(exclusiveRecommendation.getConsequent());

			// TODO: Do something useful with the Itemset instances
		}

		// The alternative to working with the target object is to declare appropriate OutputFields
		// An OutputField element has attributes for specifying the rule selection algorithm, rule sorting criteria (basis, order), output feature etc.
		List<FieldName> outputFields = associationModelEvaluator.getOutputFields();
		for(FieldName outputField : outputFields){
			Object outputValue = result.get(outputField);

			// TODO: Do something useful with the output object
		}
	}
}