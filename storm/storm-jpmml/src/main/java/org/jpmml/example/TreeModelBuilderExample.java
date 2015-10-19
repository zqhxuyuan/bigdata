/*
 * Copyright (c) 2011 University of Tartu
 */
package org.jpmml.example;

import java.io.*;
import java.util.*;

import org.jpmml.evaluator.*;

import org.dmg.pmml.*;

import com.beust.jcommander.Parameter;

public class TreeModelBuilderExample extends Example {

	@Parameter (
		names = {"--model"},
		description = "The PMML file"
	)
	private File model = null;


	static
	public void main(String... args) throws Exception {
		execute(TreeModelBuilderExample.class, args);
	}

	@Override
	public void execute() throws Exception {
		PMML pmml = createGolfingModel();

		if(this.model != null){
			IOUtil.marshal(pmml, this.model);
		}

		TreeModelEvaluator treeModelEvaluator = new TreeModelEvaluator(pmml);

		Map<FieldName, ?> arguments = EvaluationExample.readArguments(treeModelEvaluator);

		Map<FieldName, ?> result = treeModelEvaluator.evaluate(arguments);

		EvaluationExample.writeResult(treeModelEvaluator, result);
	}

	static
	private PMML createGolfingModel(){
		Header header = new Header()
			.withCopyright("www.dmg.org")
			.withDescription("A very small binary tree model to show structure.");

		PMML pmml = new PMML(header, new DataDictionary(), "4.1");

		Node n1 = createNode("1", new True(), "will play");

		TreeModel treeModel = new TreeModel(new MiningSchema(), n1, MiningFunctionType.CLASSIFICATION);
		treeModel.withModelName("golfing");

		pmml.withModels(treeModel);

		FieldName temperature = FieldName.create("temperature");
		declareField(pmml, treeModel, temperature, FieldUsageType.ACTIVE, null);

		FieldName humidity = FieldName.create("humidity");
		declareField(pmml, treeModel, humidity, FieldUsageType.ACTIVE, null);

		FieldName windy = FieldName.create("windy");
		declareField(pmml, treeModel, windy, FieldUsageType.ACTIVE, createValues("true", "false"));

		FieldName outlook = FieldName.create("outlook");
		declareField(pmml, treeModel, outlook, FieldUsageType.ACTIVE, createValues("sunny", "overcast", "rain"));

		FieldName whatIdo = FieldName.create("whatIdo");
		declareField(pmml, treeModel, whatIdo, FieldUsageType.PREDICTED, createValues("will play", "may play", "no play"));

		DataDictionary dataDictionary = pmml.getDataDictionary();
		dataDictionary.withNumberOfFields(5);

		//
		// Upper half of the tree
		//

		Predicate n2Predicate = createSimplePredicate(outlook, SimplePredicate.Operator.EQUAL, "sunny");

		Node n2 = createNode("2", n2Predicate, "will play");
		n1.withNodes(n2);

		Predicate n3Predicate = createCompoundPredicate(CompoundPredicate.BooleanOperator.SURROGATE,
			createSimplePredicate(temperature, SimplePredicate.Operator.LESS_THAN, "90"),
			createSimplePredicate(temperature, SimplePredicate.Operator.GREATER_THAN, "50")
		);

		Node n3 = createNode("3", n3Predicate, "will play");
		n2.withNodes(n3);

		Predicate n4Predicate = createSimplePredicate(humidity, SimplePredicate.Operator.LESS_THAN, "80");

		Node n4 = createNode("4", n4Predicate, "will play");
		n3.withNodes(n4);

		Predicate n5Predicate = createSimplePredicate(humidity, SimplePredicate.Operator.GREATER_OR_EQUAL, "80");

		Node n5 = createNode("5", n5Predicate, "no play");
		n3.withNodes(n5);

		Predicate n6Predicate = createCompoundPredicate(CompoundPredicate.BooleanOperator.OR,
			createSimplePredicate(temperature, SimplePredicate.Operator.GREATER_OR_EQUAL, "90"),
			createSimplePredicate(temperature, SimplePredicate.Operator.LESS_OR_EQUAL, "50")
		);

		Node n6 = createNode("6", n6Predicate, "no play");
		n2.withNodes(n6);

		//
		// Lower half of the tree
		//

		Predicate n7Predicate = createCompoundPredicate(CompoundPredicate.BooleanOperator.OR,
			createSimplePredicate(outlook, SimplePredicate.Operator.EQUAL, "overcast"),
			createSimplePredicate(outlook, SimplePredicate.Operator.EQUAL, "rain")
		);

		Node n7 = createNode("7", n7Predicate, "may play");
		n1.withNodes(n7);

		Predicate n8Predicate = createCompoundPredicate(CompoundPredicate.BooleanOperator.AND,
			createSimplePredicate(temperature, SimplePredicate.Operator.GREATER_THAN, "60"),
			createSimplePredicate(temperature, SimplePredicate.Operator.LESS_THAN, "100"),
			createSimplePredicate(outlook, SimplePredicate.Operator.EQUAL, "overcast"),
			createSimplePredicate(humidity, SimplePredicate.Operator.LESS_THAN, "70"),
			createSimplePredicate(windy, SimplePredicate.Operator.EQUAL, "false")
		);

		Node n8 = createNode("8", n8Predicate, "may play");
		n7.withNodes(n8);

		Predicate n9Predicate = createCompoundPredicate(CompoundPredicate.BooleanOperator.AND,
			createSimplePredicate(outlook, SimplePredicate.Operator.EQUAL, "rain"),
			createSimplePredicate(humidity, SimplePredicate.Operator.LESS_THAN, "70")
		);

		Node n9 = createNode("9", n9Predicate, "no play");
		n7.withNodes(n9);

		return pmml;
	}

	static
	private void declareField(PMML pmml, Model model, FieldName name, FieldUsageType usage, List<Value> values){
		OpType opType = (values != null ? OpType.CATEGORICAL : OpType.CONTINUOUS);
		DataType dataType = (values != null ? DataType.STRING : DataType.DOUBLE);

		DataField dataField = new DataField(name, opType, dataType)
			.withValues(values);

		DataDictionary dataDictionary = pmml.getDataDictionary();
		dataDictionary.withDataFields(dataField);

		MiningField miningField = new MiningField(name)
			.withUsageType((FieldUsageType.ACTIVE).equals(usage) ? null : usage);

		MiningSchema miningSchema = model.getMiningSchema();
		miningSchema.withMiningFields(miningField);
	}

	static
	private List<Value> createValues(String... strings){
		List<Value> values = new ArrayList<Value>();

		for(String string : strings){
			values.add(new Value(string));
		}

		return values;
	}

	static
	private Node createNode(String id, Predicate predicate, String score){
		return new Node()
			.withId(id)
			.withPredicate(predicate)
			.withScore(score);
	}

	static
	private SimplePredicate createSimplePredicate(FieldName name, SimplePredicate.Operator operator, String value){
		return new SimplePredicate(name, operator)
			.withValue(value);
	}

	static
	private CompoundPredicate createCompoundPredicate(CompoundPredicate.BooleanOperator operator, Predicate... predicates){
		return new CompoundPredicate(operator)
			.withPredicates(predicates);
	}
}