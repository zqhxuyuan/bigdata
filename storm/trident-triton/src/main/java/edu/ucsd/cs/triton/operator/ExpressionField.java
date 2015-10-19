package edu.ucsd.cs.triton.operator;

import edu.ucsd.cs.triton.expression.BaseExpression;

public class ExpressionField extends ProjectionField {
	private BaseExpression _expression;
	
	public ExpressionField() {
		_expression = null;
		_outputField = null;
	}
	
	public ExpressionField(final BaseExpression expression, final String outputField) {
		_expression = expression;
		_outputField = outputField;
	}
	
	public void setExpression(final BaseExpression expression) {
		_expression = expression;
	}
	
	public BaseExpression getBaseExpression() {
		return _expression;
	}
	
	public void dump() {
		_expression.dump("");
	}
	
	public String toString() {
		dump();
		return "input: " + "expression" + " output: " + _outputField;
	}
}
