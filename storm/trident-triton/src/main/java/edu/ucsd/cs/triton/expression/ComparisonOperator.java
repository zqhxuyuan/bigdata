package edu.ucsd.cs.triton.expression;

public enum ComparisonOperator {
	EQ("=="), NEQ("!="), GT(">"), GET(">="), LT("<"), LET("<=");
	
	private final String _op;
	
	ComparisonOperator(String op) {
		_op = op;
	}
	
	public static ComparisonOperator fromString(final String op) {
		if (op.equals("=")) {
			return ComparisonOperator.EQ;
		} else if (op.equals("<>")) {
			return ComparisonOperator.NEQ;
		} else if (op.equals(">")) {
			return ComparisonOperator.GT;
		} else if (op.equals(">=")) {
			return ComparisonOperator.GET;
		} else if (op.equals("<")) {
			return ComparisonOperator.LT;
		} else if (op.equals("<=")) {
			return ComparisonOperator.LET;
		} else {
			throw new IllegalArgumentException("Invalid logic opeartor [" + op + "]");
		}
	}
	
	@Override
	public String toString() {
		return _op;
	}
}
