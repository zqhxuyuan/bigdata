package edu.ucsd.cs.triton.expression;

public enum LogicOperator {
	AND("&&"), OR("||"), NOT("!");
	
	private final String _op;
	
	LogicOperator(final String op) {
		_op = op;
	}
	
	public static LogicOperator fromString(final String op) {
		if (op.equalsIgnoreCase("and")) {
			return AND;
		} else if (op.equalsIgnoreCase("or")) {
			return OR;
		} else if (op.equalsIgnoreCase("not")) {
			return NOT;
		} else {
			throw new IllegalArgumentException("Invalid logic opeartor [" + op + "]");
		}
	}
	
	@Override
	public String toString() {
		return _op;
	}
}
