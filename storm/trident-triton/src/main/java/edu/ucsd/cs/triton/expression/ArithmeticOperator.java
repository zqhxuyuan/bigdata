package edu.ucsd.cs.triton.expression;

public enum ArithmeticOperator {
	PLUS('+'), MINUS('-'), MULT('*'), DIVIDE('/');
	
	private final char _op;
	
	ArithmeticOperator(char op) {
		_op = op;
	}
	
	public static ArithmeticOperator fromString(final String op) {
		if (op.equals("+")) {
			return PLUS;
		} else if (op.equals("-")) {
			return MINUS;
		} else if (op.equals("*")) {
			return MULT;
		} else if (op.equals("/")) {
			return DIVIDE;
		} else {
			throw new IllegalArgumentException("Invalid logic opeartor [" + op + "]");
		}
	}
	
	public String toString() {
		return Character.toString(_op);
	}
}
