package edu.ucsd.cs.triton.expression;

public class ArithmeticExpression extends BaseExpression {
	private ArithmeticOperator _op;
	
	public ArithmeticExpression(ArithmeticOperator op) {
		super();
		_op = op;
	}
	
	public ArithmeticExpression(ArithmeticOperator op, BaseExpression left, BaseExpression right) {
		_left = left;
		_right = right;
		_op = op;
	}

	@Override
	public void dump(String prefix) {
		System.out.println(prefix + _op);
		_left.dump(prefix + " ");
		_right.dump(prefix + " ");
	}

	@Override
  public ArithmeticExpression clone() {
	  return new ArithmeticExpression(_op, _left.clone(), _right.clone());
  }

	public ArithmeticOperator getOperator() {
	  // TODO Auto-generated method stub
	  return _op;
  }
}
