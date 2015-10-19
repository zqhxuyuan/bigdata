package edu.ucsd.cs.triton.expression;

public class IntegerExpression extends BaseExpression {
	private int _value;
	
	public IntegerExpression(int value) {
		super();
		_value = value;
		_isLeaf = true;
	}
	
	public int getValue() {
		return _value;
	}
	
	@Override
	public String toString() {
		return Integer.toString(_value);
	}

	@Override
	public void dump(String prefix) {
		System.out.println(prefix + toString());
	}
	
	@Override
  public IntegerExpression clone() {
	  // TODO Auto-generated method stub
	  return new IntegerExpression(_value);
  }
}
