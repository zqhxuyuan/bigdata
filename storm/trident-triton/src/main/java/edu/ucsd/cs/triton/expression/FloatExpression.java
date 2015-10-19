package edu.ucsd.cs.triton.expression;

public class FloatExpression extends BaseExpression {
	private float _value;
	
	public FloatExpression(float value) {
		super();
		_value = value;
		_isLeaf = true;
	}
	
	public float getValue() {
		return _value;
	}
	
	public void dump(String prefix) {
		System.out.println(prefix + toString());
	}
	
	public String toString() {
		return Float.toString(_value);
	}

	@Override
  public FloatExpression clone() {
	  // TODO Auto-generated method stub
	  return new FloatExpression(_value);
  }
}
