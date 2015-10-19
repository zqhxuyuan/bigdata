package edu.ucsd.cs.triton.expression;

public class StringExpression extends BaseExpression {
		private String _value;
		
		public StringExpression(String value) {
			super();
			_value = value;
			_isLeaf = true;
		}
		
		public String getValue() {
			return _value;
		}
		
		@Override
		public String toString() {
			return _value;
		}

		@Override
		public void dump(String prefix) {
			System.out.println(prefix + _value);
		}

		@Override
	  public StringExpression clone() {
		  // TODO Auto-generated method stub
		  return new StringExpression(_value);
	  }
}
