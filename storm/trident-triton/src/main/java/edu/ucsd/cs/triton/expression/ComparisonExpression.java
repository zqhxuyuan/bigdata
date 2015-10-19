package edu.ucsd.cs.triton.expression;


public class ComparisonExpression extends BooleanExpression {
	
	private ComparisonOperator _op;
	
	public ComparisonExpression(ComparisonOperator op) {
		super();
		_op = op;
	}
	
	public ComparisonExpression(ComparisonOperator op, BaseExpression left, BaseExpression right) {
		_left = left;
		_right = right;
		_op = op;
	}
	
	public ComparisonOperator getOperator() {
		return _op;
	}
	
	public boolean isJoinExpression() {
		if (_op != ComparisonOperator.EQ) {
			return false;
		}
		
		if (!( _left instanceof AttributeExpression && _right instanceof AttributeExpression)) {
			return false;
		}
		
		Attribute left = ((AttributeExpression) _left).getAttribute();
		Attribute right = ((AttributeExpression) _right).getAttribute();

		return ! (left.getStream().equals(right.getStream()));
	}
	
	@Override
	public void dump(String prefix) {
		System.out.println(prefix + _op);
		_left.dump(prefix + " ");
		_right.dump(prefix + " ");
	}
	
	@Override
	public String toString() {
		return _op.toString();
	}
	
	@Override
	public ComparisonExpression clone() {
		return new ComparisonExpression(_op, _left.clone(), _right.clone());
	}
}
