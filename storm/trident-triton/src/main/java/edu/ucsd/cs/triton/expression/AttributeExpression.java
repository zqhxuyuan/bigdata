package edu.ucsd.cs.triton.expression;


public class AttributeExpression extends BaseExpression {
	private Attribute _attribute;
	
	public AttributeExpression(final Attribute attribute) {
		super();
		_attribute = attribute;
		_isLeaf = true;
	}
	
	public Attribute getAttribute() {
		return _attribute;
	}
	
	@Override
	public String toString() {
		return _attribute.toString();
	}
	
	@Override
	public void dump(String prefix) {
		System.out.println(prefix + toString());
	}

	@Override
  public AttributeExpression clone() {
	  return new AttributeExpression(new Attribute(_attribute.getStream(), _attribute.getName()));
  }
}
