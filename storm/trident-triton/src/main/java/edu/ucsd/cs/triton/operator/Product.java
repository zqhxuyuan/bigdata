package edu.ucsd.cs.triton.operator;

public class Product extends BasicOperator {
	
	
	public Product() {
		_type = OperatorType.PRODUCT;
	}
	
  /** Accept the visitor. **/
  @Override
	public Object accept(OperatorVisitor visitor, Object data) {
    return visitor.visit(this, data);
  }
} 
