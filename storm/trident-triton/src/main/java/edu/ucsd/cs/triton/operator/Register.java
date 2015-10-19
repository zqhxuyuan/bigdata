package edu.ucsd.cs.triton.operator;

import edu.ucsd.cs.triton.resources.BaseDefinition;

public class Register extends BasicOperator {
	private final BaseDefinition _definition;
	
	Register(final BaseDefinition def) {
		_type = OperatorType.REGISTER;
		_definition = def;
	}
	
	/**
	 * 
	 * @return the registered stream/relation definition
	 */
	public BaseDefinition getDefinition() {
		return _definition;
	}
	
  /** Accept the visitor. **/
  @Override
	public Object accept(OperatorVisitor visitor, Object data) {
    return visitor.visit(this, data);
  }
}
