package edu.ucsd.cs.triton.expression;

import java.util.HashSet;
import java.util.Set;
import java.util.Stack;

public abstract class BaseExpression {
	protected BaseExpression _left;
	protected BaseExpression _right;
	protected boolean _isLeaf;

	abstract public BaseExpression clone();
	abstract public void dump(String prefix);
	
	public BaseExpression() {
		_left = _right = null;
		_isLeaf = false;
	}
	
	public BaseExpression getLeft() {
		return _left;
	}
	
	public BaseExpression getRight() {
		return _right;
	}
	
	public String toString() {
		return _left.toString() + " " + _right.toString();
	}
	
	public Attribute[] getInputFields() {
	  // TODO Auto-generated method stub
	  Set<Attribute> inputFields = new HashSet<Attribute> ();
	  BaseExpression cur = this;
		Stack<BaseExpression> stack = new Stack<BaseExpression> ();
	  stack.push(cur);
	  
	  while (!stack.empty()) {
	  	cur = stack.pop();
	  	if (cur instanceof AttributeExpression) {
	  		Attribute attribute = ((AttributeExpression) cur).getAttribute();
	  		if (!inputFields.contains(attribute))
	  		inputFields.add(attribute);
	  	}
	  	
	  	if (cur._left != null) {
	  		stack.push(cur._left);
	  	}
	  	
	  	if (cur._right != null) {
	  		stack.push(cur._right);
	  	}
	  }
	  return inputFields.toArray (new Attribute[inputFields.size ()]);
	}
}
