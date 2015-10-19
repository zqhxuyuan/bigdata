package edu.ucsd.cs.triton.operator;

public interface IOperator {

	/**
	 * This pair of methods are used to inform the node of its parent.
	 */
	public void setParent(IOperator n);

	public IOperator getParent();

	/**
	 * This method tells the node to add its argument to the node's list of
	 * children.
	 */
	public void addChild(IOperator n, int i);

	/**
	 * This method returns a child node. The children are numbered from zero, left
	 * to right.
	 */
	public IOperator getChild(int i);

	/** Return the number of children the node has. */
	public int getNumChildren();
	
  public Object accept(OperatorVisitor visitor, Object data);
	
	public void dump(String prefix);
}
