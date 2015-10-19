package edu.ucsd.cs.triton.codegen.language;

public interface IStatement {
	
	/**
	 * This pair of methods are used to inform the node of its parent.
	 */
	public void setParent(IStatement n);

	public IStatement getParent();
	
	public void addChild(IStatement n);
	
	/**
	 * This method returns a child node. The children are numbered from zero, left
	 * to right.
	 */
	public IStatement getChild(int i);

	/** Return the number of children the node has. */
	public int getNumChildren();
	
	public String translate();
}
