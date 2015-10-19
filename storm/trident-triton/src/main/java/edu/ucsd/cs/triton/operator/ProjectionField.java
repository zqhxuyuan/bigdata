package edu.ucsd.cs.triton.operator;

public abstract class ProjectionField {
	protected String _outputField;
	
	public String getOutputField() {
		return _outputField;
	}
	
	public void setOutputField(final String outputField) {
	  // TODO Auto-generated method stub
		_outputField = outputField;
  }
	
	public void dump() {
		System.out.println("Base field");
	}
}
