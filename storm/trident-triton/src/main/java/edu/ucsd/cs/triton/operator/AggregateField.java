package edu.ucsd.cs.triton.operator;

public class AggregateField extends ProjectionField {

	private String _aggregateFunction;
	
	public AggregateField(final String aggregateFunction, final String outputField) {
		_aggregateFunction = aggregateFunction;
		_outputField = outputField;
	}
	
	public AggregateField(String aggregateFunction) {
	  // TODO Auto-generated constructor stub
		_aggregateFunction = aggregateFunction;
  }

	public void setAggregateFunction(final String aggregateFunction) {
		_aggregateFunction = aggregateFunction;
	}
	
	public String getAggregateFunction(){
		return _aggregateFunction;
	}
	
	
	@Override
	public String toString() {
		return "aggregate function: " + _aggregateFunction + " output: " + _outputField;
	}
}
