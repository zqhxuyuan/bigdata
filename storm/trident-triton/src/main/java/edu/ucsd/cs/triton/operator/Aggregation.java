package edu.ucsd.cs.triton.operator;

import java.util.ArrayList;
import java.util.List;

import edu.ucsd.cs.triton.expression.Attribute;

public class Aggregation extends BasicOperator {
	private List<Aggregator> _aggregatorList;
	private List<Attribute> _groupByList;
	
	public Aggregation() {
		_type = OperatorType.AGGREGATION;
		_groupByList = new ArrayList<Attribute> ();
		_aggregatorList = new ArrayList<Aggregator> ();
	}

	public boolean addGroupByAttribute(Attribute attribute) {
		return _groupByList.add(attribute);
	}
	
	/**
	 * This is an hack on the trident code generation when sliding window is 
	 * involved. We need to add a "windowId" into the group by list.
	 * @param inputStream
	 */
	public void addWindowId(final String inputStream) {
		_groupByList.add(0, new Attribute(inputStream, "windowId"));
	}

	public boolean addAggregator(final Aggregator aggregator) {
		return _aggregatorList.add(aggregator);
	}
	
	public List<Aggregator> getAggregatorList() {
		return _aggregatorList;
	}
	
	public List<Attribute> getGroupByList() {
		return _groupByList;
	}
	
	public boolean containsGroupBy() {
		return _groupByList.size() > 0;
	}
	
	public boolean isEmpty() {
		return _aggregatorList.isEmpty() && _groupByList.isEmpty();
	}
	
  /** Accept the visitor. **/
  @Override
	public Object accept(OperatorVisitor visitor, Object data) {
    return visitor.visit(this, data);
  }
}
