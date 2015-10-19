package edu.ucsd.cs.triton.operator;

import edu.ucsd.cs.triton.expression.Attribute;
import edu.ucsd.cs.triton.expression.BooleanExpression;

public class Selection extends BasicOperator {

	private BooleanExpression _filter;

	public Selection(final BooleanExpression filter) {
	  // TODO Auto-generated constructor stub
		_type = OperatorType.SELECTION;
		_filter = filter;
  }

	public Selection() {
	  // TODO Auto-generated constructor stub
		_filter = null;
  }

	public void setFilter(final BooleanExpression filter) {
	  // TODO Auto-generated method stub
	  _filter = filter;
  }
	
	public BooleanExpression getFilter() {
		return _filter;
	}
	
	public Attribute[] getFilterInputFields() {
	  // TODO Auto-generated method stub
	  return _filter.getInputFields();
  }
	
	public boolean isEmpty() {
		return (_filter == null);
	}
	
	public void dump() {
		if (_filter != null) {
			_filter.dump("");
		}
	}
	
  /** Accept the visitor. **/
  @Override
	public Object accept(OperatorVisitor visitor, Object data) {
    return visitor.visit(this, data);
  }
}
