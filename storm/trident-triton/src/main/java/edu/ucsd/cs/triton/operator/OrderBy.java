package edu.ucsd.cs.triton.operator;

import java.util.ArrayList;
import java.util.List;

/**
 * Sort operator, sort the first N (default = 10) tuple by list of attribute
 * @author zhihengli
 *
 */
public class OrderBy extends BasicOperator {
	private int _limit = 10;
	private List<OrderByAttribute> _orderByAttributeList;
	
	public OrderBy() {
		super();
		_type = OperatorType.ORDER;
		_orderByAttributeList = new ArrayList<OrderByAttribute> ();
	}
	
	public OrderBy(int limit, List<OrderByAttribute> orderByAttribute) {
		super();
		_type = OperatorType.ORDER;
		_limit = limit;
		_orderByAttributeList = orderByAttribute;
	}
	
	public void addOrderByAttribute(final OrderByAttribute attribute) {
		_orderByAttributeList.add(attribute);
	}
	
	public boolean isEmpty() {
		return (_orderByAttributeList.size() == 0);
	}
	
	public List<OrderByAttribute> getOrderByAttributeList() {
		return _orderByAttributeList;
	}
	
	public int getLimit(){
		return _limit;
	}

	public void setLimit(int limit) {
	  // TODO Auto-generated method stub
	  _limit = limit;
  }
	
  /** Accept the visitor. **/
  @Override
	public Object accept(OperatorVisitor visitor, Object data) {
    return visitor.visit(this, data);
  }
}
