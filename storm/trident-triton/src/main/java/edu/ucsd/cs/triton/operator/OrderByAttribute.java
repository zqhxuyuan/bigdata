package edu.ucsd.cs.triton.operator;

import edu.ucsd.cs.triton.expression.Attribute;

public class OrderByAttribute extends Attribute {

	private boolean _desc = false;
	
	public OrderByAttribute(String stream, String name) {
	  super(stream, name);
	  // TODO Auto-generated constructor stub
  }
	
	public OrderByAttribute(String stream, String name, boolean desc) {
		this(stream, name);
		_desc = desc;
	}
	
	public boolean getDesc() {
		return _desc;
	}
}
