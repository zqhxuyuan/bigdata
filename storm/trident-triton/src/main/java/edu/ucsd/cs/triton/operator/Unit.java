package edu.ucsd.cs.triton.operator;

public enum Unit {
	SECOND(1), 
	MINITUE(1 * 60), 
	HOUR(1 * 60 * 60), 
	DAY(1 * 60 * 60 * 24);
	
	private int _value;
	
	private Unit(int value) {
		_value = value;
	}
	
	public int getValue() {
		return _value;
	}

	public static Unit fromString(String unit) {
	  // TODO Auto-generated method stub
		if (unit.equalsIgnoreCase("sec")) {
			return SECOND;
		} else if (unit.equalsIgnoreCase("min")) {
			return MINITUE;
		} else if (unit.equalsIgnoreCase("hr")) {
			return HOUR;
		} else if (unit.equalsIgnoreCase("day")) {
			return DAY; 
		} else {
			throw new IllegalArgumentException("Invalid logic opeartor [" + unit + "]");
		}
  }
}
