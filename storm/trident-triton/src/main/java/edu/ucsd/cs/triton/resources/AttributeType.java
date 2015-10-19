package edu.ucsd.cs.triton.resources;

public enum AttributeType {
	INT("Integer"), FLOAT("Float"), STRING("String"), TIMESTAMP("Long");
	
	private final String _type;
	
	AttributeType(final String type) {
		_type = type;
	}
	
	@Override
	public String toString() {
		return _type;
	}
}
