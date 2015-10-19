package edu.ucsd.cs.triton.operator;

import edu.ucsd.cs.triton.expression.Attribute;

public class KeyPair {
	private Attribute _leftField;
	private Attribute _rightField;
	
	public KeyPair(final Attribute leftField, final Attribute rightField) {
		_leftField = leftField;
		_rightField = rightField;
	}

	public Attribute getLeftField() {
		return _leftField;
	}

	public void setLeftField(Attribute _leftField) {
		this._leftField = _leftField;
	}

	public Attribute getRightField() {
		return _rightField;
	}

	public void setRightField(Attribute _rightField) {
		this._rightField = _rightField;
	}
	
	public Attribute getFieldByStream(final String stream) {
		if (_leftField.getStream().equals(stream)) {
			return _leftField;
		} else if (_rightField.getStream().equals(stream)) {
			return _rightField;
		} else {
			System.err.println("error, no field find in join key pair.");
			return null;
		}
	}
	
	@Override
	public String toString() {
		return "{" + _leftField + ", " + _rightField + "}";
	}
}