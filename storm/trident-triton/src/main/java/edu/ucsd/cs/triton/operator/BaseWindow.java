package edu.ucsd.cs.triton.operator;

public abstract class BaseWindow extends BasicOperator {
	protected String _inputStream;
	
	public void setInputStream(String inputStream) {
		_inputStream = inputStream;
	}
	
	public String getInputStream() {
		return _inputStream;
	}
}
