package edu.ucsd.cs.triton.operator;


public class OutputStream extends BasicOperator {
	private static final String STDOUT = "stdout";
	private final String _fileName;
	
	private final String[] _outputFieldList;
	
	public OutputStream(final String fileName, final String[] outputFields) {
		_type = OperatorType.OUTPUT_STREAM;
		_fileName = fileName;
		_outputFieldList = outputFields;
	}
	
	public String getFileName() {
		return _fileName;
	}

	public static OutputStream newStdoutStream(final String[] outputFields) {
	  // TODO Auto-generated method stub
	  return new OutputStream(STDOUT, outputFields);
  }
	
	public String[] getOutputFieldList() {
		return _outputFieldList;
	}
	
	public boolean isStdout() {
		return _fileName != null && _fileName.equals(STDOUT);
	}
	
  /** Accept the visitor. **/
  @Override
	public Object accept(OperatorVisitor visitor, Object data) {
    return visitor.visit(this, data);
  }
}
