package edu.ucsd.cs.triton.operator;

public class InputStream extends BasicOperator {
	private final String _name;
	private final String _rename;
	
	public InputStream(final String name, final String rename) {
		_type = OperatorType.INPUT_STREAM;
		_name = name;
		_rename = rename;
	}
	
	public InputStream(final String name) {
		_type = OperatorType.INPUT_STREAM;
		_name = name;	
		_rename = null;
	}

	public String getName() {
		return _name;
	}
	
  /** Accept the visitor. **/
  @Override
	public Object accept(OperatorVisitor visitor, Object data) {
    return visitor.visit(this, data);
  }
	
	public String getRename() {
		return _rename;
	}
	
	public boolean hasRename() {
		return _rename != null;
	}
}
