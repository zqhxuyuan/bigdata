package edu.ucsd.cs.triton.codegen.language;

public final class SimpleStatement extends BaseJavaStatement {

	private final String _stmt;
	
	public SimpleStatement(final String stmt) {
		_stmt = stmt;
	}
	
	@Override
  protected void translate(int n, LanguageBuilder sb) {
	  // TODO Auto-generated method stub
	  sb.indent(n).append(_stmt).end().newline();
  }
}
