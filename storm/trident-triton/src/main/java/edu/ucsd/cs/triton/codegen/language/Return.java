package edu.ucsd.cs.triton.codegen.language;

public class Return extends BaseJavaStatement  {

	private final String _stmt; 
	
	public Return(String stmt) {
		_stmt = stmt;
	}

	@Override
  protected void translate(int n, LanguageBuilder sb) {
	  // TODO Auto-generated method stub
	  sb.indent(n).append(Keyword.RETURN).space().append(_stmt).end();
  }
}
