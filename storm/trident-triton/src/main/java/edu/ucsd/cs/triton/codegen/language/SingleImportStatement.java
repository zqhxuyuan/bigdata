package edu.ucsd.cs.triton.codegen.language;

public class SingleImportStatement extends BaseJavaStatement {

	private final String _importString; 
	
	public SingleImportStatement(String importString) {
		_importString = importString;
	}

	@Override
  protected void translate(int n, LanguageBuilder builder) {
	  // TODO Auto-generated method stub
	  builder.indent(n)
	  	.append(Keyword.IMPORT).space()
	  	.append(_importString)
	  	.end();
	}

}
