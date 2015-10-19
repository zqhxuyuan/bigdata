package edu.ucsd.cs.triton.codegen.language;

public class Package extends BaseJavaStatement {
	private final String _packageName;
	
	public Package(final String packageName) {
		_packageName = packageName;
	}

	@Override
  protected void translate(int n, LanguageBuilder sb) {
	  // TODO Auto-generated method stub
	  sb.indent(n)
	  	.append(Keyword.PACKAGE).space().append(_packageName).end().newline();
	}
}
