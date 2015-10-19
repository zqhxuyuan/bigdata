package edu.ucsd.cs.triton.codegen.language;

public class MemberFunction extends BlockStatement {

	private final String _signature;
	
	public MemberFunction(String signature) {
	  // TODO Auto-generated constructor stub
		this._signature = signature;
	}

	@Override
  protected void translate(int n, LanguageBuilder sb) {
	  // TODO Auto-generated method stub
	  sb.indent(n)
	  	.append(_signature).beginBlock();
	  
	  childrenTranslate(n + PrintStyle.INDENT, sb);
	  
	  sb.indent(n)
	  	.endBlock();
	  sb.newline();
  }

}
