package edu.ucsd.cs.triton.codegen.language;

public class Else extends BaseJavaStatement {

	@Override
	protected void translate(int n, LanguageBuilder sb) {
		// TODO Auto-generated method stub
		sb.indent(n)
			.append(Keyword.ELSE).beginBlock();

		childrenTranslate(n + PrintStyle.INDENT, sb);

		sb.indent(n).endBlock();
	}

	public Else SimpleStmt(String stmt) {
	  // TODO Auto-generated method stub
		addChild(new SimpleStatement(stmt));
	  return this;
  }
	
	public MemberFunction endIf() {
		return (MemberFunction) end();
	}
}
