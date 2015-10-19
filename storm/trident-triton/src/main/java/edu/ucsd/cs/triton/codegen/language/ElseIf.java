package edu.ucsd.cs.triton.codegen.language;

public class ElseIf extends BaseJavaStatement {

	private final String _condition;
	
	public ElseIf(final String condition) {
		_condition = condition;
	}
	
	@Override
	protected void translate(int n, LanguageBuilder sb) {
		// TODO Auto-generated method stub
		sb.indent(n)
			.append(Keyword.ELSE).space().append(Keyword.IF).space().append('(').append(_condition).append(')').beginBlock();

		childrenTranslate(n + PrintStyle.INDENT, sb);

		sb.indent(n).endBlock();
	}
	
	public ElseIf SimpleStmt(String stmt) {
	  // TODO Auto-generated method stub
		addChild(new SimpleStatement(stmt));
	  return this;
  }
	
	public MemberFunction endIf() {
		return (MemberFunction) end();
	}
}
