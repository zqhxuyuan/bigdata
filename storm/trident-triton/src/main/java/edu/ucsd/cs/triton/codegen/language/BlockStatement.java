package edu.ucsd.cs.triton.codegen.language;

public class BlockStatement extends BaseJavaStatement {

	public BlockStatement SimpleStmt(final String stmt) {
		addChild(new SimpleStatement(stmt));
		return this;
	}

	public If If(final String condition) {
		If stmt = new If(condition);
		addChild(stmt);
		return stmt;
	}
	
	public Else Else() {
		Else stmt = new Else();
		_parent.addChild(stmt);
		return stmt;
	}
	
	public ElseIf ElseIf(final String condition) {
		ElseIf stmt = new ElseIf(condition);
		_parent.addChild(stmt);
		return stmt;
	}
	
	
	public BlockStatement EndIf() {
		return (BlockStatement) end();
	}
	
	public For For(final String condition) {
		For stmt = new For(condition);
		addChild(stmt);
		return stmt;
	}
	
	public BlockStatement EndFor() {
		return (BlockStatement) end();
	}
	
	public While While(final String condition) {
		While stmt = new While(condition);
		addChild(stmt);
		return stmt;
	}
	
	public BlockStatement EndWhile() {
		return (BlockStatement) end();
	}
	
	public BlockStatement Return(final String stmt) {
		addChild(new Return(stmt));
		return this;
	}
	
	public ClassStatement InnerClass(final String name) {
		ClassStatement cs = new ClassStatement(name);
		addChild(cs);
		return cs;
	}	

	public ClassStatement EndInnerClass() {
		return (ClassStatement) _parent;
	}

	public MemberFunction MemberFunction(final String signature) {
		MemberFunction mf = new MemberFunction(signature);
		addChild(mf);
		return mf;
	}
	
	public ClassStatement EndMemberFunction() {
		return (ClassStatement) this._parent;
	}
	
	public JavaProgram EndClass() {
		return (JavaProgram) this._parent;
	}
	
	@Override
  protected void translate(int n, LanguageBuilder sb) {
	  // TODO Auto-generated method stub
	  
  }
}
