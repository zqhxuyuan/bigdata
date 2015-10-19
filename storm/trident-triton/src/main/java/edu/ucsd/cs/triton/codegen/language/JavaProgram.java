package edu.ucsd.cs.triton.codegen.language;

public class JavaProgram extends BaseJavaStatement {
	
	private final String _programName;
	
	public JavaProgram(final String programName) {
		_programName = programName;
		_parent = null;
	}
	
	public JavaProgram Package() {
		addChild(new Package(_programName.toLowerCase()));
		return this;
	}
	
	public JavaProgram Package(String pkgName) {
		addChild(new Package(pkgName));
		return this;
	}
	
	public ImportStatement Import() {
		ImportStatement stmt = new ImportStatement();
		addChild(stmt);
		return stmt;
	}

	public ClassStatement Class() {
		ClassStatement stmt = new ClassStatement(Keyword.PUBLIC, _programName);
		addChild(stmt);
	  return stmt;
  }
	
	public JavaProgram addSimpleStatement(final String stmt) {
		addChild(new SimpleStatement(stmt));
		return this;
	}
	
	public String getProgramName() {
		return _programName;
	}
	
	@Override
  protected void translate(int n, LanguageBuilder sb) {
	  // TODO Auto-generated method stub
		childrenTranslate(n, sb);
  }
}
