package edu.ucsd.cs.triton.codegen.language;

import java.util.List;

public class ImportStatement extends BaseJavaStatement {
	
	public ImportStatement add(final String importString) {
		_children.add(new SingleImportStatement(importString));
		return this;
	}
	
	public ImportStatement add(final List<String> importList) {
		for (String importString : importList) {
			_children.add(new SingleImportStatement(importString));
		}
		
		return this;
	} 
	
	public ImportStatement add(final String[] importList) {
		for (String importString : importList) {
			_children.add(new SingleImportStatement(importString));
		}
		
		return this;
	} 
	
	public JavaProgram EndImport() {
		return (JavaProgram) end();
	}

	@Override
  protected void translate(int n, LanguageBuilder builder) {
	  // TODO Auto-generated method stub
	  childrenTranslate(n, builder);
	  builder.newline();
  }
}
