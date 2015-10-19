package edu.ucsd.cs.triton.codegen.language;

import java.util.List;

public class ClassStatement extends BlockStatement {

	private final String _className;
	private final String _scope;
	private final boolean _isStatic;
	private String _baseClass;
	private String _interface;
	
	private ClassStatement(final String scope, final String className, final boolean isStatic) {
		_scope = scope;
		_className = className;
		_isStatic = isStatic;
		_baseClass = null;
		_interface = null;
	}
	
	public ClassStatement(final String className) {
		this("", className, false);
	}
	
	public ClassStatement(final String scope, final String className) {
		this(scope, className, false);
	}
	
	public static ClassStatement createStaticClass(final String scope, final String className) {
		return new ClassStatement(scope, className, true);
	}
	
	public void addInnerClass(ClassStatement cs) {
		_children.add(cs);
	}
	
	public ClassStatement addInnerClass(List<ClassStatement> list) {
		for (ClassStatement cs : list) {
			_children.add(cs);
		}
		return this;
	}
	
	public ClassStatement addMemberFunction(MemberFunction mf) {
		if (mf != null) {
			_children.add(mf);
		}
		
		return this;
	}
	
	@Override
  protected void translate(int n, LanguageBuilder sb) {
	  // TODO Auto-generated method stub
	  sb.indent(n)
	  	.append(_scope).space()
	  	.append(_isStatic? "static " : "")
	  	.append(Keyword.CLASS).space()
	  	.append(_className);
	  
	  if (_baseClass != null) {
	  	sb.space().append(Keyword.EXTENDS).space().append(_baseClass);
	  }
	  
	  if (_interface != null) {
	  	sb.space().append(Keyword.IMPLEMENTS).space().append(_interface);
	  }
	  
	  sb.beginBlock();
	  
	  childrenTranslate(n + PrintStyle.INDENT, sb);
	  
	  sb.indent(n).endBlock();
	  sb.newline();
  }

	public ClassStatement addSimpleStmt(List<String> list) {
	  // TODO Auto-generated method stub
		for (String stmt : list) {
			_children.add(new SimpleStatement(stmt));
		}
		return this;
  }


	public ClassStatement Extends(String string) {
	  // TODO Auto-generated method stub
		_baseClass = string;
	  return this;
  }

	public ClassStatement Implements(String string) {
	  // TODO Auto-generated method stub
		_interface = string;
	  return this;
  }
}
