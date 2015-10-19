package edu.ucsd.cs.triton.codegen.language;

import java.util.ArrayList;
import java.util.List;

public abstract class BaseJavaStatement implements IStatement {
	protected List<IStatement> _children;
	protected IStatement _parent;
	
	protected abstract void translate(int n, LanguageBuilder sb);
	
	public BaseJavaStatement() {
		_children = new ArrayList<IStatement> ();
	}

	@Override
	public IStatement getParent() {
		return this._parent;
	}

	@Override
  public void setParent(IStatement _statement) {
	  // TODO Auto-generated method stub
	  this._parent = _statement;
  }

	
	@Override
  public void addChild(IStatement n) {
	  // TODO Auto-generated method stub
	  this._children.add(n);
	  n.setParent(this);
  }

	@Override
  public IStatement getChild(int i) {
	  // TODO Auto-generated method stub
	  return this._children.get(i);
  }

	@Override
  public int getNumChildren() {
	  // TODO Auto-generated method stub
	  return this._children.size();
  }
	
	public IStatement end() {
		return _parent;
	}
	
	@Override
	public String translate() {
		LanguageBuilder sb = new LanguageBuilder();
		translate(0, sb);
		return sb.toString();
	}
	
	protected void childrenTranslate(int n, LanguageBuilder sb) {
		for (IStatement statement: _children) {
			((BaseJavaStatement) statement).translate(n, sb);
		}
	}
}
