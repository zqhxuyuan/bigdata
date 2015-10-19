package edu.ucsd.cs.triton.codegen.language;

import org.apache.commons.lang3.StringUtils;

public final class LanguageBuilder {
	private StringBuilder _sb;
	
	LanguageBuilder() {
		_sb = new StringBuilder();
	}
	
	public LanguageBuilder indent(int n) {
		if (n > 0) {
			_sb.append(StringUtils.leftPad("", n, ' '));
		}
		
		return this;
	}
	
	public LanguageBuilder newline() {
		_sb.append('\n');
		return this;
	}
	
	public LanguageBuilder space() {
		_sb.append(' ');
		return this;
	}
	
	/**
	 * append " {\n"
	 * @return
	 */
	public LanguageBuilder beginBlock() {
		_sb.append(" {\n");
		return this;
	}
	
	/**
	 * append "}\n"
	 * @return this
	 */
	public LanguageBuilder endBlock() {
		_sb.append("}\n");
		return this;
	}
	
	/**
	 * append ";\n"
	 * @return
	 */
	public LanguageBuilder end() {
		_sb.append(';').append('\n');
		return this;
	}

	public LanguageBuilder append(boolean arg0) {
		_sb.append(arg0);
	  return this;
  }

	public LanguageBuilder append(char c) {
		_sb.append(c);
	  return this;
  }

	public LanguageBuilder append(char[] str) {
		_sb.append(str);
	  return this;
  }

	public LanguageBuilder append(double d) {
		_sb.append(d);
	  return this;
  }

	public LanguageBuilder append(float f) {
		_sb.append(f);
	  return this;
  }

	public LanguageBuilder append(int i) {
		_sb.append(i);
	  return this;
  }

	public LanguageBuilder append(long lng) {
		_sb.append(lng);
	  return this;
  }

	public LanguageBuilder append(Object obj) {
		_sb.append(obj);
	  return this;
  }

	public LanguageBuilder append(String str) {
		_sb.append(str);
	  return this;
  }

	public LanguageBuilder append(StringBuffer sb) {
		_sb.append(sb);
		return this;
  }
	
	public String toString() {
		return _sb.toString();
	}
}
