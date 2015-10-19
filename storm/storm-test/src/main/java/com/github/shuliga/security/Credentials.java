package com.github.shuliga.security;

import javax.xml.bind.annotation.XmlRootElement;
import java.io.Serializable;

/**
 * User: yshuliga
 * Date: 07.01.14 12:32
 */
@XmlRootElement
public class Credentials implements Serializable {

	public enum Roles {
		PHYSICIAN, NURSE, SYSTEM;

		public String getLabel(){
			return name().toLowerCase();
		}
	}


	public static final String SEPARATOR = ":";

	public String login;
	public String role;

	public Credentials(){
	}
	public Credentials(String login, String role){
		this.login = login;
		this.role = role;
	}

	@Override
	public String toString() {
		return login + SEPARATOR + role;
	}

	@Override
	public boolean equals(Object obj) {
		return obj != null && obj instanceof Credentials &&  obj.toString().equals(toString());
	}

	@Override
	public int hashCode() {
		return toString().hashCode();
	}
}
