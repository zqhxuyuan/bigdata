package com.github.shuliga.event.command;

import javax.xml.bind.annotation.XmlRootElement;
import java.io.Serializable;

/**
 * User: yshuliga
 * Date: 08.01.14 13:49
 */
@XmlRootElement
public class CommandJson implements Serializable{

	static final long serialVersionUID = 253783678479L;

	public enum Action {
		NOTIFY, DELEGATE, REGISTER, LOGOFF;
		public String getLabel(){
			return this.name().toLowerCase();
		}
	}

	public CommandJson(){

	}

	public CommandJson(	String action, String message, String sourceEventId, String targetRole, String targetName){
		this.action = action;
		this.message = message;
		this.sourceEventId = sourceEventId;
		this.targetRole = targetRole;
		this.targetName = targetName;
	}

	public String action;
	public String message;
	public String sourceEventId;
	public String targetRole;
	public String targetName;

	@Override
	public String toString() {
		return "Command: " + action + ", " + message + ", targetName: " + targetName + ", targetRole: " + targetRole + ", sourceEventId: " + sourceEventId;
	}
}
