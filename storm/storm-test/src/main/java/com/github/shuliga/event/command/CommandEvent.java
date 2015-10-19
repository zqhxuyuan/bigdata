package com.github.shuliga.event.command;

import com.github.shuliga.event.common.AbstractEvent;
import com.github.shuliga.event.common.EventType;

/**
 * User: yshuliga
 * Date: 06.01.14 11:58
 */
public class CommandEvent extends AbstractEvent {

	static final long serialVersionUID = 999945367678479L;

	public String commandName;
	public String targetRole;
	public String targetName;
	public String sourceEventId;

	public CommandEvent(String commandName, String targetName, String targetRole, String payload, String sourceId, String sourceEventId){
		super(payload, sourceId);
		this.commandName = commandName;
		this.targetName = targetName;
		this.targetRole = targetRole;
		this.sourceEventId = sourceEventId;
	}


	@Override
	protected EventType createEventType() {
		return new CommandEventType();
	}
}
