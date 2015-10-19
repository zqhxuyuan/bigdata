package com.github.shuliga.event.common;

import java.io.Serializable;
import java.util.Date;
import java.util.Random;

/**
 * User: yshuliga
 * Date: 06.01.14 11:49
 */
public abstract class AbstractEvent implements Serializable{

	static final long serialVersionUID = 25378367678479L;
	private static Random rnd = new Random();

	public String id;
	public Date timestamp;
	public String sourceId;
	public EventType type;
	public Object payload;

	public AbstractEvent(){
		type = createEventType();
		timestamp = new Date();
		id = String.valueOf(rnd.nextLong());
	}

	public AbstractEvent(Object payload, String sourceId){
		this();
		this.payload = payload;
		this.sourceId = sourceId;
	}

	protected abstract EventType createEventType();

	public String getEventTypeName(){
		return type.getTypeName();
	}

}
