package com.github.shuliga.event.notification;

import com.github.shuliga.event.common.EventType;

import java.io.Serializable;

/**
 * User: yshuliga
 * Date: 06.01.14 11:53
 */
public class NotificationEventType extends EventType implements Serializable{

	public static final Long serialVersionUID = 590293957827364L;

	public final String typeName = "NOTIFICATION_EVENT";

	@Override
	public String getTypeName() {
		return typeName;
	}
}
