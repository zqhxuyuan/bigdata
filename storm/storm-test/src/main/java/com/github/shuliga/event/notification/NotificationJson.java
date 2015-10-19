package com.github.shuliga.event.notification;

import javax.xml.bind.annotation.XmlRootElement;
import java.io.Serializable;

/**
 * User: yshuliga
 * Date: 10.01.14 17:29
 */
@XmlRootElement
public class NotificationJson implements Serializable{
	static final long serialVersionUID = 2092835678479L;

	public String eventId;
	public String message;
	public String senderName;
	public String eventPayloadReference;

	public NotificationJson(){
		super();
	}

	public NotificationJson(String eventId, String message, String senderName, String eventPayloadReference){
		this.eventId = eventId;
		this.eventPayloadReference = eventPayloadReference;
		this.message = message;
		this.senderName = senderName;
	}

	public boolean isEmpty() {
		return message != null && senderName != null;
	}

	@Override
	public String toString() {
		return "Sender: " + senderName + ", message:" + message + ", eventId" + eventId;
	}
}
