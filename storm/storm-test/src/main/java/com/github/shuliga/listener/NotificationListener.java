package com.github.shuliga.listener;

import com.github.shuliga.http.HttpClient;
import com.github.shuliga.commands.ConsoleCommands;
import com.github.shuliga.event.notification.NotificationJson;

import java.util.Map;

/**
 * User: yshuliga
 * Date: 10.01.14 17:58
 */
public class NotificationListener implements Runnable {

	private static final long PULLING_TIMEOUT = 200;
	private final HttpClient client;
	private final Map<String, Object> context;
	private final Map<String, Object> eventsContext;

	public NotificationListener(String baseUrl, Map<String, Object> context, Map<String, Object> eventsContext){
		this.client = new HttpClient(baseUrl);
		this.context = context;
		this.eventsContext = eventsContext;
	}

	@Override
	public void run() {
		while (!Thread.interrupted()){
			NotificationJson notification = getNotification();
			if (notification != null && notification.isEmpty()) {
				System.out.println("New notification:" +notification.eventId +  ", from: " + notification.senderName);
				eventsContext.put(notification.eventId, notification);
			}
			try {
				Thread.sleep(PULLING_TIMEOUT);
			} catch (InterruptedException e) {
				break;
			}
		}
		System.out.println("Notification Listener stopped");
	}

	private NotificationJson getNotification() {
		NotificationJson notification = null;
		String token = (String) context.get("token");
		if (token != null){
			client.prepare("/notification", "token", token);
			try {
			notification = client.get(NotificationJson.class);
			} catch (Throwable e){
				e.printStackTrace();
			}
		}
		return notification;
	}
}
