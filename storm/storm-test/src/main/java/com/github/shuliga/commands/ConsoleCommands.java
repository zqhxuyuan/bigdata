package com.github.shuliga.commands;

import com.github.shuliga.event.command.CommandJson;
import com.github.shuliga.event.notification.NotificationJson;
import com.github.shuliga.http.HttpClient;
import com.github.shuliga.security.Credentials;
import com.github.shuliga.security.Token;

import java.util.Map;
import java.util.Scanner;

/**
 * User: yshuliga
 * Date: 07.01.14 13:54
 */
public enum ConsoleCommands {

	HELP {
		@Override
		public void execute(HttpClient client, Scanner scanner, Map<String, Object> context, String[] args) {
			System.out.println("Available commands:");
			for(ConsoleCommands value : ConsoleCommands.values()){
				System.out.println(value.name());
			}
		}

	},

	REGISTER {
		@Override
		public void execute(HttpClient client, Scanner scanner, Map<String, Object> context, String[] args) {
			if (args.length == 2) {
			Credentials credentials = new Credentials(args[0], args[1]);
				Token token = client.post(Token.class, credentials);
				if (token != null) {
					String existingToken = (String) context.get(Constants.TOKEN);
					if ( existingToken != null && existingToken.equals(token.token) ) {
						System.out.println("Client is already registered");
					} else {
						if (!token.token.equals("null")) {
							context.put(Constants.TOKEN, token.token);
							context.put(Constants.CREDENTIALS, credentials);
							System.out.println("Client has been registered, the token is: " + token.token);
						} else {
							System.out.println("Credentials not recognized. Try again.");
						}
					}
				}
			} else {
				client.head();
				System.out.println("Wrong arguments");
			}
		}
	},

	LOGOFF {
		@Override
		public void execute(HttpClient client, Scanner scanner, Map<String, Object> context, String[] args) {
			String existingToken = (String) context.get(Constants.TOKEN);
			if (existingToken != null) {
				client.post(createCommand("logoff", null, existingToken, null));
				context.remove(Constants.TOKEN);
				context.remove(Constants.CREDENTIALS);
				getEventsContext(context).clear();
				System.out.println("Logged off");
			} else {
				System.out.println("Can not logoff. Do register first.");
			}
		}
	},

	EVENT {
		@Override
		public void execute(HttpClient client, Scanner scanner, Map<String, Object> context, String[] args) {
			if (args.length == 1){
				String eventId = args[0];
				displayEvent(eventId, getEventsContext(context));
				System.out.println(" -- type 'v' + 'Enter' to fetch content, or 'Enter' to prompt");
				if (scanner.nextLine().equalsIgnoreCase("v")){
					String existingToken = (String) context.get(Constants.TOKEN);
					client.prepare("/payload/" + existingToken, HttpClient.TEXT_PLAIN, "id", eventId);
					System.out.println(client.get(String.class));
				}
			} else {
				for (Map.Entry<String, Object> entry : getEventsContext(context).entrySet()){
					displayEvent(entry.getKey(), getEventsContext(context));
				}
			}
		}
	},

	DELEGATE {
		@Override
		public void execute(HttpClient client, Scanner scanner, Map<String, Object> context, String[] args) {
			String eventId = args[0];
			String targetName = args.length == 2 ? args[1] : null;
			NotificationJson event = (NotificationJson) getEventsContext(context).get(eventId);
			if (event == null){
				System.out.println("No case to delegate.");
			} else {
				if (targetName == null) {
					System.out.println(" - receiver name missed");
					return;
				}
				client.post(createCommand("delegate", event.eventId, "Please, follow up this case: " + event.eventId, targetName));
				System.out.println("Case " + event.eventId + " was delegated to " + targetName);
				getEventsContext(context).remove(eventId);
			}
		}
	},

	EXIT {
		@Override
		public void execute(HttpClient client, Scanner scanner, Map<String, Object> context, String[] args) {

		}

	};


	private static void displayEvent(String key, Map<String, Object> eventsContext) {
		NotificationJson event = (NotificationJson) eventsContext.get(key);
		System.out.println(event.eventId + ": " + event.message + ", from: " + event.senderName);
	}

	private static Map<String, Object> getEventsContext(Map<String, Object> context) {
		return ((Map<String, Object>)context.get("eventsContext"));
	}

	private static CommandJson createCommand(String action, String eventId, String message, String targetName) {
		return new CommandJson(action, message, eventId, Credentials.Roles.NURSE.getLabel(), targetName);
	}

	public abstract void execute(HttpClient client, Scanner scanner, Map<String, Object> context, String[] args);

	public  static class Constants {
		public static final String EVENT_ID = "eventId";
		public static final String TOKEN = "token";
		public static final String CREDENTIALS = "credentials";
	}
}
