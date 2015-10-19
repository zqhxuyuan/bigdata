package com.github.shuliga;

import com.github.shuliga.commands.CommandProcessor;
import com.github.shuliga.commands.ConsoleCommands;
import com.github.shuliga.http.HttpClient;
import com.github.shuliga.listener.NotificationListener;
import com.github.shuliga.utils.FileUtil;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;
import java.util.concurrent.ConcurrentHashMap;

/**
 * User: yshuliga
 * Date: 07.01.14 11:37
 */
public class ConsoleHttpClient {

	public static final String PROMPT = ">>";

	public static void main(String[] args) throws IOException {

		Map<String, Object> context = new ConcurrentHashMap<String, Object>();
		Map<String, Object> eventsContext = new ConcurrentHashMap<String, Object>();
		context.put("eventsContext", eventsContext);
		System.out.println("Console client started");
		String baseUrl = "http://54.83.31.41:8080/api";
		HttpClient client = new HttpClient(baseUrl);
		Scanner scanIn = new Scanner(System.in);
		CommandProcessor commandProcessor = new CommandProcessor(client, scanIn, context);
		Thread notificationListenerThread = new Thread(new NotificationListener(baseUrl, context, eventsContext));
		notificationListenerThread.start();
		System.out.println("Notification Listener started");
		String commandText = "";
		do {
			System.out.print(PROMPT);
			commandText = scanIn.nextLine();
			commandProcessor.execute(commandText);
		} while (!commandText.equals("exit"));
		notificationListenerThread.interrupt();
		System.out.println("Console client stopped");
	}

}
