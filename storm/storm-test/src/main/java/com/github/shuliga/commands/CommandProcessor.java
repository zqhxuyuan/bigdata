package com.github.shuliga.commands;

import java.util.Arrays;
import java.util.Map;
import java.util.Scanner;

import com.github.shuliga.http.HttpClient;

/**
 * User: yshuliga
 * Date: 07.01.14 13:48
 */
public class CommandProcessor {
	private HttpClient client;
	private Scanner scanner;
	private Map<String, Object> context;

	public CommandProcessor(HttpClient client, Scanner scanner, Map<String, Object> context){
		this.client = client;
		this.scanner = scanner;
		this.context = context;
	}

	public void execute(String commandText){
		try {
			if (commandText != null && !commandText.isEmpty()){
				String[] orig = commandText.split("\\s+");
				String[] args = orig.length >= 2 ? Arrays.copyOfRange(orig,1, orig.length) : new String[]{};
				ConsoleCommands.valueOf(orig[0].toUpperCase()).execute(client.prepare(fillPath(orig[0].toLowerCase())), scanner, context, args);
			}
		} catch (IllegalArgumentException iae) {
			System.out.println(" - unknown command");
		} catch (Exception e){
			e.printStackTrace();
		}
	}

	private String fillPath(String command) {
		String path = "register".equals(command) ?
			"/register":
			"/command/" + context.get("token") ;
		return path;
	}
}
