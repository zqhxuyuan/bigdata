package com.github.shuliga.http;

import org.apache.commons.io.IOUtils;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Random;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * User: yshuliga
 * Date: 21.11.13 18:16
 */
public class HttpTestClient {

	public static final String LOCALHOST = "localhost";

	static final int DELAY = 1;
	static final int RPS = 500;
	static final int ONE_SECOND = 1000;

	static final int TTW = 1000;

	static AtomicInteger requestsCount = new AtomicInteger();
	static AtomicInteger failedRequestsCount = new AtomicInteger();
	static Timer timer;

	static final String[] words = new String[] {"nathan", "mike", "jackson", "golda", "bertels"};
	static final String CHARSET_NAME = "UTF-8";
	static final Random rand = new Random();

	public static void main(String[] args) {
		assertTimingParameters();
		final String host = args.length > 0 ? args[0] : LOCALHOST;
		final int port = args.length > 1 ? Integer.parseInt(args[1]) : HttpServerQueue.PORT;
		final String stormRequestUrl = getStormRequestUrl(host, port);
		initCounterLogging(TTW);
		System.out.println("Test started.");
		do {
			new Thread(){
				@Override
				public void run() {
					int code = doRequest(stormRequestUrl, getJson());
					if (code == HttpServerQueue.CODE_OK) {
						requestsCount.incrementAndGet();
					} else {
						failedRequestsCount.incrementAndGet();
					}
				}
			}.start();
			try {
				Thread.sleep(ONE_SECOND / RPS - DELAY );
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		} while (requestsCount.get() < HttpServerQueue.REQUESTS_COUNT);
		stopTest();
	}

	private static int doRequest(String url, String json){
		HttpURLConnection connection;
		try {
			connection = (HttpURLConnection)new URL(url).openConnection();
		} catch (IOException e) {
			return -1;
		}
		connection.setDoOutput(true); // Triggers POST.
		connection.setRequestProperty("Accept-Charset", CHARSET_NAME);
		connection.setRequestProperty("Content-Type", "application/json");
		OutputStream output = null;
		InputStream response = null;
		BufferedReader reader = null;
		int status = -1;
		try {
			output = connection.getOutputStream();
			output.write(json.getBytes(CHARSET_NAME));
			response = connection.getInputStream();
			reader = new BufferedReader(new InputStreamReader(response, CHARSET_NAME));
			for (String line; (line = reader.readLine()) != null;) { // explicitly read response content
			}
			status = connection.getResponseCode();
		} catch (IOException e) {
			status = HttpServerQueue.CODE_NO_SERVICE;
			System.out.println(e.getMessage());
		} finally {
			IOUtils.closeQuietly(output);
			IOUtils.closeQuietly(reader);
			IOUtils.closeQuietly(response);
			connection.disconnect();
		}
		return status;
	}

	private static String getWord() {
		return words[rand.nextInt(words.length)];
	}

	private static String getStormRequestUrl(String host, int port) {
		return "http://" + host + ":" + port + "/spout";
	}

	private static String getJson() {
		return "{\"personId\" : 100, \"comment\"  : \"" + getWord() + "\" , \"date\": \"21-12-2013\"}";
	}

	private static void initCounterLogging(final int timeToWait) {
		timer = new Timer();
		timer.schedule(new TimerTask() {
			@Override
			public void run() {
				printCounterStats();
			}
		}, 0, timeToWait);
	}

	private static void printCounterStats() {
		System.out.println(" Requests created: " + requestsCount.get() + ", failed: " + failedRequestsCount.get());
	}


	private static void stopTest() {
		timer.cancel();
		printCounterStats();
		System.out.println("Test complete.");
	}

	private static void assertTimingParameters() {
		if ((ONE_SECOND / RPS <= DELAY) || (DELAY >= ONE_SECOND)){
			System.out.println("Wrong RPS, TTW, DELAY combination. Impossible to execute.");
			System.exit(-1);
		}
	}

}
