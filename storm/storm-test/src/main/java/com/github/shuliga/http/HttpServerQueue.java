package com.github.shuliga.http;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.util.Date;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * User: yshuliga
 * Date: 21.11.13 15:25
 */
public class HttpServerQueue extends Thread {

	public static final int CODE_OK = 200;
	public static final int CODE_NO_SERVICE = 503;

	public static final int REQUESTS_COUNT = 5000;
	public static final int TTW = 1000;
	public static final int PORT = 9998;
	public static AtomicInteger counter = new AtomicInteger(0);
	public volatile boolean stop = false;
	private Timer timer;

	public static class SpoutRequest {
		public int personId;
		public String comment;
		public Date date;
	}

	private static BlockingQueue<SpoutRequest> queue = new SynchronousQueue<SpoutRequest>();

	class Handler implements HttpHandler {

		public void handle(HttpExchange xchg) throws IOException {
			BufferedReader streamReader = new BufferedReader(new InputStreamReader(xchg.getRequestBody(), "UTF-8"));
			StringBuilder requestStrBuilder = new StringBuilder();
			String inputStr;
			while ((inputStr = streamReader.readLine()) != null){
				requestStrBuilder.append(inputStr);
			}
			Gson gson = new GsonBuilder()
					.setDateFormat("dd-MM-yyyy")
					.create();
			SpoutRequest spoutRequest = gson. fromJson(requestStrBuilder.toString(), SpoutRequest.class);
			try {
				queue.put(spoutRequest);
				counter.incrementAndGet();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}

			xchg.sendResponseHeaders(CODE_OK, 0);
			OutputStream os = xchg.getResponseBody();
			os.close();
			if (counter.get() >= REQUESTS_COUNT){
				stopServer();
			}
		}
	}

	private HttpServer server;

	public HttpServerQueue(){
		try {
			server =  HttpServer.create(new InetSocketAddress(PORT), 0);
			server.createContext("/spout", new Handler());
			server.start();
			System.out.println("HTTP Server started");
			initCounterLogging(TTW);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	private void initCounterLogging(final int timeToWait) {
		timer = new Timer();
		timer.schedule(new TimerTask() {
			@Override
			public void run() {
				printCounterStats();
			}
		}, 0, timeToWait);
	}

	private void printCounterStats() {
		System.out.println(" Requests processed: " + counter.get() + ", queue RemainingCapacity: " + queue.remainingCapacity());
	}

	public void stopServer(){
		timer.cancel();
		server.stop(0);
		printCounterStats();
		System.out.println("HTTP Server stopped");
	}

	public static void main(String[] args) {
		HttpServerQueue server = new HttpServerQueue();
		server.start();
	}

	public static BlockingQueue<SpoutRequest> getQueue() {
		return queue;
	}
}
