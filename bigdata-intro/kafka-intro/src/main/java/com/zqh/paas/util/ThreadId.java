package com.zqh.paas.util;

import java.util.UUID;

public class ThreadId {
	private static ThreadLocal<String> threadLocal = new ThreadLocal<String>();

	private ThreadId() {

	}

	public static String getThreadId() {
		if (threadLocal.get() == null) {
			threadLocal.set(String.valueOf(UUID.randomUUID()
					.getLeastSignificantBits()));
		}
		return threadLocal.get();
	}

	public static void setThreadId(String threadId) {
		threadLocal.set(threadId);
	}

	public static void setThreadIdWithIP(String ip) {
		threadLocal.set(ip + ":" + UUID.randomUUID().getLeastSignificantBits());
	}
}
