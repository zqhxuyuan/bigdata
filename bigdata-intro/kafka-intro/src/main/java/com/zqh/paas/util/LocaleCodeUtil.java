package com.zqh.paas.util;

public class LocaleCodeUtil {

	private static ThreadLocal<String> threadLocal = new ThreadLocal<String>();

	private LocaleCodeUtil() {

	}

	public static String getLocaleCode() {
		return threadLocal.get();
	}

	public static void setLocaleCode(String localeCode) {
		threadLocal.set(localeCode);
	}

}
