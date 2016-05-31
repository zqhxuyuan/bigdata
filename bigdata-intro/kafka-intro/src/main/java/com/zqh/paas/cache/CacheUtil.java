package com.zqh.paas.cache;

import java.text.SimpleDateFormat;
import java.util.*;

import com.zqh.paas.PaasContextHolder;
import com.zqh.paas.cache.ICache;

public class CacheUtil {

	private static ICache cacheSv = null;

	private CacheUtil() {}

	private static ICache getIntance() {
		if (null != cacheSv)
			return cacheSv;
		return  (ICache) PaasContextHolder.getContext().getBean("cacheSv");
	}

	public static void addItem(String key, Object object) {
		getIntance().addItem(key, object);
	}
	public static void addItem(String key, Object object, int seconds) {
		getIntance().addItem(key, object, seconds);
	}
	public static Object getItem(String key) {
		return getIntance().getItem(key);
	}
	public static void delItem(String key) {
		getIntance().delItem(key);
	}
	public static long getIncrement(String key) {
		return getIntance().getIncrement(key);
	}
	public static void addMap(String key, Map<String, String> map) {
		getIntance().addMap(key, map);
	}
	public static Map<String, String> getMap(String key) {
		return getIntance().getMap(key);
	}
	public static String getMapItem(String key,String field) {
		return getIntance().getMapItem(key, field);
	}
	public static void addMapItem(String key,String field,String value) {
		getIntance().addMapItem(key, field,value);
	}
	public static void delMapItem(String key,String field) {
		getIntance().delMapItem(key, field);
	}
	public static void addSet(String key, Set<String> set) {
		getIntance().addSet(key, set);
	}
	public static Set<String> getSet(String key) {
		return getIntance().getSet(key);
	}
	public static void addList(String key, List<String> list) {
		getIntance().addList(key, list);
	}
	public static List<String> getList(String key) {
		return getIntance().getList(key);
	}
	public static void addItemFile(String key, byte[] file) {
		getIntance().addItemFile(key, file);
	}
}
