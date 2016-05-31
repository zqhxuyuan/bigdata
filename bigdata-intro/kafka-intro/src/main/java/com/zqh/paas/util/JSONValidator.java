package com.zqh.paas.util;

import net.sf.json.JSONObject;

public class JSONValidator {

	private JSONValidator() {

	}

	public static boolean isChanged(JSONObject jsonObj, String key, String value) {
		if (jsonObj.containsKey(key) && jsonObj.getString(key) != null
				&& !jsonObj.getString(key).equals(value)) {
			return true;
		}
		return false;
	}

	public static boolean isChanged(JSONObject jsonObj, String key, int value) {
		if (jsonObj.containsKey(key) && jsonObj.getString(key) != null
				&& !jsonObj.getString(key).equals(value+"")) {
			return true;
		}
		return false;
	}	
	public static void main(String[] args) {

	}

}
