package com.zqh.paas.util;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class Md5Encoder {

	private Md5Encoder() {
	}

	public static String encode(String rawPass) {

		MessageDigest messageDigest = getMessageDigest();

		byte[] digest = messageDigest.digest(rawPass.getBytes());

		return Base64Util.encode(digest).toLowerCase();
	}

	protected final static MessageDigest getMessageDigest()
			throws IllegalArgumentException {
		try {
			return MessageDigest.getInstance("MD5");
		} catch (NoSuchAlgorithmException e) {
			throw new IllegalArgumentException("No such algorithm [" + "MD5"
					+ "]");
		}
	}

	public static boolean isPasswordValid(String encPass, String rawPass) {
		String pass1 = "" + encPass;
		String pass2 = encode(rawPass);

		return pass1.equals(pass2);
	}

	public static void main(String[] args) {
		String result = Md5Encoder.encode("111111");
		System.out.println("password=" + result);

		boolean isGood = Md5Encoder.isPasswordValid(
				"E10ADC3949BA59ABBE56E057F20F883E", "111111");
		System.out.println("isGood=" + isGood);
	}

}
