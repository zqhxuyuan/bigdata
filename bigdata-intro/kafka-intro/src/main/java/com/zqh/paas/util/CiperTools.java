package com.zqh.paas.util;

import java.security.Key;

import javax.crypto.Cipher;
import javax.crypto.KeyGenerator;
import javax.crypto.SecretKey;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.DESKeySpec;
import javax.crypto.spec.IvParameterSpec;

import org.apache.log4j.Logger;

public class CiperTools {
	private static final Logger log = Logger.getLogger(CiperTools.class);

	public static final String KEY_ALGORITHM = "DES";
	public static final String DES_ECB_ALGORITHM = "DES/ECB/PKCS5Padding";
	public static final String DES_CBC_ALGORITHM = "DES/CBC/PKCS5Padding";
	public static final String DES_CBC_NOPADDING = "DES/CBC/NoPadding";
	public static final String SECURITY_KEY = "92ba640738a413b0";
	public static final byte[] DES_CBC_IV = {0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00};

	public static byte[] generateKey() {
		try {
			KeyGenerator keyGenerator = KeyGenerator.getInstance(KEY_ALGORITHM);
			keyGenerator.init(56);
			SecretKey secretKey = keyGenerator.generateKey();
			return InetTool.hex2Ascii(secretKey.getEncoded());
		} catch (Exception e) {
			log.error("exception:",e);
		}
		return null;
	}

	private static Key toKey(byte[] key) {
		try {
			DESKeySpec des = new DESKeySpec(key);
			SecretKeyFactory keyFactory = SecretKeyFactory.getInstance(KEY_ALGORITHM);
			SecretKey secretKey = keyFactory.generateSecret(des);
			return secretKey;
		} catch (Exception e) {
			log.error("exception:" ,e);
		}
		return null;
	}

	public static byte[] encrypt(byte[] data, byte[] key, String algorithm) {
		try {
			Key k = toKey(key);
			Cipher cipher = Cipher.getInstance(algorithm);
			if(DES_CBC_ALGORITHM.equals(algorithm) || DES_CBC_NOPADDING.equals(algorithm)) {
				IvParameterSpec spec = new IvParameterSpec(DES_CBC_IV);
				cipher.init(Cipher.ENCRYPT_MODE, k, spec);
			}else {
				cipher.init(Cipher.ENCRYPT_MODE, k);
			}
			return cipher.doFinal(data);
		} catch (Exception e) {
			log.error("exception:" ,e);
		}
		return null;
	}

	public static byte[] decrypt(byte[] data, byte[] key, String algorithm) {
		try {
			Key k = toKey(key);
			Cipher cipher = Cipher.getInstance(algorithm);
			if(DES_CBC_ALGORITHM.equals(algorithm) || DES_CBC_NOPADDING.equals(algorithm)) {
				IvParameterSpec spec = new IvParameterSpec(DES_CBC_IV);
				cipher.init(Cipher.DECRYPT_MODE, k,spec);
			}else {
				cipher.init(Cipher.DECRYPT_MODE, k);
			}
			return cipher.doFinal(data);
		} catch (Exception e) {
			log.error("exception:" , e);
		}
		return null;
	}
	
	public static String encrypt(String data) {
		byte[] aa = encrypt(data.getBytes(), InetTool.ascii2Hex(SECURITY_KEY.getBytes()),DES_ECB_ALGORITHM);
		return new String(InetTool.hex2Ascii(aa));
	}
	
	public static String decrypt(String data) {
		byte[] aa = InetTool.ascii2Hex(data.getBytes());
		return new String(decrypt(aa, InetTool.ascii2Hex(SECURITY_KEY.getBytes()),DES_ECB_ALGORITHM));
	}
	
	public static String encrypt(String data, String algorithm) {
		byte[] aa = encrypt(data.getBytes(), InetTool.ascii2Hex(SECURITY_KEY.getBytes()),algorithm);
		return new String(InetTool.hex2Ascii(aa));
	}
	
	public static String decrypt(String data, String algorithm) {
		byte[] aa = InetTool.ascii2Hex(data.getBytes());
		return new String(decrypt(aa, InetTool.ascii2Hex(SECURITY_KEY.getBytes()),algorithm));
	}
	
	public static byte[] paddingZero(byte[] in) {
		if(in == null || in.length == 0) {
			return null;
		}
		int inLen = in.length;
		int m = inLen % 8;
		byte[] out = null;
		if(m == 0) {
			out = new byte[inLen];
		}else {
			out = new byte[inLen + 8 - m];
		}
		int outLen = out.length;
		for(int i=0; i<outLen; i++) {
			if(i<inLen) {
				out[i] = in[i];
			}else {
				out[i] = 0x00;
			}
		}
		return out;
	}
	
	public static void main(String[] args) {
		String orign = "sa";
		System.out.println(CiperTools.encrypt(orign));
		System.out.println(CiperTools.decrypt(CiperTools.encrypt(orign)));
		
		byte[] in = orign.getBytes();
		System.out.println("00:" + new String(InetTool.hex2Ascii(in)));
		byte[] out = paddingZero(in);
		System.out.println("00:" + new String(InetTool.hex2Ascii(out)));
		byte[] aa = encrypt(out,SECURITY_KEY.getBytes(),DES_CBC_NOPADDING);
		//d10bb6123a72de91695c14346c9469e7
		//d10bb6123a72de91a2bb2489b69613b4
		System.out.println("aa:" + new String(InetTool.hex2Ascii(aa)));
		byte[] bb = decrypt(aa,SECURITY_KEY.getBytes(),DES_CBC_NOPADDING);
		System.out.println("bb:" + new String(InetTool.hex2Ascii(bb)));
	}
}
