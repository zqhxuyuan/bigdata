package com.zqh.paas.util;

import java.io.FileInputStream;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.security.KeyStore;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.Signature;
import java.security.cert.Certificate;
import java.security.cert.CertificateFactory;
import org.apache.commons.codec.binary.Base64;

import org.apache.log4j.Logger;

public class SignUtil {
	public static final Logger log = Logger.getLogger(SignUtil.class);

	private static final String PKCS12 = "PKCS12";
	private static final String X509 = "X.509";
	private static final String SHA1withRSA = "SHA1withRSA";
	private final static String MD5 = "MD5";
	private final static String SHA1 = "SHA-1";
	private final static String CHARSET = "UTF-8";

	private static PrivateKey loadPrivateKey(String pfxFile, String pfxPwd,
			String privateKeyPwd) {
		try {
			InputStream bis = new FileInputStream(pfxFile);
			KeyStore store = KeyStore.getInstance(PKCS12);
			store.load(bis, pfxPwd.toCharArray());
			String alias = store.aliases().nextElement();
			return (PrivateKey) store
					.getKey(alias, privateKeyPwd.toCharArray());
		} catch (Exception e) {
			log.error("load private key failed.", e);
			return null;
		}
	}

	private static PublicKey loadPublicKey(String cerFile) {
		try {
			InputStream bis = new FileInputStream(cerFile);
			CertificateFactory cf = CertificateFactory.getInstance(X509);
			Certificate cert = cf.generateCertificate(bis);
			return cert.getPublicKey();
		} catch (Exception e) {
			log.error("load public key failed.", e);
			return null;
		}
	}

	public static String sign(String plainText, String charset, String pfxFile,
			String pfxPwd, String privateKeyPwd) {
		try {
			Signature signature = Signature.getInstance(SHA1withRSA);
			signature.initSign(loadPrivateKey(pfxFile, pfxPwd, privateKeyPwd));
			if (charset != null && charset.length() > 0) {
				signature.update(plainText.getBytes(charset));
			} else {
				signature.update(plainText.getBytes());
			}
			byte[] sign = signature.sign();
			return new String(InetTool.hex2Ascii(sign));
		} catch (Exception e) {
			log.error("", e);
		}
		return null;
	}

	public static String sign(String plainText, String pfxFile, String pfxPwd,
			String privateKeyPwd) {
		return sign(plainText, null, pfxFile, pfxPwd, privateKeyPwd);
	}

	public boolean verifySignature(String sign, String plainText, String cerFile) {
		try {
			PublicKey key = loadPublicKey(cerFile);
			byte[] signArray = InetTool.ascii2Hex(sign.getBytes());
			Signature signature = Signature.getInstance(SHA1withRSA);
			signature.initVerify(key);
			signature.update(plainText.getBytes());
			return signature.verify(signArray);
		} catch (Exception e) {
			log.error("", e);
		}
		return false;
	}

	public static boolean verifySignatureWithCharset(String sign,
			String plainText, String cerFile, String charset) {
		try {
			PublicKey key = loadPublicKey(cerFile);
			byte[] signArray = InetTool.ascii2Hex(sign.getBytes());
			Signature signature = Signature.getInstance(SHA1withRSA);
			signature.initVerify(key);
			signature.update(plainText.getBytes(charset));
			return signature.verify(signArray);
		} catch (Exception e) {
			log.error("", e);
		}
		return false;
	}

	/**
	 * 指定算法对象
	 * 
	 * @param algorithm
	 * @return
	 */
	public static MessageDigest getMessageDigest(String algorithm) {
		MessageDigest digest = null;
		try {
			digest = MessageDigest.getInstance(algorithm);
		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
		}
		return digest;
	}

	/**
	 * MD5摘要后BASE64编码
	 * 
	 * @param input
	 * @return
	 */
	public static String MD5(String input) {
		MessageDigest digest = null;
		byte[] resBytes = null;
		try {
			digest = getMessageDigest(MD5);
			digest.update(input.getBytes(CHARSET));
			resBytes = digest.digest();
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		}
		return new String(Base64.encodeBase64(resBytes));
	}

	/**
	 * SHA1摘要后BASE64编码
	 * 
	 * @param input
	 * @return
	 */
	public static String SHA1(String input) {
		MessageDigest digest = null;
		byte[] resBytes = null;
		try {
			digest = getMessageDigest(SHA1);
			digest.update(input.getBytes(CHARSET));
			resBytes = digest.digest();
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		}
		return new String(Base64.encodeBase64(resBytes));
	}

	public static void main(String[] args) {
		String strSendData = "test date";
		String sign = SignUtil.sign(strSendData, "GBK",
				"/Volumes/HD/Downloads/weg_private_key.pfx", "aipay123456",
				"aipay654321");
		System.out.println(sign);
		boolean ret = SignUtil.verifySignatureWithCharset(sign, strSendData,
				"/Volumes/HD/Downloads/weg_public_key.cer", "GBK");
		System.out.println(ret);
		System.out.println("lc，MD5加密后的结果:" + MD5("lc"));
		System.out.println("lc，SHA-1加密后的结果：" + SHA1("woego.cn"));
	}
}
