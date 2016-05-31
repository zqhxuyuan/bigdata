package com.zqh.paas.util;

import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.SocketTimeoutException;
import java.net.URL;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class HttpUtil {
	private static int connectionTimeout = 10000;

	private static int readTimeout = 10000;

	private static final Log logger = LogFactory.getLog(HttpUtil.class);

	/**
	 * 图片服务器 上传图片
	 * @param url
	 * @return
	 */
	public static String upImage(String url,
			byte[] image,String name) {
		HttpURLConnection connection = null;
		BufferedReader in = null;
		String result = "";

		try {
			if (logger.isDebugEnabled()) {
				logger.debug("Attempting to access " + url);
			}
			URL logoutUrl = new URL(url);

			connection = (HttpURLConnection) logoutUrl.openConnection();
			connection.setDoInput(true);
			connection.setDoOutput(true);
			connection.setRequestMethod("POST");
			connection.setChunkedStreamingMode(1024*1024); 
			connection.setReadTimeout(readTimeout);
			connection.setConnectTimeout(connectionTimeout);
			connection.setRequestProperty("connection", "Keep-Alive");
			connection.setRequestProperty("Charsert", "UTF-8");
			connection.setRequestProperty("Content-Type","multipart/form-data;file="+name);
			connection.setRequestProperty("filename",name);

			DataOutputStream printout = new DataOutputStream(
					connection.getOutputStream());

			printout.write(image);
			printout.flush();
			printout.close();

			in = new BufferedReader(new InputStreamReader(
					connection.getInputStream()));
			String s = null;
			while ((s = in.readLine()) != null) {
				result += s;
			}
			if (logger.isDebugEnabled()) {
				logger.debug("Finished sending message to" + url);
			}
			return result;
		} catch (SocketTimeoutException e) {
			logger.warn("Socket Timeout Detected while attempting to send message to ["
					+ url + "].");
		} catch (Exception e) {
			logger.error(e, e);
		} finally {
			if (in != null)
				try {
					in.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			if (connection != null)
				connection.disconnect();
		}
		return null;
	}
	
	/**
	 * solr引擎更新索引
	 * @param url
	 * @return
	 */
	public static boolean updateIndx(String url) {
		HttpURLConnection connection = null;

		try {
			if (logger.isDebugEnabled()) {
				logger.debug("Attempting to access " + url);
			}
			URL logoutUrl = new URL(url);

			connection = (HttpURLConnection) logoutUrl.openConnection();
			connection.setDoInput(true);
			connection.setDoOutput(true);
			connection.setRequestMethod("POST");
			connection.setReadTimeout(readTimeout);
			connection.setConnectTimeout(connectionTimeout);
			connection.setRequestProperty("Charsert", "UTF-8");
			connection.setRequestProperty("Content-Type","text/html");
			
			int code = connection.getResponseCode();
			return code == 200;
		} catch (SocketTimeoutException e) {
			logger.warn("Socket Timeout Detected while attempting to send message to ["
					+ url + "].");
		} catch (Exception e) {
			logger.error(e, e);
		} finally {
			if (connection != null)
				connection.disconnect();
		}
		return false;
	}

}