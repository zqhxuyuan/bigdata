package com.zqh.paas.log.impl;

import java.util.ArrayList;
import java.util.List;

import net.sf.json.JSONObject;

import org.apache.log4j.AppenderSkeleton;
import org.apache.log4j.spi.LoggingEvent;

import com.zqh.paas.config.ConfigurationCenter;
import com.zqh.paas.message.impl.MessageSender;
import com.zqh.paas.util.InetTool;
import com.zqh.paas.util.ThreadId;

public class MessageLogAppender extends AppenderSkeleton {

	private static MessageSender sender = null;
	private static final String localIp = InetTool.getHostAddr();
	private String logTopic = "paas_log_mongo_topic";
	private String appName = "unkown";
	private static String ccAddr = null;
	private static String runMod = ConfigurationCenter.PROD_MODE;
	private static ConfigurationCenter confCenter = null;
	private static String confPath = "/com/zqh/paas/message/logMessageSender";
	private static int timeOut = 10000;
	private static String configurationFile = "/paasConf.properties";

	public MessageLogAppender() {

	}

	private static MessageSender getSender() {
		if (sender == null) {
			init();
		}
		return sender;
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	private static synchronized void init() {
		if (confCenter == null) {
			List files = new ArrayList();
			files.add(configurationFile);
			confCenter = new ConfigurationCenter(ccAddr, timeOut, runMod, files);
			confCenter.init();
		}
		if (sender == null) {
			sender = new MessageSender();
			sender.setConfCenter(confCenter);
			sender.setConfPath(confPath);
			sender.init();
		}
	}

	protected void append(LoggingEvent event) {
		JSONObject json = new JSONObject();
		json.put("thread_id", ThreadId.getThreadId());
		json.put("ip_addr", localIp);
		json.put("app_name", appName);
		json.put("log_time", event.timeStamp);
		json.put("class_name", event.getLocationInformation().getClassName());
		json.put("method_name", event.getLocationInformation().getMethodName());
		json.put("line_number", event.getLocationInformation().getLineNumber());
		json.put("logger_name", event.getLoggerName());
		json.put("log_level", event.getLevel().toString());
		json.put("log_msg", event.getMessage());
		getSender().sendMessage(json, logTopic);

	}

	public void close() {
		if (this.closed) {
			return;
		}
		this.closed = true;
	}

	public boolean requiresLayout() {
		return true;
	}

	public String getLogTopic() {
		return logTopic;
	}

	public void setLogTopic(String logTopic) {
		this.logTopic = logTopic;
	}

	public String getAppName() {
		return appName;
	}

	public void setAppName(String appName) {
		this.appName = appName;
	}

	public String getConfPath() {
		return confPath;
	}

	@SuppressWarnings("static-access")
	public void setConfPath(String confPath) {
		this.confPath = confPath;
	}

	public String getCcAddr() {
		return ccAddr;
	}

	@SuppressWarnings("static-access")
	public void setCcAddr(String ccAddr) {
		this.ccAddr = ccAddr;
	}

	public String getRunMod() {
		return runMod;
	}

	@SuppressWarnings("static-access")
	public void setRunMod(String runMod) {
		this.runMod = runMod;
	}

	public String getConfigurationFile() {
		return configurationFile;
	}

	@SuppressWarnings("static-access")
	public void setConfigurationFile(String configurationFile) {
		this.configurationFile = configurationFile;
	}
}
