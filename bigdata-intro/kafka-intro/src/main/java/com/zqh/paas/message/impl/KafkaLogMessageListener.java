package com.zqh.paas.message.impl;

import net.sf.json.JSONObject;

import org.apache.log4j.Logger;

import com.zqh.paas.log.ILogWriter;
import com.zqh.paas.message.IMessageListener;
import com.zqh.paas.message.Message;
import com.zqh.paas.message.MessageStatus;

public class KafkaLogMessageListener implements IMessageListener {
	private static final Logger log = Logger.getLogger(KafkaLogMessageListener.class);

	private String logTopic = "paas_log_mongo_topic";
	private ILogWriter logWriter = null;

	public void receiveMessage(Message message, MessageStatus status) {
		if (logTopic.equals(message.getTopic())) {
			if (log.isDebugEnabled()) {
				log.debug("get log message: " + message.getMsg());
			}
			try {
				logWriter.write(JSONObject.fromObject(message.getMsg()));
			} catch (Exception e) {
				logWriter.write(null != message.getMsg() ? message.getMsg().toString() : "Null Object");
				e.printStackTrace();
			}
		}
	}

	public String getLogTopic() {
		return logTopic;
	}

	public void setLogTopic(String logTopic) {
		this.logTopic = logTopic;
	}

	public ILogWriter getLogWriter() {
		return logWriter;
	}

	public void setLogWriter(ILogWriter logWriter) {
		this.logWriter = logWriter;
	}

}
