package com.zqh.paas.message.impl;

import java.util.Iterator;
import java.util.Properties;
import java.util.Random;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import net.sf.json.JSONObject;

import org.apache.log4j.Logger;

import com.zqh.paas.PaasException;
import com.zqh.paas.config.ConfigurationCenter;
import com.zqh.paas.config.ConfigurationWatcher;
import com.zqh.paas.message.Message;

public class MessageSender implements ConfigurationWatcher {
	public static final Logger log = Logger.getLogger(MessageSender.class);

	private String confPath = "/com/zqh/paas/message/messageSender";

	private ConfigurationCenter confCenter = null;

	private Producer<String, String> producer = null;

	private Properties props = null;

	private Random r = new Random();

	public void init() {
		if (log.isInfoEnabled()) {
			log.info("init MessageSender...");
		}
		try {
			process(confCenter.getConfAndWatch(confPath, this));
		} catch (PaasException e) {
			e.printStackTrace();
		}
	}

	@SuppressWarnings("rawtypes")
	public void process(String conf) {
		if (log.isInfoEnabled()) {
			log.info("new MessageSender configuration is received: " + conf);
		}
		JSONObject json = JSONObject.fromObject(conf);
		Iterator keys = json.keySet().iterator();
		boolean changed = false;
		if (keys != null) {
			String key = null;
			while (keys.hasNext()) {
				key = (String) keys.next();

				if (props == null) {
					props = new Properties();
					changed = true;
				}
				if (props.containsKey(key)) {
					if (props.get(key) == null || !props.get(key).equals(json.getString(key))) {
						props.put(key, json.getString(key));
						changed = true;
					}
				} else {
					props.put(key, json.getString(key));
					changed = true;
				}
			}
		}
		if (changed) {
			ProducerConfig cfg = new ProducerConfig(props);
			producer = new Producer<String, String>(cfg);
		}
	}

	private int getRandomId() {
		return Math.abs(r.nextInt());
	}

	public void sendMessage(Object message, String topic) {
		Message msg = new Message();
		msg.setId(getRandomId());
		msg.setTopic(topic);
		msg.setMsg(JSONObject.fromObject(message).toString());
		KeyedMessage<String, String> km = new KeyedMessage<String, String>(topic, String.valueOf(msg.getId()), JSONObject.fromObject(msg).toString());
		producer.send(km);
	}
	
	public void sendMessage(String message,String topic) {
		Message msg = new Message();
		msg.setId(getRandomId());
		msg.setTopic(topic);
		msg.setMsg(message);
		KeyedMessage<String, String> km = new KeyedMessage<String, String>(topic, String.valueOf(msg.getId()), JSONObject.fromObject(msg).toString());
		producer.send(km);
	}

	public String getConfPath() {
		return confPath;
	}

	public void setConfPath(String confPath) {
		this.confPath = confPath;
	}

	public ConfigurationCenter getConfCenter() {
		return confCenter;
	}

	public void setConfCenter(ConfigurationCenter confCenter) {
		this.confCenter = confCenter;
	}

	public void destroy() {
		if (null != producer) {
			producer.close();
		}
	}

	public enum MessageTopics{
		MESSAGE_TOPIC_GOODS;
	}
}
