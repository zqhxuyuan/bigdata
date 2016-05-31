package com.zqh.paas.message;

import java.io.Serializable;

import net.sf.json.JSONObject;

public class Message implements Serializable {
	private static final long serialVersionUID = -8999926014129704627L;
	private String topic;
	private Object msg;
	private int id;
	private String group;

	@SuppressWarnings("unchecked")
	public <T> T getDeserializeJsonObject(Class<T> cls) {
		return (T) JSONObject.toBean(JSONObject.fromObject(msg), cls);
	}

	public int getId() {
		return id;
	}

	public void setId(int id) {
		this.id = id;
	}

	public String getTopic() {
		return topic;
	}

	public void setTopic(String topic) {
		this.topic = topic;
	}

	public Object getMsg() {
		return msg;
	}

	public void setMsg(Object msg) {
		this.msg = msg;
	}

	public String getGroup() {
		return group;
	}

	public void setGroup(String group) {
		this.group = group;
	}

	public String toString() {
		StringBuffer sb = new StringBuffer();
		sb.append(this.getId()).append(":").append(this.getTopic()).append(":");
		sb.append(this.getMsg());
		return sb.toString();
	}
}
