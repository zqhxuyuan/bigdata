package org.shirdrn.storm.analytics.common;

import org.shirdrn.storm.api.common.GenericResult;

public class KeyedResult<T> extends GenericResult {

	private static final long serialVersionUID = 1L;
	private String key;
	private T data;
	
	public String getKey() {
		return key;
	}
	public void setKey(String key) {
		this.key = key;
	}
	public T getData() {
		return data;
	}
	public void setData(T data) {
		this.data = data;
	}
	
	@Override
	public String toString() {
		StringBuffer sb = new StringBuffer();
		sb
		.append("[indicator=").append(indicator).append(",")
		.append("key=").append(key).append(",");
		if(data == null) {
			sb.append("data=").append("]");
		} else {
			sb.append("data=").append(data).append("]");
		}
		return sb.toString();
	}
	
}
