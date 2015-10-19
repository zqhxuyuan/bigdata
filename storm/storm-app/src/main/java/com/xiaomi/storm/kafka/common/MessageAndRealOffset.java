package com.xiaomi.storm.kafka.common;

import kafka.message.Message;

public class MessageAndRealOffset {
	
	/**
	 * @return the msg
	 */
	public Message getMsg() {
		return msg;
	}

	/**
	 * @return the offset
	 */
	public long getOffset() {
		return offset;
	}

	private Message msg;
	private long offset;
	
	public MessageAndRealOffset(Message msg, long offset) {
		this.msg = msg;
		this.offset = offset;
	}
}
