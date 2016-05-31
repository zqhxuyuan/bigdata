package com.zqh.paas.message;


public interface IMessageListener {
	public void receiveMessage(Message message, MessageStatus status);
}
