package com.zqh.paas.message.impl;

import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;

import org.apache.log4j.Logger;

import com.zqh.paas.message.IMessageListener;
import com.zqh.paas.message.Message;
import com.zqh.paas.message.MessageStatus;



public class MessageProcessor implements Runnable {
	public static final Logger log = Logger.getLogger(MessageProcessor.class);
	
	private String threadName = null;
	private KafkaStream<String, Message> stream = null;
	private IMessageListener listener = null;
	
	public MessageProcessor(int threadId, KafkaStream<String, Message> stream, IMessageListener listener) {
		this.threadName = "MessageProcessor " + threadId;
		this.stream = stream;
		this.listener = listener;
		if(log.isInfoEnabled()) {
			log.info(this.threadName + " started");
		}
	}
	public void run() {
		ConsumerIterator<String, Message> it = stream.iterator();
		Message msg = null;
		while(it.hasNext() ) {
			msg = it.next().message();
			if(log.isDebugEnabled()) {
				log.debug(threadName + " process msg:" + msg.toString());
			}
			MessageStatus status = new MessageStatus();
			listener.receiveMessage(msg, status);
		}
		if(log.isInfoEnabled()) {
			log.info(threadName + " is stopped");
		}
	}

}
