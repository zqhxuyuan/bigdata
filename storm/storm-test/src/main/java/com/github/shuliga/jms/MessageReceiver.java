package com.github.shuliga.jms;

import com.github.shuliga.event.command.CommandJson;
import com.github.shuliga.event.notification.NotificationEvent;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.annotation.Resource;
import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.ObjectMessage;
import javax.jms.Queue;
import javax.jms.QueueConnection;
import javax.jms.QueueConnectionFactory;
import javax.jms.QueueSession;
import javax.jms.Session;

/**
 * User: yshuliga
 * Date: 08.01.14 14:15
 */
public class MessageReceiver {

	private QueueConnection connection;

	@Resource(name = "BSCA_POC")
	private QueueConnectionFactory connectionFactory;
	@Resource(mappedName = "jms/bsca_poc/NotificationEventQueue")
	private Queue queue;

	public MessageReceiver(){
	}

	@PostConstruct
	protected void init(){
		try {
			connection = connectionFactory.createQueueConnection();
			connection.start();
		} catch (JMSException e) {
			e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
		}
	}

	public NotificationEvent receiveNotification(String name){
		QueueSession session = null;
		NotificationEvent notification = null;
		try {
			session = connection.createQueueSession(false, Session.AUTO_ACKNOWLEDGE);
			MessageConsumer receiver = session.createReceiver(queue, "destinationName='" + name + "'");
			ObjectMessage message = (ObjectMessage) receiver.receive(100);
			if (message != null) {
				notification = (NotificationEvent) message.getObject();
			}
		} catch (Exception e) {
			e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
		} finally {
			if (session != null) {
				try {session.close();}
				catch (JMSException e) { }
			}
		}
		return notification;
	}

	@PreDestroy
	protected void clean(){
		if (connection != null) {
			try { connection.close(); }
			catch (JMSException e) { }
		}

	}

}
