package com.github.shuliga.jms;

import com.github.shuliga.event.command.CommandEvent;
import com.github.shuliga.event.command.CommandJson;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.annotation.Resource;
import javax.ejb.Stateless;
import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;

/**
 * User: yshuliga
 * Date: 08.01.14 14:15
 */
public class MessageSender {

	private Connection connection;

	@Resource(name = "BSCA_POC")
	private ConnectionFactory connectionFactory;
	@Resource(mappedName = "jms/bsca_poc/DataEventQueue")
	private Queue queue;

	public MessageSender(){
	}

	@PostConstruct
	protected void init(){
		try {
			connection = connectionFactory.createConnection();
		} catch (JMSException e) {
			e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
		}

	}

	public void sendCommand(CommandEvent command) throws JMSException {
		Session session = null;
		try {
			session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
			MessageProducer producer = session.createProducer(queue);
			Message message = session.createObjectMessage(command);
			producer.send(message);
		} finally {
			if (session != null) {
				try {session.close();}
				catch (JMSException e) { }
			}
		}
	}

	@PreDestroy
	protected void clean(){
		if (connection != null) {
			try { connection.close(); }
			catch (JMSException e) { }
		}

	}

}
