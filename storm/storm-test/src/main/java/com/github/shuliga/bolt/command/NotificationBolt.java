package com.github.shuliga.bolt.command;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import com.github.shuliga.context.JMSContext;
import com.github.shuliga.event.notification.NotificationEvent;

import javax.jms.JMSException;
import javax.jms.MessageProducer;
import javax.jms.ObjectMessage;
import javax.jms.Session;
import java.util.Map;
import java.util.Random;

/**
 * User: yshuliga
 * Date: 06.01.14 14:54
 */
public class NotificationBolt extends BaseRichBolt {

	private final Random rnd = new Random();
	private double threshold;
	private OutputCollector _collector;

	private Session session;
	private MessageProducer producer;

	public NotificationBolt() {
	}

	@Override
	public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
		this._collector = outputCollector;
		try {
			session = JMSContext.INSTANCE.connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
			producer = session.createProducer(JMSContext.INSTANCE.notificationQueue);
		} catch (JMSException e) {
			e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
		}
	}

	// "destinationToken", "notificationMessage", "eventId", "eventPayloadReference", "senderName"
	@Override
	public void execute(Tuple tuple) {
		String destination = tuple.getString(0);
		if (destination != null) {
			sendNotification(tuple.getString(0), tuple.getString(1), tuple.getString(2), tuple.getString(3), tuple.getString(4));
			_collector.ack(tuple);
		} else {
			_collector.fail(tuple);
		}
	}

	private void sendNotification(String destinationName, String notificationMessage, String eventId, String eventPayloadReference, String senderName) {
		try {
			ObjectMessage message = session.createObjectMessage(new NotificationEvent(destinationName, notificationMessage, eventId, eventPayloadReference, senderName));
			message.setStringProperty("destinationName", destinationName);
			producer.send(message);
		} catch (JMSException e) {
			e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields());
	}

}
