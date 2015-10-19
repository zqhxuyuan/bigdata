package com.github.shuliga.spout;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import com.google.gson.Gson;
import com.github.shuliga.context.JMSContext;
import com.github.shuliga.context.JMSContextProducer;
import com.github.shuliga.data.PharmacyEventData;
import com.github.shuliga.event.command.CommandEvent;

import javax.jms.JMSException;
import javax.jms.MessageConsumer;
import javax.jms.ObjectMessage;
import javax.jms.Session;
import java.util.Map;

/**
 * User: yshuliga
 * Date: 06.01.14 11:20
 */
public class CommandEventSpout extends BaseRichSpout {

	private SpoutOutputCollector _connector;
	private transient Gson gson;
	private transient Session session;
	private transient MessageConsumer consumer;

	@Override
	public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
		_connector = spoutOutputCollector;
		gson = new Gson();
		try{
			session = JMSContext.INSTANCE.connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
			consumer = session.createConsumer(JMSContext.INSTANCE.dataQueue);
		} catch (Exception e){
			e.printStackTrace();
		}
	}

	@Override
	public void nextTuple() {
		ObjectMessage message = null;
		try {
			message = (ObjectMessage) consumer.receive();
			if (message != null){
				CommandEvent command = (CommandEvent) message.getObject();
				_connector.emit(new Values(command.commandName, command.payload, command.targetRole, command.targetName, String.valueOf(command.id), command.sourceId, command.sourceEventId));
			}
		} catch (JMSException e) {
			e.printStackTrace();
		}
	}

	private String getPayloadJSON(PharmacyEventData data) {
		return gson.toJson(data);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("action", "message", "targetRole", "targetName", "eventId", "senderName", "sourceEventId"));
	}

	@Override
	public void close() {
		try {
			session.close();
		} catch (JMSException e) {
		}
	}

}
