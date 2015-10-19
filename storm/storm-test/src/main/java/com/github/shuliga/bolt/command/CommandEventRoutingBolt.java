package com.github.shuliga.bolt.command;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import com.github.shuliga.context.UserContext;
import com.github.shuliga.event.command.CommandJson;
import com.github.shuliga.security.Credentials;

import java.util.Map;
import java.util.Random;
import java.util.logging.Logger;

/**
 * User: yshuliga
 * Date: 06.01.14 14:54
 */
public class CommandEventRoutingBolt extends BaseRichBolt {

	private final Random rnd = new Random();
	private double threshold;
	private OutputCollector _collector;

	public CommandEventRoutingBolt() {
	}

	@Override
	public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
		this._collector = outputCollector;
	}

	//"action", "message", "targetRole", "targetName", "eventId", "senderName", "sourceEventId"

	@Override
	public void execute(Tuple tuple) {
		CommandJson.Action action = CommandJson.Action.valueOf(tuple.getString(0).toUpperCase());
		Logger.getLogger(getClass().getName()).warning("Command received: " + action);
		switch (action){
			case DELEGATE: {
				createNotification(getTargetName(tuple), tuple.getString(1), tuple.getString(5), tuple.getString(6));
				break;
			}
			case NOTIFY: {
				createNotification(getTargetName(tuple), tuple.getString(1), tuple.getString(5), tuple.getString(6));
				break;
			}
			case REGISTER: {
				registerUser(tuple.getString(1), tuple.getString(2), tuple.getString(3));
				break;
			}
			case LOGOFF: {
				logoffByToken(tuple.getString(1));
				break;
			}
		}
		_collector.ack(tuple);
	}

	private void registerUser(String token, String role, String name) {
		UserContext.getInstance().put(token, new Credentials(name, role));
	}

	private void logoffByToken(String token) {
		UserContext.getInstance().remove(token);
	}

	private String getTargetName(Tuple tuple) {
		String targetName = tuple.getString(3);
		if (targetName == null || targetName.isEmpty()){
			targetName = findNameByRole(tuple.getString(2));
		}
		return targetName;
	}

	private String findNameByRole(String role) {
		String name = null;
		for (Credentials credential : UserContext.getInstance().getAll()){
			if (credential.role.equals(role)) {
				name = credential.login;
			}
		}
		return name;
	}

	private void createNotification(String targetName, String message, String senderName, String sourceEventId) {
		System.out.println(createMessageText(targetName, message));
		_collector.emit(new Values(targetName, message, sourceEventId, null, senderName));
	}

	private String createMessageText(String targetName, String message) {
		return "Notifying active physician: " + targetName + ", with: " + message;
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("destinationName", "notificationMessage", "sourceEventId", "eventPayloadReference", "senderName"));
	}

}
