package com.github.shuliga.bolt.stream;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import com.github.shuliga.context.UserContext;
import com.github.shuliga.http.HttpClient;
import com.github.shuliga.event.command.CommandJson;
import com.github.shuliga.security.Credentials;
import com.github.shuliga.security.Token;

import java.util.Map;
import java.util.Random;

/**
 * User: yshuliga
 * Date: 06.01.14 14:54
 */
public class PharmacyFraudDetectorBolt extends BaseRichBolt {

	public static final String PHARMACY_DATA_STREAM = "PharmacyDataStream";

	public static final String BASE_URL = "http://10.145.239.4:8080/api";

	private final Random rnd = new Random();
	private double threshold;
	private OutputCollector _collector;

	private HttpClient client;
	private String token;

	public PharmacyFraudDetectorBolt(Double threshold) {
		this.threshold = threshold;
	}

	@Override
	public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
		this._collector = outputCollector;
		client = new HttpClient(BASE_URL);
		token = apiRegister();
	}

	//"patientId", "eventData", "fraudIndex"
	@Override
	public void execute(Tuple tuple) {
		double fraudIndex = getFraudIndex(tuple);
		if (fraudIndex > threshold) {
			String eventId = createEventID();
			createNewEvent(getPatientId(tuple), fraudIndex, eventId);
			_collector.emit(new Values(PHARMACY_DATA_STREAM, eventId, tuple.getString(1)));
		}
		_collector.ack(tuple);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("sourceType", "eventId", "json"));
	}

	private void createNewEvent(String patientId, double fraudIndex, String eventId) {
		String messageText = createMessageText(patientId, fraudIndex);
		System.out.println(messageText);
		client.prepare("/command/" + token).post(createCommand(messageText, eventId));
	}

	private CommandJson createCommand(String messageText, String eventId) {
		CommandJson command = new CommandJson(CommandJson.Action.NOTIFY.getLabel(), messageText, eventId, Credentials.Roles.PHYSICIAN.getLabel(), null);
		return command;
	}

	private String createEventID() {
		return String.valueOf(rnd.nextLong());
	}

	private String createMessageText(String patientId, double fraudIndex) {
		return "NEW FRAUD EVENT DETECTED! PatientId: " + patientId + ", index: " + fraudIndex;
	}

	private double getFraudIndex(Tuple tuple) {
		return tuple.getDouble(2);
	}

	public String getPatientId(Tuple tuple) {
		return tuple.getString(0);
	}

	private String apiRegister() {
		return client.prepare("/register").post(Token.class, new Credentials(PHARMACY_DATA_STREAM, "system")).token;
	}

}
