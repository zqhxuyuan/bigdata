package com.github.shuliga.bolt.stream;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import com.github.shuliga.data.MedicineData;
import com.github.shuliga.data.PharmacyEventData;
import com.google.gson.Gson;

import java.util.Date;
import java.util.Map;

/**
 * User: yshuliga
 * Date: 06.01.14 14:16
 */
public class PharmacyFraudIndexBolt extends BaseRichBolt {

	private OutputCollector _collector;
	private Gson gson;

	@Override
	public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
		this._collector = outputCollector;
		gson = new Gson();
	}

	//"eventType", "patientId", "patientDataJSON"
	@Override
	public void execute(Tuple tuple) {
		double fraudIndex = getFraudIndex(gson.fromJson(tuple.getString(2), PharmacyEventData.class));
		_collector.emit(new Values(tuple.getString(1), tuple.getString(2), fraudIndex));
		_collector.ack(tuple);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("patientId", "eventData", "fraudIndex"));
	}

	private double getFraudIndex(PharmacyEventData eventData) {
		double fraudIndex = 0.0;
		for (MedicineData medicine : eventData.medicineDataList){
			double currentFraudIndex = getFraudIndex(medicine);
			fraudIndex = currentFraudIndex > fraudIndex ? currentFraudIndex : fraudIndex;
		}
		return fraudIndex;
	}

	private double getFraudIndex(MedicineData medicine) {
		return (medicine.filledRefills / getFillDateSpan(medicine.fillDate) )* medicine.csaSchedule;
	}

	private int getFillDateSpan(Date fillDate) {
		Date today =  new Date();
		return (int) ((today.getTime() - fillDate.getTime()) / 86400000);
	}

}
