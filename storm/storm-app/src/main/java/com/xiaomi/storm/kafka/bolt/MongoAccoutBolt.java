/*
 * authored by lixiao1@xiaomi.com
 * feature account by key/value every 30s to emit in one Mongo record
 * Date:20130224 
 */

package com.xiaomi.storm.kafka.bolt;

import java.math.BigDecimal;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import backtype.storm.Config;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import backtype.storm.topology.base.BaseBasicBolt;

import com.xiaomi.storm.kafka.common.BaseCounter;
import com.xiaomi.storm.kafka.common.TupleHelpers;
import com.mongodb.BasicDBObjectBuilder;
import com.mongodb.DBObject; 
import com.mongodb.util.JSON;

import org.json.simple.JSONObject;

public class MongoAccoutBolt<T> extends BaseBasicBolt {
	
	/**
	 * Accouting bolt using key/value format
	 */
	private static final long serialVersionUID = 1L;
	
	public MongoAccoutBolt(String lostYear, long emitFrequencyInSeconds, String logKey, String logValue) {
		this.emitFrequencyInSeconds = emitFrequencyInSeconds;
		this.logKey = logKey;
		this.logValue = logValue;
	}
	
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		declarer.declare(new Fields("records"));
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		Map<String, Object> conf = new HashMap<String, Object>();
		conf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, emitFrequencyInSeconds);
		return conf;
	}

	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map stormConf, TopologyContext context) {
		// TODO Auto-generated method stub
		objCounter = new BaseCounter<Object>();
	}

	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {
		this._collector = collector;
		if (TupleHelpers.isTickTuple(input)) {
			emitCurrentCounts();
		} else {
			countObjAndAck(input);
		}
	}

	private void countObjAndAck(Tuple input) {		
		String jsonText = input.getStringByField("bytes");
		
		/*
		 * Official MongoDB Java Driver comes with utility methods for parsing JSON to BSON and serializing BSON to JSON.
		 * import com.mongodb.DBObject;
		 * import com.mongodb.util.JSON;
		 * DBObject dbObj = ... ;
		 * String json = JSON.serialize( dbObj );
		 * DBObject bson = ( DBObject ) JSON.parse( json ); 
		 */
			
		if(null != jsonText) {
			DBObject bson = (DBObject)JSON.parse((jsonText));
			String recordValue = null;
			Double counterValue = null;
			String recordKey = null;
			try {
				recordValue = bson.get(this.logValue).toString();
				counterValue = Double.valueOf(recordValue);
				recordKey = bson.get(this.logKey).toString();
			} catch (NullPointerException e) {
				recordValue = null;
				counterValue = 0.0d;
				recordKey = "platform";
				e.printStackTrace();
			} catch (NumberFormatException e) {
                recordValue = null;
                counterValue = 0.0d;
                recordKey = "platform";
                e.printStackTrace();
            }
			objCounter.incrementCount(recordKey, counterValue);
			//this._collector.ack(input);
		}
	}

	private void emitCurrentCounts() {
		// TODO Auto-generated method stub
		try {
			Map<Object, Double> counts = objCounter.getCounts();
			emit(counts);
		} catch (NullPointerException e) {
			e.printStackTrace();
		}
		objCounter.wipeObjects();
	}
	
	@SuppressWarnings("all")
	private synchronized void emit(Map<Object, Double> counts) {
		
		records = new JSONObject();
		for(Entry<Object, Double> entry : counts.entrySet()) {
			Object obj = entry.getKey();
			Double countValue = entry.getValue();
			double sumValue = countValue.doubleValue();
			long arraySize = objCounter.computeObjectSize(obj);
			double avgResponseTime = sumValue / arraySize;
			Long pvObj = Long.valueOf(arraySize);
			BigDecimal b = new BigDecimal(Double.valueOf(avgResponseTime).doubleValue());
			Double arTimeObj =  Double
					.valueOf(b.setScale(2, BigDecimal.ROUND_HALF_UP)
					.doubleValue());
			/*
			 * records should like this 
			 *{401:{status_code_count:123,avg_request_time:456},503:{status_code_count:123,avg_request_time:456},
			 *...,200:{status_code_count:XXX,avg_request_time:XXX}}
			*/
			innerRecords = new JSONObject();
			innerRecords.put("status_code_count", pvObj);
			innerRecords.put("avg_request_time", arTimeObj);
			records.put(obj.toString(), innerRecords);
			//innerRecords = null;
		}
		synchronized(this) {
			this._collector.emit(new Values(records.toString()));
		}
		records.clear(); 
	}

	@Override
	public void cleanup() {
		// TODO Auto-generated method stub
	}
	
	public DBObject getDBObjectForInput(Tuple input) {
		BasicDBObjectBuilder dbObjectBuilder = new BasicDBObjectBuilder();
		for (String field : input.getFields()) {
			Object value = input.getValueByField(field);
			//?serialization error
			//System.out.println("######################## field value:" + value.toString());
			dbObjectBuilder.append(field, value);
			if (isValidDBObjectField(value)) {
				dbObjectBuilder.append(field, value);
			}
		}
		return dbObjectBuilder.get();
	}
	
	private boolean isValidDBObjectField(Object value) {
		return value instanceof String
				|| value instanceof Date
				|| value instanceof Integer
				|| value instanceof Float
				|| value instanceof Double
				|| value instanceof Short
				|| value instanceof Long
				|| value instanceof DBObject;
	}
	
	private String logKey;
	private String logValue;
	private long emitFrequencyInSeconds;
	private BaseCounter<Object> objCounter;
	private BasicOutputCollector _collector;
	private JSONObject records;
	private JSONObject innerRecords;

}
