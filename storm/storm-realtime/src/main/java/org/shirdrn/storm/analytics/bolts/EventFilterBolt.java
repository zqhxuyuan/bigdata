package org.shirdrn.storm.analytics.bolts;

import java.util.Collection;
import java.util.Map;

import net.sf.json.JSONObject;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.shirdrn.storm.analytics.constants.EventFields;
import org.shirdrn.storm.analytics.constants.StatFields;
import org.shirdrn.storm.analytics.utils.RealtimeUtils;
import org.shirdrn.storm.api.EventHandlerManager;
import org.shirdrn.storm.api.Result;

import redis.clients.jedis.Jedis;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

/**
 * Distribute interested coming events.
 * 
 * @author Yanjun
 */
public class EventFilterBolt extends BaseRichBolt {

	private static final long serialVersionUID = 1L;
	private static final Log LOG = LogFactory.getLog(EventFilterBolt.class);
	private OutputCollector collector;
	private EventHandlerManager<Collection<Result>, Jedis, JSONObject> eventHandlerManager;
	
	@SuppressWarnings({ "rawtypes" })
	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		this.collector = collector;
		eventHandlerManager = RealtimeUtils.getEventHandlerManager();
	}
	
	@Override
	public void execute(Tuple input) {
		String event = input.getString(0);
		LOG.debug("INPUT: event=" + event);
		JSONObject jo = null;
		try {
			jo = JSONObject.fromObject(event);
			String eventCode = jo.getString(EventFields.EVENT_CODE);
			boolean interested = eventHandlerManager.isInterestedEvent(eventCode);
			if(interested) {
				collector.emit(new Values(event));
				LOG.debug("Emitted: event=" + event);
			}
		} catch (Exception e) {
			LOG.warn("Illegal JSON format data: " + event);
		} finally {
			collector.ack(input);
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields(StatFields.EVENT_DATA));
	}

}
