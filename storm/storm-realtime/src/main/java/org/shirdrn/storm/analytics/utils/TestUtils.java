package org.shirdrn.storm.analytics.utils;

import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.shirdrn.storm.analytics.constants.StatFields;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

public class TestUtils {

	public static BaseRichSpout getTestSpout() {
		return new TestSpout();
	}
	
	public static class TestSpout extends BaseRichSpout {

		private static final long serialVersionUID = 1L;
		private static final Log LOG = LogFactory.getLog(TestSpout.class);
		private String[] events;
		int pointer = 0;
		SpoutOutputCollector collector;
		
		@SuppressWarnings("rawtypes")
		@Override
		public void open(Map conf, TopologyContext context,
				SpoutOutputCollector collector) {
			this.collector = collector;
			events = new String[] {
					// install
					"{\"event_code\":\"400000\",\"install_id\":\"000000HX\",\"udid\":\"ABCQQQQQQQQQQQQQQQQQQCBA\",\"event_time\":\"2015-03-03 05:51:12\",\"app_id\":\"1\",\"os_type\":0,\"version\":\"3.1.2\",\"channel\":\"A-Faster\"}",
					// open
					"{\"event_code\":\"100010\",\"install_id\":\"000000HX\",\"udid\":\"ABCQQQQQQQQQQQQQQQQQQCBA\",\"event_time\":\"2015-03-03 07:11:48\"}",
					"{\"event_code\":\"100010\",\"install_id\":\"000000HX\",\"udid\":\"ABCQQQQQQQQQQQQQQQQQQCBA\",\"event_time\":\"2015-03-03 08:45:09\"}",
					// play start
					"{\"event_code\":\"101013\",\"install_id\":\"000000HX\",\"udid\":\"ABCQQQQQQQQQQQQQQQQQQCBA\",\"event_time\":\"2015-03-03 08:00:15\"}",
					"{\"event_code\":\"101013\",\"install_id\":\"000000HX\",\"udid\":\"ABCQQQQQQQQQQQQQQQQQQCBA\",\"event_time\":\"2015-03-03 09:09:57\"}",
					"{\"event_code\":\"101013\",\"install_id\":\"000000HX\",\"udid\":\"ABCQQQQQQQQQQQQQQQQQQCBA\",\"event_time\":\"2015-03-03 09:10:07\"}",
					"{\"event_code\":\"101013\",\"install_id\":\"000000HX\",\"udid\":\"ABCQQQQQQQQQQQQQQQQQQCBA\",\"event_time\":\"2015-03-03 09:10:50\"}",
					"{\"event_code\":\"101013\",\"install_id\":\"000000HX\",\"udid\":\"ABCQQQQQQQQQQQQQQQQQQCBA\",\"event_time\":\"2015-03-03 09:09:57\"}",
					"{\"event_code\":\"101013\",\"install_id\":\"000000HX\",\"udid\":\"ABCQQQQQQQQQQQQQQQQQQCBA\",\"event_time\":\"2015-03-03 19:09:57\"}",
					// play end
					"{\"event_code\":\"101010\",\"install_id\":\"000000HX\",\"udid\":\"ABCQQQQQQQQQQQQQQQQQQCBA\",\"event_time\":\"2015-03-03 08:12:39\",\"duration\":\"189\"}",
					"{\"event_code\":\"101010\",\"install_id\":\"000000HX\",\"udid\":\"ABCQQQQQQQQQQQQQQQQQQCBA\",\"event_time\":\"2015-03-03 09:42:46\",\"duration\":\"381\"}",
					"{\"event_code\":\"101010\",\"install_id\":\"000000HX\",\"udid\":\"ABCQQQQQQQQQQQQQQQQQQCBA\",\"event_time\":\"2015-03-03 10:42:46\",\"duration\":\"99\"}",
					"{\"event_code\":\"101010\",\"install_id\":\"000000HX\",\"udid\":\"ABCQQQQQQQQQQQQQQQQQQCBA\",\"event_time\":\"2015-03-03 11:42:46\",\"duration\":\"147\"}",
					"{\"event_code\":\"101010\",\"install_id\":\"000000HX\",\"udid\":\"ABCQQQQQQQQQQQQQQQQQQCBA\",\"event_time\":\"2015-03-03 12:42:46\",\"duration\":\"1000\"}",
					"{\"event_code\":\"101010\",\"install_id\":\"000000HX\",\"udid\":\"ABCQQQQQQQQQQQQQQQQQQCBA\",\"event_time\":\"2015-03-03 13:42:46\",\"duration\":\"301\"}",
					"{\"event_code\":\"101010\",\"install_id\":\"000000HX\",\"udid\":\"ABCQQQQQQQQQQQQQQQQQQCBA\",\"event_time\":\"2015-03-03 14:25:13\",\"duration\":\"1022\"}"
			};
			
		}

		@Override
		public void nextTuple() {
			if(pointer < events.length) {
				String data = events[pointer];
				collector.emit(new Values(data));
				LOG.info("Spout emitted: " + data);
				++pointer;
				Utils.sleep(1);
			} else {
				Utils.sleep(5);
				pointer = 0;
			}
		}

		@Override
		public void declareOutputFields(OutputFieldsDeclarer declarer) {
			declarer.declare(new Fields(StatFields.EVENT_DATA));			
		}
		
	}
}
