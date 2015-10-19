package org.shirdrn.storm.analytics.bolts;

import java.util.Map;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.shirdrn.storm.analytics.EventProcessor;
import org.shirdrn.storm.analytics.EventProcessorFactory;
import org.shirdrn.storm.analytics.JedisConnectionManager;
import org.shirdrn.storm.analytics.constants.Constants;
import org.shirdrn.storm.analytics.utils.RealtimeUtils;
import org.shirdrn.storm.api.ConnectionManager;
import org.shirdrn.storm.api.common.BoltTupleDispatcher;
import org.shirdrn.storm.api.common.DispatchedRichBolt;

import redis.clients.jedis.Jedis;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;

public class EventStatResultPersistBolt extends DispatchedRichBolt<Tuple, Void> {

	private static final long serialVersionUID = 1L;
	private static final Log LOG = LogFactory.getLog(EventStatResultPersistBolt.class);
	private transient ConnectionManager<Jedis> connectionManager;
	
	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		super.prepare(stormConf, context, collector);
		
		Configuration conf = RealtimeUtils.getDefaultConfiguration();
		// configure tuple dispatcher
		dispatcher = new BoltTupleDispatcher<Void>(collector);
		int parallelism = 1;
		try {
			parallelism = conf.getInt(Constants.REALTIME_DISPATCHED_PROCESSOR_PARALLELISM, parallelism);
		} catch (Exception e) { }
		LOG.info("Configure: parallelism=" + parallelism);
		
		connectionManager = JedisConnectionManager.newInstance();
		LOG.info("Connection manager started!");
		
		dispatcher.setProcessorFactory(new EventProcessorFactory(connectionManager));
		dispatcher.setProcessorClass(EventProcessor.class);
		dispatcher.setProcessorParallelism(parallelism);
		dispatcher.start();
		LOG.info("Tuple dispatcher started!");
	}
	
	@Override
	public void execute(Tuple input) {
		try {
			dispatcher.dispatch(input);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub

	}
	
}
