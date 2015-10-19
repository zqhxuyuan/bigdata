package org.shirdrn.storm.analytics;

import net.sf.json.JSONObject;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.shirdrn.storm.analytics.common.KeyedResult;
import org.shirdrn.storm.analytics.common.StatResult;
import org.shirdrn.storm.api.CallbackHandler;
import org.shirdrn.storm.api.Result;
import org.shirdrn.storm.api.TupleDispatcher;
import org.shirdrn.storm.api.TupleDispatcher.ProcessorFactory;
import org.shirdrn.storm.api.TupleDispatcher.Processor;
import org.shirdrn.storm.commons.constants.StatIndicators;

import redis.clients.jedis.Jedis;
import backtype.storm.task.OutputCollector;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

/**
 * Process actually time-consuming logic. If you set <code>parallelism</code> by
 * invoking {@link TupleDispatcher#setProcessorFactoryWithParallelism(org.shirdrn.storm.api.TupleDispatcher.Processor, int)},
 * after a {@link TupleDispatcher} instance created. The tuple dispatcher will create multiple {@link TupleDispatcher.Processor}
 * to process in parallel.
 * 
 * @author Yanjun
 */
public class EventProcessor implements Processor<Tuple, OutputCollector, Void> {

	private static final long serialVersionUID = 1L;
	private static final Log LOG = LogFactory.getLog(EventProcessor.class);
	private final ProcessorFactory<Tuple, OutputCollector, Void> factory;
	private final EventProcessorFactory eventProcessorFactory;
	
	public EventProcessor(final ProcessorFactory<Tuple, OutputCollector, Void> factory) {
		super();
		this.factory = factory;
		eventProcessorFactory = (EventProcessorFactory) this.factory;
	}
	
	@Override
	public Void process(Tuple input) throws Exception {
		int indicator = input.getInteger(0);
		Result obj = (Result) input.getValue(1);
		LOG.debug("INPUT: indicator=" + indicator + ", obj=" + obj);
		consume(input, indicator, obj);	
		return null;
	}
	
	@SuppressWarnings("unchecked")
	private void consume(Tuple input, int indicator, Result obj) throws Exception {
		switch(indicator) {
			case StatIndicators.OPEN_AU:
			case StatIndicators.OPEN_NU:
			case StatIndicators.PLAY_AU:
			case StatIndicators.PLAY_NU:
			case StatIndicators.OPEN_TIMES:
			case StatIndicators.PLAY_TIMES:
			case StatIndicators.PLAY_NU_DURATION:
			case StatIndicators.PLAY_AU_DURATION:
				StatResult statResult = (StatResult) obj;
				// <key, field, value> like: 
				// <2015011520::11::S, 0::A-Baidu::3.1.0, 43997>
				// Explanations: 
				// 		hour->2015011520, NU->11, os type->0, channel->A-CCIX, version->3.1.0, 
				// 		statistical type->S, counter->43997
				String key = statResult.getStrHour();
				String field = statResult.toField();
				invoke(input, key, field, statResult.toString(), statResult);
				break;
				
			case StatIndicators.USER_DEVICE_INFO:
			case StatIndicators.USER_DYNAMIC_INFO:
				KeyedResult<JSONObject> result = (KeyedResult<JSONObject>) obj;
				key = result.getKey();
				JSONObject value = result.getData();
				// user device information:
				// <key, value> like: 
				// key  -> us::9d11f3ee0242a15026e51d1b3efba454
				// value-> {"aid": "0", "dt":"0", "ch":"A-CCIX", "v":"1.2.7"}
				
				// user dynamic information:
				// <key, value> like:
				// key  -> ud::9d11f3ee0242a15026e51d1b3efba454
				// field-> fod  fpd
				// value-> 2015-01-15
				invoke(input, key, null, value == null ? "" : value.toString(), result);
				break;
				
		}
	}
	
	private void invoke(Tuple input, String key, String field, String value, Result result) throws Exception {
		CallbackHandler<Jedis> callbackHandler = result.getCallbackHandler();
		if(callbackHandler != null) {
			Jedis connection = null;
			try {
				connection = eventProcessorFactory.getConnectionManager().getConnection();
				callbackHandler.callback(connection);
			} catch (Exception e) {
				LOG.error("Fail to update value for: " + "key=" + key + ", field=" + field + ", value=" + value, e);
				throw e;
			} finally {
				connection.close();
			}
		}
	}

	@Override
	public Values writeOut(Void out) {
		// TODO Auto-generated method stub
		return null;
	}
	
}
