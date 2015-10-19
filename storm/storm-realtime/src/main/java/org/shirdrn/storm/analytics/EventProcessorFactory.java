package org.shirdrn.storm.analytics;

import org.shirdrn.storm.api.ConnectionManager;
import org.shirdrn.storm.api.TupleDispatcher;
import org.shirdrn.storm.api.TupleDispatcher.Processor;
import org.shirdrn.storm.commons.utils.ReflectionUtils;

import redis.clients.jedis.Jedis;
import backtype.storm.task.OutputCollector;
import backtype.storm.tuple.Tuple;

public class EventProcessorFactory implements TupleDispatcher.ProcessorFactory<Tuple, OutputCollector, Void> {

	private static final long serialVersionUID = 1L;
	private final ConnectionManager<Jedis> connectionManager;
	
	public EventProcessorFactory(final ConnectionManager<Jedis> connectionManager) {
		this.connectionManager = connectionManager;
	}

	@SuppressWarnings("unchecked")
	@Override
	public Processor<Tuple, OutputCollector, Void> createProcessor(Class<? extends Processor<Tuple, OutputCollector, Void>> clazz) {
		return ReflectionUtils.newInstance(clazz, Processor.class, this);
	}

	public ConnectionManager<Jedis> getConnectionManager() {
		return connectionManager;
	}
}
