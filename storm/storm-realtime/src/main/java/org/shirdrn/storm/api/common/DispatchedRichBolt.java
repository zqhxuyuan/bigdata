package org.shirdrn.storm.api.common;

import java.util.Map;

import org.shirdrn.storm.api.TupleDispatcher;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.base.BaseRichBolt;

public abstract class DispatchedRichBolt<IN, OUT> extends BaseRichBolt {

	private static final long serialVersionUID = 1L;
	protected OutputCollector collector;
	protected TupleDispatcher<IN, OutputCollector, OUT> dispatcher;
	
	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		this.collector = collector;
	}

}
