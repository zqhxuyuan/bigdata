package org.shirdrn.storm.api.common;

import org.shirdrn.storm.api.TupleDispatcher;

import backtype.storm.tuple.Tuple;

/**
 * Generic tuple dispatcher abstraction.
 * 
 * @author Yanjun
 *
 * @param <IN> input tuple object, usually {@link Tuple} data
 * @param <COLLECTOR> collector object
 * @param <OUT> output data object
 */
public abstract class GenericTupleDispatcher<IN, COLLECTOR, OUT> implements TupleDispatcher<IN, COLLECTOR, OUT> {

	private static final long serialVersionUID = 1L;
	protected final COLLECTOR collector;
	protected ProcessorFactory<IN, COLLECTOR, OUT> processorFactory;
	protected Class<? extends Processor<IN, COLLECTOR, OUT>> processorClass;
	protected int parallelism = 1;
	private boolean doAckManaged = true;
	private boolean doAckFailureManaged = true;

	public GenericTupleDispatcher(COLLECTOR collector) {
		super();
		this.collector = collector;
	}
	
	@Override
	public void setProcessorFactory(ProcessorFactory<IN, COLLECTOR, OUT> processorFactory) {
		this.processorFactory = processorFactory;
	}
	
	@Override
	public void setProcessorParallelism(int parallelism) {
		this.parallelism = parallelism;		
	}
	
	@Override
	public void setProcessorClass(
			Class<? extends org.shirdrn.storm.api.TupleDispatcher.Processor<IN, COLLECTOR, OUT>> processorClass) {
		this.processorClass = processorClass;	
	}

	@Override
	public void setDoAckManaged(boolean doAckManaged) {
		this.doAckManaged = doAckManaged;	
	}

	@Override
	public boolean isDoAckManaged() {
		return doAckManaged;
	}

	@Override
	public void setDoAckFailureManaged(boolean doAckFailureManaged) {
		this.doAckFailureManaged = doAckFailureManaged;	
	}

	@Override
	public boolean isDoAckFailureManaged() {
		return doAckFailureManaged;
	}
	
	
	
}
