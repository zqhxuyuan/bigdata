package org.shirdrn.storm.api.common;

import java.util.concurrent.BlockingQueue;

import org.shirdrn.storm.api.TupleDispatcher;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

/**
 * Tuple dispatcher for spout component, and it's a asynchronous tuple distributor.
 * 
 * @author Yanjun
 *
 * @param <OUT> output data object
 */
public class SpoutTupleDispatcher<OUT> extends QueuedTupleDispatcher<Tuple, SpoutOutputCollector, OUT> {

	private static final long serialVersionUID = 1L;

	public SpoutTupleDispatcher(SpoutOutputCollector collector) {
		super(collector);
	}
	
	public SpoutTupleDispatcher(SpoutOutputCollector collector,  BlockingQueue<Tuple> queue) {
		super(collector);
	}

	@Override
	protected ProcessorRunner newProcessorRunner() {
		return new ProcessorRunnerImpl();
	}

	/**
	 * Control the behavior of the specified 
	 * {@link TupleDispatcher}.{@link Processor}.
	 * 
	 * @author yanjun
	 */
	private final class ProcessorRunnerImpl extends ProcessorRunner {
		
		private static final long serialVersionUID = 1L;

		public ProcessorRunnerImpl() {
			super();
		}
		
		@Override
		public void run() {
			while(true) {
				Tuple input = null;
				try {
					input = queue.take();
					OUT output = processor.process(input);
					Values values = processor.writeOut(output);
					if(values != null) {
						collector.emit(values);
					}
				} catch (Exception e) {
					if(input != null) {
						try {
							queue.put(input);
						} catch (InterruptedException e1) {
							e1.printStackTrace();
						}
					}
				}
			}
		}
		
	}
}
