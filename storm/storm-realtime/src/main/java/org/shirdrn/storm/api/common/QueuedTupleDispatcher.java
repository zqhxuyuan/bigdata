package org.shirdrn.storm.api.common;

import java.io.Serializable;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.shirdrn.storm.api.TupleDispatcher;
import org.shirdrn.storm.commons.utils.ThreadPoolUtils;

import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import com.google.common.base.Preconditions;

/**
 * Asynchronous {@link TupleDispatcher} based on a {@link BlockingQueue} caching
 * mechanism.
 * 
 * @author Yanjun
 *
 * @param <IN> input tuple object, usually {@link Tuple} data
 * @param <COLLECTOR> collector object
 * @param <OUT> output data object
 */
public abstract class QueuedTupleDispatcher<IN, COLLECTOR, OUT> extends GenericTupleDispatcher<IN, COLLECTOR, OUT> {
	
	private static final long serialVersionUID = 1L;
	private static final Log LOG = LogFactory.getLog(QueuedTupleDispatcher.class);
	private ExecutorService executorService;
	protected final BlockingQueue<IN> queue;

	public QueuedTupleDispatcher(COLLECTOR collector) {
		super(collector);
		this.queue = new LinkedBlockingQueue<IN>();
	}
	
	public QueuedTupleDispatcher(COLLECTOR collector, BlockingQueue<IN> queue) {
		super(collector);
		this.queue = queue;
	}
	
	@Override
	public void dispatch(IN input) throws InterruptedException {
		queue.put(input);
	}
	
	@Override
	public void start() {
		Preconditions.checkArgument(processorFactory != null, "Never set a processor factory for the dispatcher!");
		Preconditions.checkArgument(processorFactory != null, "Never set a processor class for the dispatcher!");
		executorService = ThreadPoolUtils.newCachedThreadPool("DISPATCHER");
		for (int i = 0; i < parallelism; i++) {
			ProcessorRunner runner = newProcessorRunner();
			executorService.execute(runner);
			LOG.info("Processor runner started: " + runner);
		}
	}
	
	@Override
	public void stop() {
		executorService.shutdown();
	}
	
	/**
	 * Implements and creates a thread to process tuples.
	 * @return
	 */
	protected abstract ProcessorRunner newProcessorRunner();
	
	abstract class ProcessorRunner extends Thread implements Serializable {
		
		private static final long serialVersionUID = 1L;
		private final Log log = LogFactory.getLog(QueuedTupleDispatcher.class);
		protected final Processor<IN, COLLECTOR, OUT> processor;
		
		public ProcessorRunner() {
			processor = processorFactory.createProcessor(processorClass);
			log.info("Processor created: " + processor);
		}
		
	}
	
	enum Op {
		EMIT(1),
		ACK(2),
		FAIL(3);
		
		private int code;
		
		private Op(int code) {
			this.code = code;
		}
		
		public int getCode() {
			return code;
		}
	}
	
	protected class Event {
		
		private final Op op;
		private final IN input;
		private Values values;
		
		public Event(Op op, IN input) {
			super();
			this.op = op;
			this.input = input;
		}
		
		public Event(Op op, IN input, Values values) {
			super();
			this.op = op;
			this.input = input;
			this.values = values;
		}
		
		public Op getOp() {
			return op;
		}
		public IN getInput() {
			return input;
		}
		public Values getValues() {
			return values;
		}
		public void setValues(Values values) {
			this.values = values;
		}
	}
	
}
