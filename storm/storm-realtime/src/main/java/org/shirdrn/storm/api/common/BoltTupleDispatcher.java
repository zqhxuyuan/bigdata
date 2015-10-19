package org.shirdrn.storm.api.common;

import java.io.Serializable;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;

import org.shirdrn.storm.api.TupleDispatcher;

import backtype.storm.task.OutputCollector;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

/**
 * Tuple dispatcher for bolt component, and it's a asynchronous tuple distributor.
 * 
 * @author Yanjun
 *
 * @param <OUT> output data object
 */
public class BoltTupleDispatcher<OUT> extends QueuedTupleDispatcher<Tuple, OutputCollector, OUT> {

	private static final long serialVersionUID = 1L;
	private final CollectedWorker worker;

	public BoltTupleDispatcher(OutputCollector collector) {
		super(collector);
		worker = startCollectedWorker();
	}
	
	public BoltTupleDispatcher(OutputCollector collector,  BlockingQueue<Tuple> queue) {
		super(collector, queue);
		worker = startCollectedWorker();
	}

	@Override
	protected ProcessorRunner newProcessorRunner() {
		return new ProcessorRunnerImpl();
	}
	
	private CollectedWorker startCollectedWorker() {
		final CollectedWorker worker = new CollectedWorker();
		worker.start();
		return worker;
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
					if(input != null) {
						OUT output = processor.process(input);
						Values values = processor.writeOut(output);
						if(values != null) {
							worker.addLast(new Event(Op.EMIT, input, values));
						}
						if(isDoAckManaged()) {
							worker.addLast(new Event(Op.ACK, input));
						}
					}
				} catch (Exception e) {
					if(isDoAckFailureManaged()) {
						try {
							worker.addLast(new Event(Op.FAIL, input));
						} catch (InterruptedException e1) {
							e1.printStackTrace();
						}
					}
				}
			}
		}
		
	}
	
	protected final class CollectedWorker extends Thread implements Serializable {
		
		private static final long serialVersionUID = 1L;
		private final BlockingDeque<Event> q;
		
		public CollectedWorker() {
			q = new LinkedBlockingDeque<Event>();
		}
		
		@Override
		public void run() {
			while(true) {
				try {
					Event event = q.pollFirst();
					if(event != null) {
						Tuple input = event.getInput();
						int code = event.getOp().getCode();
						switch(code) {
							case 1:
								collector.emit(input, event.getValues());
								break;
							case 2:
								collector.ack(input);
								break;
							case 3:
								collector.fail(input);
								break;
							default:
									
						}
					} else {
						Thread.sleep(50);
					}
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		}
		
		public void addLast(Event event) throws InterruptedException {
			q.putLast(event);
		}
	}
	
}
