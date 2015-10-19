package edu.ucsd.cs.triton.window;

import java.util.Map;

import storm.trident.state.State;
import storm.trident.state.StateFactory;
import storm.trident.tuple.TridentTuple;
import backtype.storm.task.IMetricsContext;

public class FixedLengthSlidingWindow extends BaseSlidingWindow {
	
	class CapacityBasedRingBuffer<T> extends RingBuffer<T> {
		private int _capacity;
		
		public CapacityBasedRingBuffer(final int capacity) {
			this._capacity = capacity;
		}
		
		@Override
	  protected boolean removeEldestEntry(T eldest) {
			return this.count() >= this._capacity;
		}	
	}
	
	public FixedLengthSlidingWindow(int windowSize) {
		_slidingWindow = new CapacityBasedRingBuffer<TridentTuple> (windowSize);
	}

	public static class Factory implements StateFactory {

		private int _windowSize;
		
		public Factory(final int windowSize) {
			this._windowSize = windowSize;
		}
		
		@Override
    public State makeState(Map conf, IMetricsContext metrics,
        int partitionIndex, int numPartitions) {
	    // TODO Auto-generated method stub
	    return new FixedLengthSlidingWindow(_windowSize);
    }
	}
}
