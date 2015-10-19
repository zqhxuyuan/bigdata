package edu.ucsd.cs.triton.window;

import java.util.Map;

import storm.trident.state.State;
import storm.trident.state.StateFactory;
import storm.trident.tuple.TridentTuple;
import backtype.storm.task.IMetricsContext;

public class TimeSlidingWindow extends BaseSlidingWindow {
	
	class TimeBasedRingBuffer extends RingBuffer<TridentTuple> {
		private long _duration;
		
		public TimeBasedRingBuffer(long duration) {
			this._duration = duration;
		}
		
		@Override
	  protected boolean removeEldestEntry(TridentTuple eldest) {
			return ((eldest.getLong(0) - System.currentTimeMillis()) > this._duration);
		}	
	}
	
	public TimeSlidingWindow(int duration) {
		_slidingWindow = new TimeBasedRingBuffer (duration);
	}

	public static class Factory implements StateFactory {

		private int _duration;
		
		public Factory(int duration) {
			this._duration = duration;
		}
		
		@Override
    public State makeState(Map conf, IMetricsContext metrics,
        int partitionIndex, int numPartitions) {
	    // TODO Auto-generated method stub
	    return new TimeSlidingWindow(_duration);
    }
	}
}
