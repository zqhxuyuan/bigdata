package edu.ucsd.cs.triton.window;

import java.util.ArrayList;
import java.util.List;

import storm.trident.state.State;
import storm.trident.tuple.TridentTuple;

public abstract class BaseSlidingWindow implements State {
	
	protected RingBuffer<TridentTuple> _slidingWindow;
	
	@Override
	public void beginCommit(Long txid) {
		// TODO Auto-generated method stub
	}

	@Override
	public void commit(Long txid) {
		// TODO Auto-generated method stub
	}

	public void setLocationsBulk(List<TridentTuple> tuples) {
		for (TridentTuple tuple : tuples) {
			_slidingWindow.add(tuple);
		}
	}

	public List<TridentTuple> getWindowBulk() {
		List<TridentTuple> tuples = new ArrayList<TridentTuple> ();
		
		for (TridentTuple tuple : _slidingWindow) {
			tuples.add(tuple);
		}
		
		return tuples;
	}
}
