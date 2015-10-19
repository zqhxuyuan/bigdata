package edu.ucsd.cs.triton.window;

import java.util.List;
import java.util.UUID;

import backtype.storm.tuple.Values;

import storm.trident.operation.TridentCollector;
import storm.trident.state.BaseStateUpdater;
import storm.trident.tuple.TridentTuple;

public class SlidingWindowUpdater extends BaseStateUpdater<BaseSlidingWindow> {

	@Override
  public void updateState(BaseSlidingWindow state, List<TridentTuple> tuples,
      TridentCollector collector) {
	  // TODO Auto-generated method stub
	  state.setLocationsBulk(tuples);
	  
	  String windowId = UUID.randomUUID().toString();
	  
	  List<TridentTuple> window = state.getWindowBulk();
	  for (TridentTuple tuple : window) {
	  	//tuple.add(windowId);
	  	Values value = new Values(windowId);
	  	value.addAll(tuple.getValues());
		  collector.emit(value);
	  }
  }
}
