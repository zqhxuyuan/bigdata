package edu.ucsd.cs.triton.twitter;

import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.builtin.Count;
import storm.trident.tuple.TridentTuple;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import edu.ucsd.cs.triton.builtin.aggregator.Max;
import edu.ucsd.cs.triton.builtin.filter.PrintFilter;
import edu.ucsd.cs.triton.builtin.spout.TwitterSpout;
import edu.ucsd.cs.triton.codegen.SimpleQuery;
import edu.ucsd.cs.triton.window.FixedLengthSlidingWindow;
import edu.ucsd.cs.triton.window.SlidingWindowUpdater;

/*
 * select count(*) as tps, max(retweetCount) as maxRetweets from Tweets.win:time_batch(1 sec)
 */
public class TwitterDemo extends SimpleQuery {
	
  public static class MyFunction extends BaseFunction {
    @Override
    public void execute(TridentTuple tuple, TridentCollector collector) {
    	collector.emit(new Values(tuple.getInteger(0) * 2, tuple.getString(1) + "!!", tuple.getInteger(2) + 2));
    }
  }
  
  /*public static class S1PreWindowFilter implements Filter  {
  	@Override
  	public void prepare(Map conf, TridentOperationContext context) {
  	}
  	@Override
  	public void cleanup() {
  	}

  	@Override
  	public boolean isKeep(TridentTuple tuple) {
  		return tuple.getInteger(0) == 3;
  	}
  }*/
	
	@Override
	public void buildQuery() {
		
		TwitterSpout _spout = new TwitterSpout();

    _topology.newStream("s1", _spout) 
		.partitionPersist(new FixedLengthSlidingWindow.Factory(3), _spout.getOutputFields(), new SlidingWindowUpdater(), new Fields("windowId", "createdAt", "retweetCount"))
    .newValuesStream() // win:time_batch(1 sec)
  	.each(new Fields("windowId", "createdAt", "retweetCount"), new PrintFilter())
  	.groupBy(new Fields("windowId")) // no group by, so only groupby windowId
  	.chainedAgg()
  	.aggregate(new Count(), new Fields("tps")) // count(*) as tps
  	.aggregate(new Fields("retweetCount"), new Max(), new Fields("maxRetweets")) // max(retweetCount) as maxRetweets
  	.chainEnd()
  	.each(new Fields("tps", "maxRetweets"), new PrintFilter()); // output
    //.partitionPersist(stateFactory, updater)
	}
  
	public static void main(String[] args) {
		TwitterDemo demo = new TwitterDemo();
    demo.execute(args);
	}
}
