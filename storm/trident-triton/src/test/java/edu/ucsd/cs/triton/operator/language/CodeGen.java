package edu.ucsd.cs.triton.operator.language;

import storm.trident.Stream;
import storm.trident.TridentTopology;
import storm.trident.operation.builtin.Count;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.generated.StormTopology;
import backtype.storm.tuple.Fields;
import edu.ucsd.cs.triton.builtin.aggregator.Max;
import edu.ucsd.cs.triton.builtin.filter.PrintFilter;
import edu.ucsd.cs.triton.twitter.TwitterBatchSpout;
import edu.ucsd.cs.triton.window.FixedLengthSlidingWindow;
import edu.ucsd.cs.triton.window.SlidingWindowUpdater;

public class CodeGen {
  
  /*
   * select count(*) as tps, max(retweetCount) as maxRetweets from Tweets.win:time_batch(1 sec)
   */
  
  public static StormTopology buildTopology() {
  	TwitterBatchSpout spout = new TwitterBatchSpout();
    
    TridentTopology topology = new TridentTopology();
    Stream s1 = topology.newStream("s1", spout) 
    		.partitionPersist(new FixedLengthSlidingWindow.Factory(3), spout.getOutputFields(), new SlidingWindowUpdater(), new Fields("windowId", "createdAt", "retweetCount"))
		    .newValuesStream() // win:time_batch(1 sec)
      	.each(new Fields("windowId", "createdAt", "retweetCount"), new PrintFilter())
      	.groupBy(new Fields("windowId")) // no group by, so only groupby windowId
      	.chainedAgg()
      	.aggregate(new Count(), new Fields("tps")) // count(*) as tps
      	.aggregate(new Fields("retweetCount"), new Max(), new Fields("maxRetweets")) // max(retweetCount) as maxRetweets
      	.chainEnd()
      	.each(new Fields("tps", "maxRetweets"), new PrintFilter()); // output
		    //.partitionPersist(stateFactory, updater)
    
    return topology.build();
  }
	
	public static void main(String[] args) throws InterruptedException, AlreadyAliveException, InvalidTopologyException {
		
    Config conf = new Config();
    
    conf.setMaxSpoutPending(20);
    
    if (args.length == 0) {
      LocalCluster cluster = new LocalCluster();
      cluster.submitTopology("twitter_demo", conf, buildTopology());
    }
    else {
      conf.setNumWorkers(3);
      StormSubmitter.submitTopology(args[0], conf, buildTopology());
    }
	}
}
