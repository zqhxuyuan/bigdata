package edu.ucsd.cs.triton.window;

import storm.trident.TridentState;
import storm.trident.TridentTopology;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.builtin.Count;
import storm.trident.testing.FixedBatchSpout;
import storm.trident.testing.MemoryMapState;
import storm.trident.tuple.TridentTuple;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.LocalDRPC;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.generated.StormTopology;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import edu.ucsd.cs.triton.builtin.filter.PrintFilter;
import edu.ucsd.cs.triton.window.FixedLengthSlidingWindow;
import edu.ucsd.cs.triton.window.SlidingWindowUpdater;

public class TestEachFunction {
	
  public static class Split extends BaseFunction {
    @Override
    public void execute(TridentTuple tuple, TridentCollector collector) {
      String sentence = tuple.getString(0);
      for (String word : sentence.split(" ")) {
        collector.emit(new Values(word));
      }
    }
  }

  public static StormTopology buildTopology(LocalDRPC drpc) {
    FixedBatchSpout spout = new FixedBatchSpout(new Fields("sentence"), 1, new Values("the cow jumped over the moon"),
        new Values("the man went to the store and bought some candy"), new Values("four score and seven years ago"),
        new Values("how many apples can you eat"), new Values("to be or not to be the person"));
    spout.setCycle(true);
    
    TridentTopology topology = new TridentTopology();
    //TridentState wordCounts = topology
    topology.newStream("spout1", spout).parallelismHint(16)
    		//.each(new Fields("sentence"), new PreFilter())
    		//.each(new Fields("sentence"), new PrintFilter());
    		// fixed length sliding window
    		.partitionPersist(new FixedLengthSlidingWindow.Factory(3), new Fields("sentence"), new SlidingWindowUpdater(), new Fields("windowId", "sentence"))
		    .newValuesStream()
		    //.each(new Fields("sentence"), new PostFilter())
//		    .groupBy(new Fields("windowId"))
		    .each(new Fields("sentence"), new Split(), new Fields("word"))
		    .groupBy(new Fields("word", "windowId"))
		    //.each(new Fields("word", "windowId"), new PrintFilter());
    		.persistentAggregate(new MemoryMapState.Factory(), new Fields("word"), new Count(), new Fields("count")).parallelismHint(16);
    
    //topology.newDRPCStream("words", drpc).each(new Fields("args"), new Split(), new Fields("word")).groupBy(new Fields(
    //    "word")).stateQuery(wordCounts, new Fields("word"), new MapGet(), new Fields("count")).each(new Fields("count"),
    //    new FilterNull()).aggregate(new Fields("count"), new Sum(), new Fields("sum"));
    return topology.build();
  }
	
	public static void main(String[] args) throws InterruptedException, AlreadyAliveException, InvalidTopologyException {
		
    Config conf = new Config();
    conf.setMaxSpoutPending(20);
    if (args.length == 0) {
      LocalDRPC drpc = new LocalDRPC();
      LocalCluster cluster = new LocalCluster();
      cluster.submitTopology("testFixedLengthSlidingWindow", conf, buildTopology(drpc));
      /*for (int i = 0; i < 100; i++) {
        System.out.println("DRPC RESULT: " + drpc.execute("words", "cat the dog jumped"));
        Thread.sleep(1000);
      }*/
    }
    else {
      conf.setNumWorkers(3);
      StormSubmitter.submitTopology(args[0], conf, buildTopology(null));
    }
	}
}
