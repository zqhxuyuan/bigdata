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

public class TestFixedLengthWindow {
	
  public static class Split extends BaseFunction {
    @Override
    public void execute(TridentTuple tuple, TridentCollector collector) {
      String sentence = tuple.getString(0);
      for (String word : sentence.split(" ")) {
        collector.emit(new Values(word));
      }
    }
  }

  public static class MyFunction extends BaseFunction {
    @Override
    public void execute(TridentTuple tuple, TridentCollector collector) {
    	collector.emit(new Values(tuple.getInteger(0) * 2, tuple.getString(1) + "!!", tuple.getInteger(2) + 2));
    }
  }
  
  
  public static StormTopology buildTopology(LocalDRPC drpc) {
    FixedBatchSpout spout = new FixedBatchSpout(new Fields("a", "b", "c", "d"), 3, new Values(1, "a", 1, "b"), new Values(2, "c", 2, "d"), new Values(3, "e", 3, "f"));
    spout.setCycle(true);
    
    TridentTopology topology = new TridentTopology();
    //TridentState wordCounts = topology
    topology.newStream("spout1", spout).parallelismHint(16)
    	.each(new Fields("a", "b", "c", "d"), new MyFunction(), new Fields("b1", "c1", "d1"))
    	.each(new Fields("b1", "c1", "d1"), new PrintFilter());
    		// fixed length sliding window
    		
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
