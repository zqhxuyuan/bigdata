package storm.starter.length.example_b;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import storm.starter.length.SlidingWindowBolt;

/**
 * This examples shows how to test whether a random set of numbers
 * is disjoint(不相交的) from each of the past 10 random set of numbers.
 * 测试一个随机集合是否和过去10个元素的每一个元素有相交/交集.
 * 
 * @author Malte Rohde <malte.rohde@inf.fu-berlin.de>
 */
public class SetIntersectionExampleTopology {
	
	public static void main(String[] args) throws InterruptedException {
		final int WINDOW_SIZE = 10;
		
		TopologyBuilder builder = new TopologyBuilder();
		
		builder.setSpout("random_set_spout", new RandomSetSpout(100, 1000, 100));
		
		builder.setBolt("sw", new SlidingWindowBolt(WINDOW_SIZE))
			.allGrouping("random_set_spout");
		
		for(int i = 0; i < WINDOW_SIZE; i++) {
			builder.setBolt("sw" + i, new SetIntersectionBolt(i))
				.allGrouping("sw");
		}
		
		Config conf = new Config();
        conf.setDebug(false);
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("rolling_sum", conf, builder.createTopology());
        Thread.sleep(10000);
        cluster.shutdown();
	}
	
}
