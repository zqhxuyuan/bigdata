package storm.starter.length.example_a;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.BoltDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.StormSubmitter;
import storm.starter.length.SlidingWindowBolt;

/**
 * This example demonstrates how to use SlidingWindowBolt and its friends
 * by simply calculating the rolling sum of the last 20 numbers in a 计算过去20个元素的和
 * endless series of random integers. To achieve this, we would like to
 * calculate the sum of four patches(4个小片) of 5 elements each in parallel and
 * afterwards add up the partial sums.
 * 由于要计算滚动的20个元素求和, 将20个分成4个小片, 每个小片只有5个元素.
 * 通过并行地对每个小片计算, 最后将每个小片的局部求和相加即为20个元素的总和.
 * 
 * @author Malte Rohde <malte.rohde@inf.fu-berlin.de>
 */
public class RollingSumExampleTopology {
	
	public static void main(String[] args) throws Exception {
		final int PARTICIPANTS = 4;  //可以认为是并行度
        final int LENGTH_WINDOW = 20;  //窗口的大小为固定的20个元素.和时间窗口的固定时间不同,这里是固定元素个数
		final int PARTICIPANTS_SIZE = LENGTH_WINDOW / PARTICIPANTS;  //每个参与者需要处理多少个数据
		// => sliding window size = 20

        final String _slidingWindow = "sw";
        final String _roolingSum = "swp";
		
		TopologyBuilder builder = new TopologyBuilder();
		
		builder.setSpout("random_number_spout", new RandomNumberSpout(1000, 100));
		
		// We tell the SlidingWindowBolt to deliver messages to four (!) participants.
		// We cannot set parallelism here. 发送给4个参与者??
		builder.setBolt(_slidingWindow, new SlidingWindowBolt(PARTICIPANTS))
			.allGrouping("random_number_spout");
		
		// Create the Collector first, so we do not have to repeat the loop.
		BoltDeclarer bd = builder.setBolt("collector", new CollectSumsBolt(PARTICIPANTS));

		for(int i = 0; i < PARTICIPANTS; i++) {
			// Create a RollingSumBolt with 5 elements.
			// No parallelism here, either.
            // 创建对应数量的参与者, SIZE表示每个参与者需要处理的数据量=总的窗口长度/参与者数量
			builder.setBolt(_roolingSum + i, new RollingSumBolt(i, PARTICIPANTS_SIZE))
                    //采用allGrouping的策略, SlidingWindowBolt的每个tuple都会发送给所有的参与者RollingSumBolt
                    //但是每个tuple是分散给每个RollingSumBolt处理的, 因此在SlidingWindowParticipantBolt中要进行判断,进行分流.
                    .allGrouping(_slidingWindow);
			
			// Connect the collector. 所有的参与者都要将自己计算的部分和,发送给CollectSumsBolt
			bd.allGrouping(_roolingSum + i);
		}
		
		Config conf = new Config();
	        conf.setDebug(false);
       		if(args.length > 0) {
			conf.setNumWorkers(8);
			StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
		} else {
			LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("rolling_sum", conf, builder.createTopology());
            Thread.sleep(10000);
            cluster.shutdown();
		}
	}
	
}
