package storm.starter.length.example_a;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

/**
 * The Collector class will print the total sum of the last 20 elements
 * every time a partial sum has been issued by any participant.
 */
class CollectSumsBolt extends BaseBasicBolt {
	final private int[] partial_sums;
	
	CollectSumsBolt(int participants) {
		this.partial_sums = new int[participants];
	}
	
	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {
        //每个参与者都会发送自己的编号, 以及自己范围内的和, 发送给CollectSumsBolt进行统一汇总处理.
		Integer id = input.getInteger(0);
		Integer s = input.getInteger(1);
		partial_sums[id] = s;
		
		int sum = 0;
		for(int x : partial_sums) sum += x;
		
		System.out.println("Current sum: " + sum);
		collector.emit(new Values(sum));
	}
	
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("rolling_sum"));			
	}
}