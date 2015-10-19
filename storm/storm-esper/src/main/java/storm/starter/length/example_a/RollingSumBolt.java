package storm.starter.length.example_a;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import storm.starter.length.ArraySlidingWindowParticipantBolt;

/**
 * A SlidingWindowParticipant that calculates the sum of its share of numbers.
 */
class RollingSumBolt extends ArraySlidingWindowParticipantBolt {
	public RollingSumBolt(int participant_id, int array_size) {
		super(participant_id, array_size);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("participant_id", "sum"));			
	}

	@Override
	protected void process(Tuple input, BasicOutputCollector collector) {
		// We do not care about all messages.
	}
	
	@Override
	protected void updated(BasicOutputCollector collector) {
		// But when we've been updated, we want to emit the sum.
		int sum = 0;
        //窗口内的所有元素, 比如每个参与者要处理5个元素, 则window_data为5个tuple. 为什么是Tuple,因为SlidingWindowBolt把Tuple input原封不动地传下来.
		for(Tuple t : window_data) {
			// window_data elements can initially be null.
			if(t == null) continue;

            //虽然SlidingWindowBolt的输出是data字段, 但是我们这里取的是number. 实际上上面的for循环取出的就是data对应的Tuple了.
			sum += t.getIntegerByField("number");
		}
        //当前第几个participant编号的参与者, 它计算属于自己范围内的和. 最后所有参与者的和相加即为20个元素的总和(CollectSumsBolt).
		collector.emit(new Values(new Integer(participant_id), new Integer(sum)));
	}
}
