package storm.starter.length.example_b;
import java.lang.reflect.Field;
import java.util.HashSet;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import storm.starter.length.SlidingWindowParticipantBolt;

class SetIntersectionBolt extends SlidingWindowParticipantBolt {

	HashSet<Integer> window_data;
	
	public SetIntersectionBolt(int participant_id) {
		super(participant_id);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("intersection"));
	}

	@SuppressWarnings("unchecked")
	@Override
	protected void process(Tuple input, BasicOutputCollector collector) {
        //在没有调用update前, window_data为null. 什么时候调用update: 属于自己这个参与者接收到第一个tuple就有数据了.
		if(window_data != null) {
            //接收到的第二个tuple以后都会调用这里,因为有了第一个的window_data!=null.
			HashSet<Integer> data = (HashSet<Integer>) input.getValue(0);
			
			// Calculate intersection. 计算交集
			
			HashSet<Integer> smaller, larger;
			if(data.size() < window_data.size()) {
				smaller = data;
				larger = window_data;
			} else {
				smaller = window_data;
				larger = data;
			}
			
			HashSet<Integer> intersection = new HashSet<Integer>();
			for(Integer i : smaller) {
                //对小集合的每个元素, 判断是否在大的集合中? 如果存在,则加入交集中
				if(larger.contains(i))
					intersection.add(i);
			}
			
			if(!intersection.isEmpty()) {
				System.out.println("Non-empty intersection found: " + intersection);
				collector.emit(new Values(intersection));
			}
		}
	}

	@SuppressWarnings("unchecked")
	@Override
	protected void update(Tuple input, BasicOutputCollector collector) {
		window_data = (HashSet<Integer>) input.getValue(0);		
	}

}
