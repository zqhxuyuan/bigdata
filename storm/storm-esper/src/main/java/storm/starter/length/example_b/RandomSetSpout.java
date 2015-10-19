package storm.starter.length.example_b;
import java.util.HashSet;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

/**
 * Simple spouts that creates random sets of numbers of equal
 * cardinality and sleeps in between messages.
 * 
 * @author Malte Rohde <malte.rohde@inf.fu-berlin.de>
 */
public class RandomSetSpout extends BaseRichSpout {
	private final int n;
    private final int range;
    private final int delay_ms;
	private final Random rand;
       
	private SpoutOutputCollector collector;
    
    
    public RandomSetSpout(int n, int range, int delay_ms) {   	
    	this.n = n;
    	this.range = range;    
    	this.delay_ms = delay_ms;
		this.rand = new Random(System.currentTimeMillis());
    }
	
	@Override
	public void open(Map conf, TopologyContext context,
			SpoutOutputCollector collector) {
		this.collector = collector;
	}

	@Override
	public void nextTuple() {
		Set<Integer> value = new HashSet<Integer>();
		for(int i = 0; i < n; i++) {
			int num;
			do {
				num = rand.nextInt(range);
			} while(value.contains(num));
			value.add(num);
		}
		collector.emit(new Values(value));
		Utils.sleep(delay_ms);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("set"));
	}

}
