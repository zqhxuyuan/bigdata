package storm.starter.length.example_a;

import java.util.Map;
import java.util.Random;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

/**
 * Example random number message generator.
 */
class RandomNumberSpout extends BaseRichSpout {
	final private int range;
	final private int delay_ms;
	final private Random rand;
	private SpoutOutputCollector collector;
	
	RandomNumberSpout(int range, int delay_ms) {
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
		collector.emit(new Values(new Integer(rand.nextInt(range))));
		Utils.sleep(delay_ms);
	}
	
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("number"));
	}
}
