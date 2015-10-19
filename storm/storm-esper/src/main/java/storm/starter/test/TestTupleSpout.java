package storm.starter.test;


import backtype.storm.Config;
import backtype.storm.topology.OutputFieldsDeclarer;
import java.util.Map;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import java.util.HashMap;
import java.util.Random;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class TestTupleSpout extends BaseRichSpout {
    public static Logger LOG = LoggerFactory.getLogger(TestTupleSpout.class);
    boolean _isDistributed;
    SpoutOutputCollector _collector;
    private static String leftField;
    private static String rightField;

    public TestTupleSpout() {
        this(true);
    }
    public TestTupleSpout(String leftField, String rightField) {
        this(true);
        this.leftField = leftField;
        this.rightField = rightField;
    }

    public TestTupleSpout(boolean isDistributed) {
        _isDistributed = isDistributed;
    }

    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        _collector = collector;
    }

    public void close() {

    }

    public void nextTuple() {
        Utils.sleep(100);
        final String[] words = new String[] {
                "nathan", "mike", "jackson", "golda", "bertels"
                //,"hello","world","scala","clojure","storm","spark","hadoop"
        };
        final Random rand = new Random();
        final String leftValue = words[rand.nextInt(words.length)];
        final String rightValue = words[rand.nextInt(words.length)];
        _collector.emit(new Values(leftValue, rightValue));
    }

    public void ack(Object msgId) {

    }

    public void fail(Object msgId) {

    }

    //TODO:其实Fields不需要动态. 写死就可以了.
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields(leftField, rightField));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        if(!_isDistributed) {
            Map<String, Object> ret = new HashMap<String, Object>();
            ret.put(Config.TOPOLOGY_MAX_TASK_PARALLELISM, 1);
            return ret;
        } else {
            return null;
        }
    }
}