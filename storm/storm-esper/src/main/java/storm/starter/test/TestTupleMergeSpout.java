package storm.starter.test;


import backtype.storm.Config;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;


public class TestTupleMergeSpout extends BaseRichSpout {
    public static Logger LOG = LoggerFactory.getLogger(TestTupleMergeSpout.class);
    boolean _isDistributed;
    SpoutOutputCollector _collector;
    String splitKey = "::";

    public TestTupleMergeSpout() {
        this(true);
    }

    public TestTupleMergeSpout(boolean isDistributed) {
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
                "nathan", "mike", "jackson", "golda", "bertels",
                "hello","world","scala","clojure","storm","spark","hadoop"
        };
        final Random rand = new Random();
        final String word = words[rand.nextInt(words.length)];
        final String secondWord = words[rand.nextInt(words.length)];

        //_collector.emit(new Values(word, secondWord));
        _collector.emit(new Values(word+splitKey+secondWord));
    }

    public void ack(Object msgId) {

    }

    public void fail(Object msgId) {

    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        //declarer.declare(new Fields("word","word2"));
        declarer.declare(new Fields("word"));
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