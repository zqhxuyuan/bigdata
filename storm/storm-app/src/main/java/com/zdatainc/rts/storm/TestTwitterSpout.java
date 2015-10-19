package com.zdatainc.rts.storm;


import backtype.storm.Config;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import com.alibaba.fastjson.JSON;
import com.zdatainc.rts.model.NegativeWords;
import com.zdatainc.rts.model.PositiveWords;
import com.zdatainc.rts.model.Twitter;
import net.sf.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;


public class TestTwitterSpout extends BaseRichSpout {
    public static Logger LOG = LoggerFactory.getLogger(TestTwitterSpout.class);
    boolean _isDistributed;
    SpoutOutputCollector _collector;
    Set<String> positiveWords = new HashSet<>();
    Set<String> negativeWords = new HashSet<>();
    int postSize = 0;
    int negaSize = 0;
    int twitterId = 0;

    public TestTwitterSpout() {
        this(true);
    }

    public TestTwitterSpout(boolean isDistributed) {
        _isDistributed = isDistributed;
    }

    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        _collector = collector;
        positiveWords = PositiveWords.getWords();
        negativeWords = NegativeWords.getWords();
        postSize = positiveWords.size();
        negaSize = negativeWords.size();
    }

    public void close() {

    }

    public void nextTuple() {
        Utils.sleep(100);
        final Random rand = new Random();
        StringBuffer sb = new StringBuffer();
        int post = rand.nextInt(20);
        for(int i=0;i<post;i++){
            sb.append(positiveWords.toArray()[rand.nextInt(postSize)]).append(" ");
        }
        for(int i=0;i<20-post;i++){
            sb.append(negativeWords.toArray()[rand.nextInt(negaSize)]).append(" ");
        }
        Twitter twitter = new Twitter();
        twitter.setId(twitterId++);
        twitter.setLang("en");
        twitter.setText(sb.toString());

        String json = JSON.toJSONString(twitter);

        _collector.emit(new Values(json));
    }

    public void ack(Object msgId) {

    }

    public void fail(Object msgId) {

    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("json"));
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