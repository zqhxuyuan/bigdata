package com.zqh.storm.grouping;

/**
 * Created by zhengqh on 15/9/8.
 */

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import java.util.Map;

public class MyBolt extends BaseRichBolt {
    private OutputCollector collector;

    @Override
    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void execute(Tuple input) {
        String word = input.getString(0);
        //小于j的word发送给stream1; 大于j的word发送给stream2;
        //每收到一个Tuple,只可能发送给stream1,stream2中的其中一个. 也就是说每次Bolt根据收到的value发射给不同的stream.
        if(word.compareTo("j") < 0){
            collector.emit("stream1", new Values(word));
        }else if(word.compareTo("j") > 0){
            collector.emit("stream2", new Values(word));
        }
        //不管什么都发送给stream3
        collector.emit("stream3", new Values(word));
    }

    //定义了多个输出流. 但是用的是同一个字段. 因为相同的字段,但是值可能不同. 根据这个字段的值发送给不同的Bolt
    //在execute中向不同的stream-id发射不同的word, 这里就要定义对应的stream-id和Fields的关系.
    @Override
    public void declareOutputFields(final OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declareStream("stream1", new Fields("field1"));
        outputFieldsDeclarer.declareStream("stream2", new Fields("field1"));
        outputFieldsDeclarer.declareStream("stream3", new Fields("field1"));
    }
}
