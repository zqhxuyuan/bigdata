package com.zqh.storm.wc;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

public class SentenceSpout1 extends BaseRichSpout {

    //Spout的输出收集器, 在还没开始nextTuple之前的open方法里初始化collector
    //INPUT --> SPOUT --> SpoutOutputCollector --> collector.emit(..)
	private SpoutOutputCollector collector;
	private int index = 0;

//    private String[] sentences = {
//            "my dog has fleas",
//            "i like cold beverages",
//            "the dog ate my homework",
//            "don't have a cow man",
//            "i don't think i like fleas" };
    private String[] sentences = {
            "Hello World","Hello Storm"
    };

    @Override
    public void open(Map config, TopologyContext context, SpoutOutputCollector collector) {
        this.collector = collector;
    }

    @Override
	public void nextTuple() {
        // only emits each sentence once
        if(index < sentences.length){
            this.collector.emit(new Values(sentences[index]));
            index ++;
        }
	}

    @Override
	public void ack(Object msgId) {
	}

    @Override
	public void fail(Object msgId) {
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("sentence"));
	}

}
