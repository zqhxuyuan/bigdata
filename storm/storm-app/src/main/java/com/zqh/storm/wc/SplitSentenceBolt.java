package com.zqh.storm.wc;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import java.util.Map;

public class SplitSentenceBolt extends BaseRichBolt {
	private OutputCollector collector;

    @Override
    public void prepare(Map config, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

	@Override
	public void execute(Tuple tuple) {
		String sentence = tuple.getStringByField("sentence");
        //tuple.getString(0)
		String[] words = sentence.split(" ");
		for (String word : words) {
			this.collector.emit(new Values(word));
            //collector.emit(tuple, new Values(word));
		}
		// Reliability in bolt:
		this.collector.ack(tuple);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("word"));
	}
}
