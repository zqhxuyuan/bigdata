package com.zqh.storm.getstart;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class WordCounter extends BaseBasicBolt {

	Integer id;
	String name;
	Map<String, Integer> counters;		// 存储单词的统计结果

	// At the end of the spout (when the cluster is shutdown We will show the word counters
	public void cleanup() {
		System.out.println("-- Word Counter ["+name+"-"+id+"] --");
		/*
		for(Map.Entry<String, Integer> entry : counters.entrySet()){
			System.out.println(entry.getKey()+": "+entry.getValue());
		}
		*/
		Object[] key = counters.keySet().toArray();   
        Arrays.sort(key);  
        for(int i = 0; i < key.length; i++) {   
            System.out.println(key[i] + " : " + counters.get(key[i]));   
        }   
	}

	// On create
	public void prepare(Map stormConf, TopologyContext context) {
		this.counters = new HashMap<String, Integer>();
		this.name = context.getThisComponentId();
		this.id = context.getThisTaskId();
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {}

	// 单词统计
	public void execute(Tuple input, BasicOutputCollector collector) {
		String str = input.getString(0);
		// If the word dosn't exist in the map we will create this, if not We will add 1
		if(!counters.containsKey(str)){
			counters.put(str, 1);
		}else{
			Integer c = counters.get(str) + 1;
			counters.put(str, c);
		}
	}
}
