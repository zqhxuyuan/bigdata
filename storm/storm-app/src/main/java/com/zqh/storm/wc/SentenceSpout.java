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

public class SentenceSpout extends BaseRichSpout {

    //Spout的输出收集器, 在还没开始nextTuple之前的open方法里初始化collector
    //INPUT --> SPOUT --> SpoutOutputCollector --> collector.emit(..)
	private SpoutOutputCollector collector;

    //Spout发射出去的Tuple,进入待处理状态. Map的key是tuple的唯一MsgID,map的value是Values对象
    //当一个Tuple被Topology成功地执行完后,在调用ack方法时,将tuple从pending中移除
    //当Tuple执行失败,fail会被回调,为了让失败的消息重新被处理,从map中把msgID对应的tuple重新发送
	private ConcurrentHashMap<UUID, Values> pending;
	private int index = 0;

    private String[] sentences = {
            "my dog has fleas",
            "i like cold beverages",
            "the dog ate my homework",
            "don't have a cow man",
            "i don't think i like fleas" };

    @Override
    public void open(Map config, TopologyContext context, SpoutOutputCollector collector) {
        this.collector = collector;
        pending = new ConcurrentHashMap<UUID, Values>();
    }

    @Override
	public void nextTuple() {
        // Realibility in spouts
        // 调用一次nextTuple方法, 只处理数组中的一个元素
        Values values = new Values(sentences[index]);
        UUID msgId = UUID.randomUUID();
        // 待处理的列表, 通过spout发送出去的Tuple,只是进入待处理的状态
        this.pending.put(msgId, values);
        // 通过Spout由Collector负责发射tuple
        this.collector.emit(values, msgId);

        // this.collector.emit(new Values(sentences[index]));

        index++;
        //一轮发送完毕,再次从头开始发送
        if (index >= sentences.length) {
            index = 0;
        }
        try {
            Thread.sleep(1);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    @Override
	public void ack(Object msgId) {
		this.pending.remove(msgId);
	}

    @Override
	public void fail(Object msgId) {
		this.collector.emit(this.pending.get(msgId), msgId);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("sentence"));
	}

}
