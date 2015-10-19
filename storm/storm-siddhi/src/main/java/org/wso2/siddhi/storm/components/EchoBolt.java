package org.wso2.siddhi.storm.components;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;

import java.util.concurrent.atomic.AtomicInteger;

public class EchoBolt extends BaseBasicBolt {
    AtomicInteger count = new AtomicInteger();
    long lastTs = System.currentTimeMillis();

    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {
        int temp = count.incrementAndGet();
        //if(temp%1000 == 0){
        //System.out.println("[" + temp + " ]Result" + tuple.getSourceStreamId() + " " + tuple.getValues());
        //}
        if (temp % SiddhiBolt.MEESAGE_COUNT == 0) {
            System.out.println("[" + temp + "]Throughput=" + (SiddhiBolt.MEESAGE_COUNT * 1000 / (System.currentTimeMillis() - lastTs)));
            lastTs = System.currentTimeMillis();
        }

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
    }
}
