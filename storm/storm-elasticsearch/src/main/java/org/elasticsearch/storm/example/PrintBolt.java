package org.elasticsearch.storm.example;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;

import java.util.List;

public class PrintBolt extends BaseBasicBolt {

    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        try {
            List<Object> list = input.getValues();
            System.out.print("PRINT>>"+System.currentTimeMillis()/1000 + "-" + input.getSourceComponent() + ";Tuple>>");
            for(Object o : list) {
                System.out.print(o + "\t\t");
            }
            System.out.println();
            /*
            String left = input.getString(0);
            if (left != null)
                System.out.println("Bolt>>"+input.getSourceComponent()+";Stream>>"+input.getSourceStreamId() + ";Tuple>>" + left.replace("\n",""));
            */
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        //declarer.declare(new Fields("print"));
    }

}

