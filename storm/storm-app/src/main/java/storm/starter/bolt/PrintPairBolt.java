package storm.starter.bolt;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;

/**
 * Created by zqhxuyuan on 15-5-24.
 */
public class PrintPairBolt extends BaseBasicBolt {

    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {
        String first = tuple.getString(0);
        int second = tuple.getInteger(1);
        System.out.println(first + "," + second);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer ofd) {
    }

}
