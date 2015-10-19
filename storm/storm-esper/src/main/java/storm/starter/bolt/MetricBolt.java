package storm.starter.bolt;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import net.sf.json.JSONObject;
import storm.starter.model.Compute;
import storm.starter.model.TDMetric;

import java.util.List;

/**
 * Created by zhengqh on 15/9/22.
 */
public class MetricBolt extends BaseBasicBolt {

    public void execute(Tuple input, BasicOutputCollector collector) {
        JSONObject json = (JSONObject)input.getValue(0);
        TDMetric metric = (TDMetric)input.getValue(1);

        int windowLength = metric.getTimeUnit();

        System.out.println(metric.getCompute().name());
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        //declarer.declare(new Fields("json", "metrics"));
    }

}
