package storm.starter.bolt;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import net.sf.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.starter.model.Compute;
import storm.starter.model.Dimension;
import storm.starter.model.TDMetric;
import storm.starter.test.MockMetrics;
import storm.starter.util.WindowConstant;

import java.util.*;

public class JSONBolt extends BaseBasicBolt {
    Logger logger =  LoggerFactory.getLogger(JSONBolt.class);

    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        try {
            String left = input.getString(0);
            if (left == null) return;
            JSONObject json = JSONObject.fromObject(left);

            //TODO: 从MySQL中获取合作方对应的指标. NOW WE HAVE ONE EVENT AND IT'S ALL METRICS.TIME TO COMPUTE THE METRICS.
            String partner = json.getString(Dimension.partnerCode);
            List<TDMetric> metricList = MockMetrics.getMetricsCache(partner);

            if(metricList!=null){
                for(TDMetric metric : metricList) {
                    //collector.emit(metric.getCompute().name(), new Values(json, metric));

                    String masterValue = json.getString(metric.getMasterField());
                    int timeUnit = metric.getTimeUnit();

                    //参与指标计算的一般只有主维度, 从维度, 而传递整个json字符串会有点大. 何不把主维度,从维度的值直接设置到Metric中. 然后只传一个Metric对象即可.
                    metric.setMasterValue(masterValue);
                    if(metric.getSlaveField() != null){
                        if(json.getString(metric.getSlaveField()) != null){
                            metric.setSlaveValue(json.getString(metric.getSlaveField()));
                        }
                    }
                    //TODO: 将CountBolt中的key提前到这里完成. 只发送必要的数据到CountBolt,而不要发送整个Metric对象.
                    collector.emit(metric.getCompute().name(), new Values(masterValue, timeUnit, metric));
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        //declarer.declare(new Fields("json", "metrics"));
        for(Compute compute : Compute.values()){
            //declarer.declareStream(compute.name(), new Fields("json", "metric"));
            declarer.declareStream(compute.name(), new Fields(WindowConstant.masterKey, WindowConstant.timeUnitKey, "metric"));
        }
    }


}


