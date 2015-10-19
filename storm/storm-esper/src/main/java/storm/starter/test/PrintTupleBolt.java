package storm.starter.test;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

public class PrintTupleBolt extends BaseBasicBolt {

    private boolean detail = false;

    public PrintTupleBolt(boolean detail){
        this.detail = detail;
    }

    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        try {
            String left = input.getString(0);
            Long count = input.getLong(1);
            String right = "";
            if(detail)
                right = input.getString(2);
            if (left != null && right != null)
                System.out.println(System.currentTimeMillis()+"-Bolt>>"+input.getSourceComponent()+";Stream>>"+input.getSourceStreamId() + ";Tuple>>" + left + ">>" + count + ":" + right);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("print"));
    }

}

