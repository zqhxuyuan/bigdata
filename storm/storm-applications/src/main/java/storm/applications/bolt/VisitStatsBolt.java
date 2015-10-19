package storm.applications.bolt;

import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import static storm.applications.constants.ClickAnalyticsConstants.*;

/**
 * User: domenicosolazzo
 */
public class VisitStatsBolt extends AbstractBolt {
    private int total = 0;
    private int uniqueCount = 0;

    @Override
    public void initialize() {
    }

    //访问状态
    @Override
    public void execute(Tuple input) {
        boolean unique = Boolean.parseBoolean(input.getStringByField(Field.UNIQUE));
        total++;    //pv:page view
        if(unique) uniqueCount++; //uv:unique view,user view
        
        collector.emit(input, new Values(total, uniqueCount));
        collector.ack(input);
    }

    @Override
    public Fields getDefaultFields() {
        return new Fields(Field.TOTAL_COUNT, Field.TOTAL_UNIQUE);
    }
}
