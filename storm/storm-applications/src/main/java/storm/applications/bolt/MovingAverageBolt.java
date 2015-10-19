package storm.applications.bolt;

import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.applications.constants.SpikeDetectionConstants.Conf;
import storm.applications.constants.SpikeDetectionConstants.Field;

/**
 * Calculates the average over a window for distinct elements.
 * http://github.com/surajwaghulde/storm-example-projects
 * 
 * @author surajwaghulde
 */
public class MovingAverageBolt extends AbstractBolt {
    private static final Logger LOG = LoggerFactory.getLogger(MovingAverageBolt.class);
    
    private int movingAverageWindow;
    private Map<String, LinkedList<Double>> deviceIDtoStreamMap;
    private Map<String, Double> deviceIDtoSumOfEvents;

    @Override
    public void initialize() {
        movingAverageWindow   = config.getInt(Conf.MOVING_AVERAGE_WINDOW, 1000);
        deviceIDtoStreamMap   = new HashMap<>();
        deviceIDtoSumOfEvents = new HashMap<>();
    }

    @Override
    public void execute(Tuple input) {
        String deviceID = input.getStringByField(Field.DEVICE_ID);
        //因为一次execute调用只能处理一条记录. 针对同一个设备,要计算平均值,每次处理的都是这个设备在某个时刻的温度值
        double nextDouble = input.getDoubleByField(Field.VALUE);
        //要计算温度的平均值,需要使用一个Map来保存之前的数据.
        double movingAvergeInstant = movingAverage(deviceID, nextDouble);

        //问题是每条记录每次都要发送吗??
        collector.emit(input, new Values(deviceID, movingAvergeInstant, nextDouble));
        collector.ack(input);
    }

    public double movingAverage(String deviceID, double nextDouble) {
        LinkedList<Double> valueList = new LinkedList<>();
        double sum = 0.0;
        
        if (deviceIDtoStreamMap.containsKey(deviceID)) {
            valueList = deviceIDtoStreamMap.get(deviceID);
            sum = deviceIDtoSumOfEvents.get(deviceID);
            if (valueList.size() > movingAverageWindow-1) {
                double valueToRemove = valueList.removeFirst();			
                sum -= valueToRemove;
            }
            valueList.addLast(nextDouble);
            sum += nextDouble;
            deviceIDtoSumOfEvents.put(deviceID, sum);
            deviceIDtoStreamMap.put(deviceID, valueList);
            return sum/valueList.size();
        } else {
            valueList.add(nextDouble);
            deviceIDtoStreamMap.put(deviceID, valueList);
            deviceIDtoSumOfEvents.put(deviceID, nextDouble);
            return nextDouble;
        }
    }

    @Override
    public Fields getDefaultFields() {
        return new Fields(Field.DEVICE_ID, Field.MOVING_AVG, Field.VALUE);
    }
}