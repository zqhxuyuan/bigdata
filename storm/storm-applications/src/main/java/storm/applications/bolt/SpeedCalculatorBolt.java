package storm.applications.bolt;

import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import storm.applications.constants.TrafficMonitoringConstants.Field;
import storm.applications.model.gis.Road;

/**
 * Copyright 2013 Xdata@SIAT
 * email: gh.chen@siat.ac.cn 
 */
public class SpeedCalculatorBolt extends AbstractBolt {
    private Map<Integer, Road> roads;

    @Override
    public void initialize() {
        roads = new HashMap<>();
    }
    
    @Override
    public void execute(Tuple input) {
        int roadID = input.getIntegerByField(Field.ROAD_ID);
        int speed  = input.getIntegerByField(Field.SPEED);
        
        int averageSpeed = 0;
        int count = 0;

        //这条路上还没有一辆taxi时,说明这辆taxi是第一个开进这条路的
        if (!roads.containsKey(roadID)) {
            Road road = new Road(roadID);
            road.addRoadSpeed(speed);
            road.setCount(1);
            road.setAverageSpeed(speed);
            
            roads.put(roadID, road);
            averageSpeed = speed;
            count = 1;
        } else {
            //越来越多的taxi开进来了
            Road road = roads.get(roadID);
            int sum = 0;

            //第二辆车进来, 计算方式和else部分不一样
            if (road.getRoadSpeedSize() < 2) { //=1的时候,只有第一辆车的时候才满足这种情况
                road.incrementCount();
                road.addRoadSpeed(speed); //现在=2了
                
                for (int it : road.getRoadSpeed()) {
                    sum += it;  //两辆车,每辆的速度相加, 除于2, 就是平均速度了
                }
                
                averageSpeed = (int)((double)sum/(double)road.getRoadSpeedSize());
                road.setAverageSpeed(averageSpeed);
                count = road.getRoadSpeedSize();
            } else {
                double avgLast = roads.get(roadID).getAverageSpeed();

                double temp = 0;
                for (int it : road.getRoadSpeed()) {
                    sum += it;
                    temp += Math.pow((it-avgLast), 2);
                }
                
                int avgCurrent = (int) ((sum + speed)/((double)road.getRoadSpeedSize() + 1));
                temp = (temp + Math.pow((speed - avgLast), 2)) / (road.getRoadSpeedSize());
                double stdDev = Math.sqrt(temp);
                
                if (Math.abs(speed - avgCurrent) <= (2 * stdDev)) {
                    road.incrementCount();
                    road.addRoadSpeed(speed);
                    road.setAverageSpeed(avgCurrent);
                    
                    averageSpeed = avgCurrent;
                    count = road.getRoadSpeedSize();
                }
            }
        }
        
        collector.emit(input, new Values(new Date(), roadID, averageSpeed, count));
        collector.ack(input);
    }

    @Override
    public Fields getDefaultFields() {
        return new Fields(Field.NOW_DATE, Field.ROAD_ID, Field.AVG_SPEED, Field.COUNT);
    }
}
