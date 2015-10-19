package storm.applications.model.gis;

import storm.applications.util.collections.FixedSizeQueue;

/**
 * 道路
 */
public class Road {
    private final int roadID;   //道路编号
    private final FixedSizeQueue<Integer> roadSpeed;  //有限队列
    private int averageSpeed;   //平均速度
    private int count;          //车的数量. 这个跟队列有什么区别??

    public Road(int roadID) {
        this.roadID = roadID;
        this.roadSpeed = new FixedSizeQueue<>(30);
    }

    public int getRoadID() {
        return roadID;
    }

    public int getAverageSpeed() {
        return averageSpeed;
    }

    public void setAverageSpeed(int averageSpeed) {
        this.averageSpeed = averageSpeed;
    }

    public int getCount() {
        return count;
    }

    public void setCount(int count) {
        this.count = count;
    }
    
    public void incrementCount() {
        this.count++;
    }

    public FixedSizeQueue<Integer> getRoadSpeed() {
        return roadSpeed;
    }

    public boolean addRoadSpeed(int speed) {
        return roadSpeed.add(speed);
    }
    
    public int getRoadSpeedSize() {
        return roadSpeed.size();
    }
}
