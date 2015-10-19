package storm.starter.tools;

import java.util.LinkedList;
import java.util.Queue;

/**
 * http://rosettacode.org/wiki/Averages/Simple_moving_average#Java
 *
 * 基于大小的窗口, 求平均值
 */
public class MovingAverage {
    private final Queue<Double> window = new LinkedList<Double>();
    private final int period;
    private double sum;

    public MovingAverage(int period) {
        assert period > 0 : "Period must be a positive integer";
        this.period = period;
    }

    public void newNum(double num) {
        sum += num;
        window.add(num);
        if (window.size() > period) {
            sum -= window.remove();
        }
    }

    public double getAvg() {
        if (window.isEmpty()) return 0; // technically the average is undefined
        return sum / window.size();
    }

    public static void main(String[] args) {
        //测试数据
        double[] testData = {1,2,3,4,5,5,4,3,2,1};
        //测试了两个不同大小的长度窗口
        int[] windowSizes = {3,5};
        for (int windSize : windowSizes) {
            MovingAverage ma = new MovingAverage(windSize);
            for (double x : testData) {
                //循环测试数据,每收到一个数据,就添加到MV的固定大小队列中
                ma.newNum(x);
                System.out.println("Next number = " + x + ", SMA = " + ma.getAvg());
            }
            System.out.println();
        }
    }
}