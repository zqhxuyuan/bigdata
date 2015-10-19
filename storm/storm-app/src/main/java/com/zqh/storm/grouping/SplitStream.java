package com.zqh.storm.grouping;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.testing.TestWordSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import storm.meta.bolt.PrintBolt;

import java.util.Map;

/**
 * Created by zhengqh on 15/9/8.
 * http://stackoverflow.com/questions/19807395/how-would-i-split-a-stream-in-apache-storm
 *
 * 输出结果示例:
 stream2>>Tuple：mike
 stream1>>Tuple：golda       小于j
 stream2>>Tuple：jackson
 stream2>>Tuple：jackson
 stream1>>Tuple：bertels     小于j
 stream2>>Tuple：jackson
 stream1>>Tuple：golda       小于j
 stream1>>Tuple：bertels     小于j
 stream1>>Tuple：bertels     小于j
 stream2>>Tuple：nathan
 stream2>>Tuple：mike
 stream2>>Tuple：nathan

 * 添加了stream3之后, 每个word都会发送给stream3
 stream2>>Tuple：jackson
 stream3>>Tuple：jackson
 stream1>>Tuple：golda
 stream3>>Tuple：golda
 stream2>>Tuple：nathan
 stream3>>Tuple：nathan
 */
public class SplitStream {

    public static final String TOPOLOGY_NAME = "Test";

    public static void main(String[] args) throws InterruptedException {
        // Topology definition
        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("word", new TestWordSpout());
        builder.setBolt("boltWithStreams", new MyBolt()).shuffleGrouping("word");

        /*
        builder.setBolt("myBolt1", new PrintBolt()).shuffleGrouping("boltWithStreams", "stream1");
        builder.setBolt("myBolt2", new PrintBolt()).shuffleGrouping("boltWithStreams", "stream2");
        builder.setBolt("myBolt3", new PrintBolt()).shuffleGrouping("boltWithStreams", "stream3");
        */

        //等价于上面的方式, 这样更加简洁, 结果是一样的.
        //PrintBolt的上一个Bolt是MyBolt(因为boltWithStreams是MyBolt的component-id)
        //PrintBolt的分组策略, 除了MyBolt的component-id=boltWithStreams,还有stream-id.
        builder.setBolt("printBolt", new PrintBolt())
                //注意分组策略的compoent-id和stream-id都是上一个Bolt即MyBolt的.
                .localOrShuffleGrouping("boltWithStreams", "stream1")
                .localOrShuffleGrouping("boltWithStreams", "stream2")
                .localOrShuffleGrouping("boltWithStreams", "stream3");

        //如果去掉某个stream,说明MyBolt的下一个Bolt没有接收对应的stream,则这个stream的数据都不会被接收到, 尽管在MyBolt中发射了数据.

        // Configuration
        Config conf = new Config();
        conf.setDebug(false);
        conf.put(Config.TOPOLOGY_MAX_SPOUT_PENDING, 1);
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology(TOPOLOGY_NAME, conf, builder.createTopology());

        Thread.sleep(5000);
        cluster.killTopology(TOPOLOGY_NAME);
        cluster.shutdown();
    }
}