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
 *
 * Spout发射tuple给两个Bolt. 每个Bolt都输出两个stream-id.
 * 最后的PrintBolt的同一个stream-id会来自于两个不同的Bolt发射的数据
 *
 * setAllBolt示例输出:
 Bolt>>myBolt2;Stream>>stream2;Tuple>>jackson
 Bolt>>myBolt1;Stream>>stream2;Tuple>>jackson
 Bolt>>myBolt2;Stream>>stream1;Tuple>>bertels
 Bolt>>myBolt1;Stream>>stream1;Tuple>>bertels

 * PrintBolt的一个stream-id只接收一个MyBolt时,比如stream1来自MyBolt2, stream2来自MyBolt1:
 *
 * setSingleBolt示例输出:
 Bolt>>myBolt2;Stream>>stream1;Tuple>>bertels
 Bolt>>myBolt2;Stream>>stream1;Tuple>>golda
 Bolt>>myBolt2;Stream>>stream1;Tuple>>golda
 Bolt>>myBolt2;Stream>>stream1;Tuple>>bertels
 Bolt>>myBolt1;Stream>>stream2;Tuple>>nathan
 Bolt>>myBolt1;Stream>>stream2;Tuple>>nathan
 Bolt>>myBolt2;Stream>>stream1;Tuple>>golda
 */
public class MergeStream {
    public static final String TOPOLOGY_NAME = "Test";

    public static void main(String[] args) throws InterruptedException {
        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("word", new TestWordSpout());
        //两个Bolt实际上是一样的. 只不过boltName不同,即component-id不同
        builder.setBolt("myBolt1", new MyBolt()).shuffleGrouping("word");
        builder.setBolt("myBolt2", new MyBolt()).shuffleGrouping("word");

        //setAllBolt(builder);
        setSingleBolt(builder);

        Config conf = new Config();
        conf.setDebug(false);
        conf.put(Config.TOPOLOGY_MAX_SPOUT_PENDING, 1);
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology(TOPOLOGY_NAME, conf, builder.createTopology());

        Thread.sleep(5000);
        cluster.killTopology(TOPOLOGY_NAME);
        cluster.shutdown();
    }

    //相同的stream-id有两个bolt
    public static void setAllBolt(TopologyBuilder builder){
        builder.setBolt("stream1", new PrintBolt())
                .localOrShuffleGrouping("myBolt1", "stream1")
                .localOrShuffleGrouping("myBolt2", "stream1");

        builder.setBolt("stream2", new PrintBolt())
                .localOrShuffleGrouping("myBolt1", "stream2")
                .localOrShuffleGrouping("myBolt2", "stream2");
    }

    //每个PrintBolt只用一个stream的一个上游Bolt.
    public static void setSingleBolt(TopologyBuilder builder){
        builder.setBolt("stream1", new PrintBolt())
                .localOrShuffleGrouping("myBolt2", "stream1");

        builder.setBolt("stream2", new PrintBolt())
                .localOrShuffleGrouping("myBolt1", "stream2");
    }
}

