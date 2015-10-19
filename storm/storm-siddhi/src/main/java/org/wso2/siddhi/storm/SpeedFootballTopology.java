package org.wso2.siddhi.storm;

import org.wso2.siddhi.storm.components.EchoBolt;
import org.wso2.siddhi.storm.components.FootballDataSpout;
import org.wso2.siddhi.storm.components.SiddhiBolt;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;

public class SpeedFootballTopology {

    private static SiddhiBolt configureSiddhiBolt1() {
        SiddhiBolt siddhiBolt = new SiddhiBolt(
                new String[]{"define stream PlayStream1 ( sid string, ts long, x double, y double, z double, a double, v double);"},
                new String[]{"from PlayStream1#window.timeBatch(1sec) select sid, avg(v) as avgV insert into AvgRunPlay;"},
                new String[]{"AvgRunPlay"});
        return siddhiBolt;
    }

    private static SiddhiBolt configureSiddhiBolt2() {
        SiddhiBolt siddhiBolt = new SiddhiBolt(
                new String[]{"define stream AvgRunPlay ( sid string, v double);"},
                new String[]{"from AvgRunPlay[v>20] select sid, v as v insert into FastRunPlay;"},
                new String[]{"FastRunPlay"});
        return siddhiBolt;
    }

    public static void main(String[] args) throws Exception {
        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("PlayStream1", new FootballDataSpout(), 1);
        builder.setBolt("AvgRunPlay", configureSiddhiBolt1(), 1).shuffleGrouping("PlayStream1");
        builder.setBolt("FastRunPlay", configureSiddhiBolt2(), 1).shuffleGrouping("AvgRunPlay");
        builder.setBolt("LeafEcho", new EchoBolt(), 1).shuffleGrouping("FastRunPlay");

        Config conf = new Config();
        //conf.setDebug(true);

        if (args != null && args.length > 0) {
            conf.setNumWorkers(3);

            StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
        } else {
            conf.setMaxTaskParallelism(3);

            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("word-count", conf, builder.createTopology());

            Thread.sleep(10000);
            cluster.shutdown();
        }
    }
}