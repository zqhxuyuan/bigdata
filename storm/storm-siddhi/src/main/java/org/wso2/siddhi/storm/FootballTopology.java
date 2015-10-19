package org.wso2.siddhi.storm;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.wso2.siddhi.storm.components.EchoBolt;
import org.wso2.siddhi.storm.components.SiddhiBolt;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

public class FootballTopology {

    public static class PlayStream extends BaseRichSpout {
        private BufferedReader reader;
        SpoutOutputCollector _collector;
        AtomicInteger counter = new AtomicInteger();

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declareStream("PlayStream1", new Fields("sid", "ts", "x", "y", "z"));
        }

        @Override
        public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
            this._collector = collector;
            try {
                String fileName = "small-game4";
                reader = new BufferedReader(new FileReader(fileName), 10 * 1024 * 1024);
            } catch (FileNotFoundException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public void nextTuple() {
            try {
                if (counter.get() < 100000) {
                    String line = reader.readLine();
                    while (line != null) {
                        String[] dataStr = line.split(",");
                        //System.out.println(tempCout + " "+ count); tempCout++;

                        //sid, ts (pico second 10^-12), x (mm), y(mm), z(mm), v (um/s 10^(-6)), a (us^-2), vx, vy, vz, ax, ay, az

                        double v_kmh = Double.valueOf(dataStr[5]) * 60 * 60 / 1000000000;
                        double a_ms = Double.valueOf(dataStr[6]) / 1000000;

                        long time = Long.valueOf(dataStr[1]);

                        if ((time >= 10753295594424116l && time <= 12557295594424116l) || (time >= 13086639146403495l && time <= 14879639146403495l)) {
                            Object[] data = new Object[]{dataStr[0],
                                    time,
                                    Double.valueOf(dataStr[2]),
                                    Double.valueOf(dataStr[3]),
                                    Double.valueOf(dataStr[4])};
                            _collector.emit("PlayStream1", new Values(data));
                            //System.out.println("Emited Event "+ counter.incrementAndGet());
                            break;
                        }
                        line = reader.readLine();
                    }
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

    }

    private static SiddhiBolt configureSiddhiBolt() {
        SiddhiBolt siddhiBolt = new SiddhiBolt(
                new String[]{"define stream PlayStream1 ( sid string, ts long, x double, y double, z double)",
                             "define partition sensorIDParition by PlayStream1.sid, LocationBySecondStream.sid"
                },
                new String[]{"from PlayStream1#window.timeBatch(1sec) select sid, avg(x) as xMean, avg(y) as yMean, avg(z) as zMean "
                        + "insert into LocationBySecondStream partition by sensorIDParition;",
                        "from every e1 = LocationBySecondStream -> e2 = LocationBySecondStream[e1.yMean + 10000 > yMean or yMean + 10000 > e1.yMean] within 2sec select e1.sid "
                                + "insert into LongAdvanceStream partition by sensorIDParition;"
                },
        		//new String[]{"from PlayStream1 select sid, x as xMean, y as yMean, z as zMean insert into LongAdvanceStream;"},
                new String[]{"LongAdvanceStream"});

        return siddhiBolt;

    }

    public static void main(String[] args) throws Exception {

        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("source", new PlayStream(), 1);
        builder.setBolt("node1", configureSiddhiBolt(), 1).shuffleGrouping("source", "PlayStream1");
        builder.setBolt("LeafEcho", new EchoBolt(), 1).shuffleGrouping("node1", "LongAdvanceStream");

        Config conf = new Config();

        if (args != null && args.length > 0) {
            conf.setNumWorkers(3);

            StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
        } else {
            conf.setMaxTaskParallelism(3);

            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("football-topology", conf, builder.createTopology());

            Thread.sleep(10000);
            cluster.shutdown();
        }
    }
}