package org.wso2.siddhi.storm;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import org.wso2.siddhi.storm.components.EchoBolt;
import org.wso2.siddhi.storm.components.SiddhiBolt;
import org.wso2.siddhi.storm.components.StockDataSpout;

public class StockDataTopology {

    private static final String STOCK_DATA_STREAM_DEF = "define stream StockData (symbol string, price double, volume int);";
    private static final String STOCK_QUOTE_STREAM_DEF = "define stream StockQuote (symbol string, price double);";

    private static SiddhiBolt configureSiddhiBolt(String query, String outputStream) {
        SiddhiBolt siddhiBolt = new SiddhiBolt(
                new String[]{STOCK_DATA_STREAM_DEF, STOCK_QUOTE_STREAM_DEF},
                //new String[]{"from StockData#window.timeBatch(1sec) select symbol, price, avg(volume) as avgV insert into AvgVolume;"},
                new String[]{query},
                new String[]{outputStream});

        return siddhiBolt;
    }

    public static void main(String[] args) throws Exception {
        if (args == null || args.length < 3) {
            throw new Exception("Topology name, query and exported stream must be passed as command line arguments");
        }

        for (int i = 0; i < args.length; i++) {
            System.out.println("Args[" + i + "]=" + args[i]);
        }

        String topologyName = args[0];
        String query = args[1].replace('^', ' ');
        String exportedStream = args[2];
        int spoutParallalism = (args.length > 3 && args[3] != null) ? Integer.parseInt(args[3]) : 1;
        int siddhiBoltParallalism = (args.length > 4 && args[4] != null) ? Integer.parseInt(args[4]) : 1;
        int echoBoltParallalism = (args.length > 5 && args[5] != null) ? Integer.parseInt(args[5]) : 1;
        int topologyWorkers = (args.length > 6 && args[6] != null) ? Integer.parseInt(args[6]) : 3;

        System.out.println("Query : " + query);
        System.out.println("Exported Stream : " + exportedStream);
        System.out.println("Topology : " + topologyName);
        System.out.println("Spout Parallelism : " + spoutParallalism);
        System.out.println("Siddhi Bolt Parallelism : " + siddhiBoltParallalism);
        System.out.println("Echo Bolt Parallelism : " + echoBoltParallalism);
        System.out.println("Topology Workers : " + topologyWorkers);

        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("StockData", new StockDataSpout(), spoutParallalism);
        builder.setBolt("SiddhiBolt", configureSiddhiBolt(query, exportedStream), siddhiBoltParallalism).fieldsGrouping("StockData", new Fields("symbol"));
        builder.setBolt("Echo", new EchoBolt(), echoBoltParallalism).shuffleGrouping("SiddhiBolt");

        Config conf = new Config();
        conf.setDebug(false);

        if (args.length > 0) {
            conf.setNumWorkers(topologyWorkers);
            StormSubmitter.submitTopology(topologyName, conf, builder.createTopology());
        } else {
            conf.setMaxTaskParallelism(3);
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("LatencyMeasureTopology", conf, builder.createTopology());
        }
    }
}