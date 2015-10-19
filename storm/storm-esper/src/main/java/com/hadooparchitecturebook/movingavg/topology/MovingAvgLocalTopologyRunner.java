package com.hadooparchitecturebook.movingavg.topology;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.generated.StormTopology;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import com.hadooparchitecturebook.movingavg.topology.CalcMovingAvgBolt;
import com.hadooparchitecturebook.movingavg.topology.ParseTicksBolt;
import com.hadooparchitecturebook.movingavg.topology.StockTicksSpout;

import org.apache.storm.hdfs.bolt.HdfsBolt;
import org.apache.storm.hdfs.bolt.format.DefaultFileNameFormat;
import org.apache.storm.hdfs.bolt.format.DelimitedRecordFormat;
import org.apache.storm.hdfs.bolt.format.FileNameFormat;
import org.apache.storm.hdfs.bolt.format.RecordFormat;
import org.apache.storm.hdfs.bolt.rotation.FileRotationPolicy;
import org.apache.storm.hdfs.bolt.rotation.FileSizeRotationPolicy.Units;
import org.apache.storm.hdfs.bolt.rotation.FileSizeRotationPolicy;
import org.apache.storm.hdfs.bolt.rotation.TimedRotationPolicy;
import org.apache.storm.hdfs.bolt.sync.CountSyncPolicy;
import org.apache.storm.hdfs.bolt.sync.SyncPolicy;
import storm.starter.test.PrintBolt;

/**
 * Create topology to calculate moving averages over stock data.
 */
public class MovingAvgLocalTopologyRunner {

    public static void main(String[] args) {

        Config config = new Config();
        config.setDebug(true);

        StormTopology topology = buildTopology();

        if(args.length>0){
            try {
                StormSubmitter.submitTopology("cluster-moving-average",
                        config,
                        topology);
            } catch(AlreadyAliveException e) {
                e.printStackTrace();
            } catch(InvalidTopologyException e) {
                e.printStackTrace();
            }
        }else{
            LocalCluster localCluster = new LocalCluster();
            localCluster.submitTopology("local-moving-avg", config, topology);
        }
    }

    /**
     * Return the object creating our moving average topology.
     */
    private static StormTopology buildTopology() {

        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("stock-ticks-spout", new StockTicksSpout());

        //保存一份数据到HDFS中,测试时可以去掉
//        builder.setBolt("hdfs-persister-bolt", createHdfsBolt())
//                .shuffleGrouping("stock-ticks-spout");

        builder.setBolt("parse-ticks", new ParseTicksBolt())
                .shuffleGrouping("stock-ticks-spout");

        //相同的ticker要放到一起计算
        builder.setBolt("calc-moving-avg", new CalcMovingAvgBolt(), 2)
                .fieldsGrouping("parse-ticks", new Fields("ticker"));

        //builder.setBolt("print", new PrintBolt()).shuffleGrouping("calc-moving-avg");

        return builder.createTopology();
    }

    /**
     * Create bolt which will persist ticks to HDFS.
     */
    private static HdfsBolt createHdfsBolt() {

        // Use "|" instead of "," for field delimiter:
        RecordFormat format = new DelimitedRecordFormat()
                .withFieldDelimiter("|");
        // sync the filesystem after every 1k tuples:
        SyncPolicy syncPolicy = new CountSyncPolicy(100);

        // Rotate files when they reach 5MB:
        FileRotationPolicy rotationPolicy =
                new FileSizeRotationPolicy(5.0f, Units.MB);

        // Write records to <user>/stock-ticks/ directory in HDFS:
        FileNameFormat fileNameFormat = new DefaultFileNameFormat()
                .withPath("stock-ticks/");

        HdfsBolt hdfsBolt = new HdfsBolt()
                .withFsUrl("hdfs://localhost:8020")
                .withFileNameFormat(fileNameFormat)
                .withRecordFormat(format)
                .withRotationPolicy(rotationPolicy)
                .withSyncPolicy(syncPolicy);

        return hdfsBolt;
    }
}
