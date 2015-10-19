package com.zqh.storm.wc;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

/**
 * 1. 首先启动ZooKeeper
 * 2. 在IDE中直接运行, 由于没有参数, 使用本地模式.
 * 3. 输出结果:
 --- FINAL COUNTS ---
 a : 2487
 ate : 2488
 beverages : 2488
 cold : 2488
 cow : 2487
 dog : 4976
 don't : 4974
 fleas : 4976
 has : 2488
 have : 2487
 homework : 2488
 i : 7464
 like : 4976
 man : 2487
 my : 4976
 the : 2488
 think : 2487
 --------------
 */
public class WordCountTopology {

	private static final String SENTENCE_SPOUT_ID = "sentence-spout";
	private static final String SPLIT_BOLT_ID = "split-bolt";
	private static final String COUNT_BOLT_ID = "count-bolt";
	private static final String REPORT_BOLT_ID = "report-bolt";
	private static final String TOPOLOGY_NAME = "word-count-topology";

	public static void main(String[] args) throws Exception {
        /**
         * INPUT:
         *   Hello World
         *   Hello Storm
         *
         * Spout的并行度设置为2, 则会发送两次
         *   Hello : 4
         *   Storm : 2
         *   World : 2
         *
         * Spout的并行度设置为1, 只会发送一次
         *   Hello : 2
         *   Storm : 1
         *   World : 1
         */
        TopologyBuilder builder = buildTopology(1,2,4,4);
        //TopologyBuilder builder = buildTopology(2,2,4,4);
        Config config = new Config();
        config.setNumWorkers(2);

        if(args.length == 0){
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology(TOPOLOGY_NAME, config, builder.createTopology());

            Thread.sleep(10000);

            cluster.killTopology(TOPOLOGY_NAME);
            cluster.shutdown();
        } else{
            StormSubmitter.submitTopology(args[0], config, builder.createTopology());
        }
	}

    /**
     * @param pSpout Spout的并行度, 注意在本示例中,如果大于1,表示发送了多次!
     * @param pSSBolt SplitSentenceBolt的并行度
     * @param tSSBolt SplitSentenceBolt的任务数
     * @param pWCBolt WordCount的并行度
     * @return
     */
    public static TopologyBuilder buildTopology(int pSpout, int pSSBolt, int tSSBolt, int pWCBolt){
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout(SENTENCE_SPOUT_ID, new SentenceSpout1(), pSpout);

        // SentenceSpout --> SplitSentenceBolt
        builder.setBolt(SPLIT_BOLT_ID, new SplitSentenceBolt(), pSSBolt)
                .setNumTasks(tSSBolt)
                .shuffleGrouping(SENTENCE_SPOUT_ID);

        // SplitSentenceBolt --> WordCountBolt
        builder.setBolt(COUNT_BOLT_ID, new WordCountBolt(), pWCBolt)
                .fieldsGrouping(SPLIT_BOLT_ID, new Fields("word"));

        // WordCountBolt --> ReportBolt
        //builder.setBolt(REPORT_BOLT_ID, new ReportBolt()).globalGrouping(COUNT_BOLT_ID);

        return builder;
    }
}
