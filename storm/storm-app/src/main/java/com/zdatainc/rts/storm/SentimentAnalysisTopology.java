package com.zdatainc.rts.storm;

import com.zdatainc.rts.model.Properties;
import org.apache.log4j.Logger;
import org.apache.log4j.BasicConfigurator;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.generated.StormTopology;
import backtype.storm.StormSubmitter;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.ZkHosts;
import storm.kafka.StringScheme;

public class SentimentAnalysisTopology {
    private final Logger LOGGER = Logger.getLogger(this.getClass());
    private static final String KAFKA_TOPIC =
            Properties.getString("rts.storm.kafka_topic");

    public static void main(String[] args) throws Exception {
        BasicConfigurator.configure();

        if (args != null && args.length > 0) {
            StormSubmitter.submitTopology(
                    args[0],
                    createConfig(false),
                    createTopology());
        } else {
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology(
                    "sentiment-analysis",
                    createConfig(true),
                    createTopology());
            Thread.sleep(60000);
            cluster.shutdown();
        }
    }

    private static StormTopology createTopology() {
        SpoutConfig kafkaConf = new SpoutConfig(
                new ZkHosts(Properties.getString("rts.storm.zkhosts")),
                KAFKA_TOPIC,
                "/kafka",
                "KafkaSpout");
        kafkaConf.scheme = new SchemeAsMultiScheme(new StringScheme());
        TopologyBuilder topology = new TopologyBuilder();

        topology.setSpout("kafka_spout", new KafkaSpout(kafkaConf), 4);

        //英文twitter
        topology.setBolt("twitter_filter", new TwitterFilterBolt(), 4)
                .shuffleGrouping("kafka_spout");

        //文本过滤,转换为小写
        topology.setBolt("text_filter", new TextFilterBolt(), 4)
                .shuffleGrouping("twitter_filter");

        //去掉常用词
        topology.setBolt("stemming", new StemmingBolt(), 4)
                .shuffleGrouping("text_filter");

        //相同的输出, 会发射到两个bolt中. 一个用于判断正能量的文本, 一个为负能量
        //正能量
        topology.setBolt("positive", new PositiveSentimentBolt(), 4)
                .shuffleGrouping("stemming");
        //负能量
        topology.setBolt("negative", new NegativeSentimentBolt(), 4)
                .shuffleGrouping("stemming");

        /**
         *                |-----Positive-----|
         * SteamingBolt --|                  |---- JoinBolt
         *                |-----Negative-----|
         */
        //join连接. 由于同一条文本,发送给Positive和Negative两个Bolt处理.
        //Bolt处理的速度不同. JoinBolt接收到的也就没有顺序.
        topology.setBolt("join", new JoinSentimentsBolt(), 4)
                //使用字段分组, 确保相同的twitter被放在一起join
                //由于一条twitter有两个输出,这里Join就对应有两个输入.
                .fieldsGrouping("positive", new Fields("tweet_id"))
                .fieldsGrouping("negative", new Fields("tweet_id"));

        topology.setBolt("score", new SentimentScoringBolt(), 4)
                .shuffleGrouping("join");

        topology.setBolt("hdfs", new HDFSBolt(), 4)
                .shuffleGrouping("score");
        topology.setBolt("nodejs", new NodeNotifierBolt(), 4)
                .shuffleGrouping("score");

        return topology.createTopology();
    }

    private static Config createConfig(boolean local) {
        int workers = Properties.getInt("rts.storm.workers");
        Config conf = new Config();
        conf.setDebug(true);
        if (local)
            conf.setMaxTaskParallelism(workers);
        else
            conf.setNumWorkers(workers);
        return conf;
    }
}
