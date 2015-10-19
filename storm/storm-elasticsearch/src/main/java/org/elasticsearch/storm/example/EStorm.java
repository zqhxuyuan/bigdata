package org.elasticsearch.storm.example;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.testing.TestWordSpout;
import backtype.storm.topology.TopologyBuilder;
import org.elasticsearch.storm.EsSpout;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * Created by zhengqh on 15/9/27.
 *
 * bin/kafka-topics.sh --zookeeper 192.168.6.55:2181 --create --topic demo_es_storm_kafka --replication-factor 1 --partitions 1
 * bin/kafka-console-consumer.sh --zookeeper 192.168.6.55:2181 --topic demo_es_storm_kafka --from-beginning
 */
public class EStorm {

    public static void main(String[] args) throws Exception{
        String query = "{\"query\": {\"range\": {\"activity.eventOccurTime\": {\"from\" : 1442419200000,\"to\" : 1442419800000}}}}";

        Map<String,String> esConfig = new HashMap<>();
        esConfig.put("pushdown","true");
        esConfig.put("es.nodes", "localhost");
        esConfig.put("es.port","9200");
        esConfig.put("es.query", query);
        esConfig.put("es.http.timeout", "5m");
        esConfig.put("es.storm.spout.fields", "activity");

        Properties kafkaConfig = new Properties();
        kafkaConfig.put("metadata.broker.list", "192.168.6.55:9092,192.168.6.56:9092,192.168.6.57:9092");
        kafkaConfig.put("request.required.acks", "0");
        kafkaConfig.put("serializer.class", "kafka.serializer.StringEncoder");

        System.out.println("build topology....");
        TopologyBuilder builder = new TopologyBuilder();

        //builder.setSpout("es-spout", new TestWordSpout(), 5);
        builder.setSpout("es-spout", new EsSpout("forseti-20150916/activity", query, esConfig), 5);
        //builder.setBolt("bolt", new KafkaBolt()).shuffleGrouping("es-spout");
        builder.setBolt("print", new PrintBolt()).shuffleGrouping("es-spout");

        Config conf = new Config();
        //conf.put(KafkaBolt.KAFKA_BROKER_PROPERTIES, kafkaConfig);
        //conf.put(KafkaBolt.TOPIC, "demo_es_storm_kafka");
        conf.setDebug(true);

        if (args != null && args.length > 0) {
            conf.setNumWorkers(3);

            StormSubmitter.submitTopologyWithProgressBar(args[0], conf, builder.createTopology());
        } else {
            conf.setMaxTaskParallelism(3);

            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("word-count", conf, builder.createTopology());

            Thread.sleep(10000000);
            cluster.shutdown();
        }
    }
}
