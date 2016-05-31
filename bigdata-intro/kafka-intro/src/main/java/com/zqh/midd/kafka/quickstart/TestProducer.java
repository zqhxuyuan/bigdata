package com.zqh.midd.kafka.quickstart;

import java.util.*;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

/**
 * Created by hadoop on 14-11-19.
 *
 * https://cwiki.apache.org/confluence/display/KAFKA/0.8.0+Producer+Example
 */
public class TestProducer {
    public static void main(String[] args) {
        long events = 100;
        Random rnd = new Random();

        // First define properties for how the Producer finds the cluster,
        // serializes the messages and if appropriate directs the message to a specific Partition.
        Properties props = new Properties();
        // 单机单服务
        props.put("metadata.broker.list", "localhost:9092");
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        props.put("partitioner.class", "com.zqh.midd.kafka.quickstart.SimplePartitioner");
        props.put("request.required.acks", "1");

        ProducerConfig config = new ProducerConfig(props);

        // Next you define the Producer object itself
        // The first is the type of the Partition key, the second the type of the message
        Producer<String, String> producer = new Producer<String, String>(config);

        for (long nEvents = 0; nEvents < events; nEvents++) {
            // Now build your message
            long runtime = new Date().getTime();
            String ip = "192.168.2." + rnd.nextInt(255);
            String msg = runtime + ",www.example.com," + ip;

            // Finally write the message to the Broker
            // passing the IP as the partition key
            KeyedMessage<String, String> data = new KeyedMessage<String, String>("test", ip, msg);
            producer.send(data);
        }
        producer.close();
    }
}