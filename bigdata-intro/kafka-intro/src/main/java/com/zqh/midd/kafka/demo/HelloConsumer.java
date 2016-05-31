package com.zqh.midd.kafka.demo;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created by hadoop on 14-11-26.
 *
 * High-level Consumer
 *
 * 首先要先创建topic: bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 3 --topic test2
 * 先启动消费者, 然后启动生产者, 观察在消费者的控制台能够看到生产者发送的消息
 *
 * bin/kafka-topics.sh --zookeeper localhost:2181 --delete --topic test2
 * bin/kafka-topics.sh --list --zookeeper localhost:2181
 */
public class HelloConsumer {
    public static void main(String[] args) throws Exception {
        Properties props = new Properties();
        // 指定zookeeper服务器地址
        props.put("zookeeper.connect", "localhost:2181");
        // 指定消费组（没有它会自动添加）
        props.put("group.id", "id1");
        // 指定kafka等待多久zookeeper回复（ms）以便放弃并继续消费。
        props.put("zookeeper.session.timeout.ms", "4000");
        // 指定zookeeper同步最长延迟多久再产生异常
        props.put("zookeeper.sync.time.ms", "2000");
        // 指定多久消费者更新offset到zookeeper中。注意offset更新时基于time而不是每次获得的消息。
        // 一旦在更新zookeeper发生异常并重启，将可能拿到已拿到过时的消息
        props.put("auto.commit.interval.ms", "1000");
        ConsumerConnector consumer = Consumer.createJavaConsumerConnector(new ConsumerConfig(props));

        String topic = "test2";

        // 我们要告诉kafka该进程会有多少个线程来处理对应的topic
        Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
        int a_numThreads = 3;
        // 用3个线程来处理topic:test2
        topicCountMap.put(topic, a_numThreads);
        // 拿到每个stream对应的topic
        Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumer.createMessageStreams(topicCountMap);
        List<KafkaStream<byte[], byte[]>> streams = consumerMap.get(topic);

        // 调用thread pool来处理topic
        ExecutorService executor = Executors.newFixedThreadPool(a_numThreads);
        for (final KafkaStream stream : streams) {
            executor.submit(new Runnable() {
                public void run() {
                    ConsumerIterator<byte[], byte[]> it = stream.iterator();
                    while (it.hasNext()) {
                        System.out.println(Thread.currentThread() + ":" + new String(it.next().message()));
                    }
                }
            });
        }
        System.in.read();
        // 关闭
        if (consumer != null) consumer.shutdown();
        if (executor != null) executor.shutdown();
    }
}
