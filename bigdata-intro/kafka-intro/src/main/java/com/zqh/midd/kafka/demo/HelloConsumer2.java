package com.zqh.midd.kafka.demo;

import com.zqh.midd.kafka.KafkaUtil;
import kafka.api.FetchRequest;
import kafka.api.FetchRequestBuilder;
import kafka.api.PartitionOffsetRequestInfo;
import kafka.common.TopicAndPartition;
import kafka.javaapi.*;
import kafka.cluster.Broker;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.message.MessageAndOffset;

import java.nio.ByteBuffer;
import java.util.*;

/**
 * Created by hadoop on 14-11-26.
 *
 * Low-level Consumer
 */
public class HelloConsumer2 {

    public static void main(String[] args) throws Exception{
        String topic = "test2";
        int partition = 1;
        //我们无需要把所有的brokers列表加进去，目的只是为了获得metedata信息，故只要有broker可连接即可
        String brokers = "localhost:9092";
        int maxReads = 100; // 读多少条数据
        // 1.找leader
        PartitionMetadata metadata = KafkaUtil.getPartitionMetadata(brokers, topic, partition, "leaderLookup");
        if (metadata == null || metadata.leader() == null) {
            System.out.println("meta data or leader not found, exit.");
            return;
        }
        // 拿到partition的leader
        Broker leadBroker = metadata.leader();
        // 获取所有副本
        System.out.println(metadata.replicas());

        // 2.获取lastOffset(这里提供了两种方式：从头取或从最后拿到的开始取(LatestTime)，下面这个是从头取)
        long whichTime = kafka.api.OffsetRequest.EarliestTime();
        System.out.println("lastTime:"+whichTime);
        String clientName = "Client_" + topic + "_" + partition;
        SimpleConsumer consumer = new SimpleConsumer(leadBroker.host(), leadBroker.port(), 100000, 64 * 1024, clientName);
        TopicAndPartition topicAndPartition = new TopicAndPartition(topic, partition);
        Map<TopicAndPartition, PartitionOffsetRequestInfo> requestInfo = new HashMap<TopicAndPartition, PartitionOffsetRequestInfo>();
        requestInfo.put(topicAndPartition, new PartitionOffsetRequestInfo(whichTime, 1));
        OffsetRequest request = new OffsetRequest(requestInfo,kafka.api.OffsetRequest.CurrentVersion(), clientName);
        // 获取指定时间前有效的offset列表
        OffsetResponse response = consumer.getOffsetsBefore(request);
        if (response.hasError()) {
            System.out.println("Error fetching data Offset Data the Broker. Reason: "
                            + response.errorCode(topic, partition));
            return;
        }
        // 千万不要认为offset一定是从0开始的
        long[] offsets = response.offsets(topic, partition);
        System.out.println("offset list:" + Arrays.toString(offsets));
        long offset = offsets[0];

        // 读数据
        KafkaUtil.consumeMsg(leadBroker, clientName, topic, partition, offset, maxReads);

        // 退出（这里象征性的写一下）
        if (consumer != null) consumer.close();
    }
}
