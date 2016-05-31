package com.zqh.midd.kafka.demo;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import java.util.Properties;
import java.util.Random;

/**
 * Created by hadoop on 14-11-26.
 */
public class HelloProducer {

    public static void main(String[] args) throws Exception{
        Properties props = new Properties();
        //指定kafka节点：注意这里无需指定集群中所有Boker，只要指定其中部分即可，它会自动取meta信息并连接到对应的Boker节点
        props.put("metadata.broker.list", "localhost:9092");
        //指定采用哪种序列化方式将消息传输给Boker,你也可以在发送消息的时候指定序列化类型，不指定则以此为默认序列化类型
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        //指定消息发送对应分区方式，若不指定，则随机发送到一个分区，也可以在发送消息的时候指定分区类型。
        props.put("partitioner.class", "com.zqh.midd.kafka.demo.SimplePartitioner");
        //该属性表示你需要在消息被接收到的时候发送ack给发送者。以保证数据不丢失
        props.put("request.required.acks", "1");

        ProducerConfig config = new ProducerConfig(props);
        //申明生产者：泛型1为分区key类型，泛型2为消息类型
        Producer<String, String> producer = new Producer<String, String>(config);

        //创建KeyedMessage发送消息，参数1为topic名，参数2为分区名（若为null则随机发到一个分区），参数3为消息
        //producer.send(new KeyedMessage<String, String>("test2", "partitionKey1", "msg1"));
        //producer.close();

        Random random = new Random(1000);
        int count = 0;
        while (true){
            //注意如果partitionKey为null的话, 要注释掉props中的partitioner.class.
            producer.send(new KeyedMessage<String, String>("test2", "partitionKey1", "msg-" + random.nextInt()));
            Thread.sleep(100);
            count ++;
            if(count == 100000){
                break;
            }
        }
        producer.close();
    }
}
