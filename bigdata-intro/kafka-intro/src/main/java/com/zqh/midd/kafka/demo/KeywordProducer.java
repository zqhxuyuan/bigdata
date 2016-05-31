package com.zqh.midd.kafka.demo;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

public class KeywordProducer {

	public static void main(String[] args) {
		/**配置producer必要的参数*/
        Properties props = new Properties();
        props.put("zk.connect", "localhost:2181");
        props.put("zk.connectiontimeout.ms", "6000");

        /**选择用哪个类来进行序列化*/
        props.put("serializer.class", "com.zqh.midd.kafka.demo.KeywordMessage");
        props.put("metadata.broker.list","localhost:9092");
        ProducerConfig config=new ProducerConfig(props);
         
        /**制造数据*/
        Keyword keyword=new Keyword();
        keyword.setUser("Chenhui");
        keyword.setId(0);
        keyword.setKeyword("china");
         
        List<Keyword> msg=new ArrayList<Keyword>();
        msg.add(keyword);
        
        keyword.setUser("zqhxuyuan");
        keyword.setId(1);
        keyword.setKeyword("kafka");
        msg.add(keyword);
         
        /**构造数据发送对象*/
        Producer<String, Keyword> producer=new Producer<String, Keyword>(config);

        producer.send((List<KeyedMessage<String, Keyword>>) 
        		new KeyedMessage<Integer, List>("test", msg));

        //ProducerData<String,Keyword> data=new ProducerData<String, Keyword>("test", msg);
        //producer.send(data);
	}
}
