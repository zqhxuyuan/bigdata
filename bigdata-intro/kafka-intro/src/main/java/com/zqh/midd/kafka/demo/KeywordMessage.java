package com.zqh.midd.kafka.demo;

import kafka.message.Message;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// 按照kafka的消息类型进行封装，只需要实现Encoder类即可
// 这样KeywordMessage就是一个可以被kafka发送和存储的对象了
public class KeywordMessage implements kafka.serializer.Encoder<Keyword>{
    
    public static final Logger LOG=LoggerFactory.getLogger(Keyword.class); 
    
    public Message toMessage(Keyword words) {
        LOG.info("start in encoding...");
        return new Message(words.toString().getBytes());
    }

    // 在封转成kafka的message时需要将数据转化成byte[]类型
	@Override
	public byte[] toBytes(Keyword words) {
		return words.toString().getBytes();
	}
 
}
