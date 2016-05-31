package com.zqh.midd.rocketmq;

import com.alibaba.rocketmq.client.producer.DefaultMQProducer;
import com.alibaba.rocketmq.client.producer.SendResult;
import com.alibaba.rocketmq.common.message.Message;

/**
 * TODO: Doesn't work anymore!
 * http://blog.csdn.net/a19881029/article/details/34446629
 * http://my.oschina.net/cloudcoder/blog/200741
 * http://www.cnblogs.com/marcotan/p/4256858.html
 */
public class Producer {
    public static void main(String[] args){
        DefaultMQProducer producer = new DefaultMQProducer("Producer");
        producer.setNamesrvAddr("localhost:9876");
        producer.setInstanceName("Producer");
        try {
            producer.start();

            Message msg = new Message("PushTopic", "push", "1", "Just for test.".getBytes());

            SendResult result = producer.send(msg);
            System.out.println("id:" + result.getMsgId() + " result:" + result.getSendStatus());

            msg = new Message("PushTopic", "push", "2", "Just for test.".getBytes());

            result = producer.send(msg);
            System.out.println("id:" + result.getMsgId() + " result:" + result.getSendStatus());

            msg = new Message("PullTopic", "pull", "1", "Just for test.".getBytes());

            result = producer.send(msg);
            System.out.println("id:" + result.getMsgId() + " result:" + result.getSendStatus());
        } catch (Exception e) {
            e.printStackTrace();
        }finally{
            producer.shutdown();
        }
    }
}