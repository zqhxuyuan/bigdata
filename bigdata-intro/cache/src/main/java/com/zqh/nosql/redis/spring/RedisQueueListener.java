package com.zqh.nosql.redis.spring;

public class RedisQueueListener<String> implements QueueListener<String> {
    @Override
    public void onMessage(String value) {
        System.out.println(value);
    }
}