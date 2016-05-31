package com.zqh.nosql.redis.spring;

public interface QueueListener<T> {
    public void onMessage(T value);
}
