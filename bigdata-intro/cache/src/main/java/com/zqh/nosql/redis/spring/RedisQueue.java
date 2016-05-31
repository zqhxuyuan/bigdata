package com.zqh.nosql.redis.spring;

import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.core.BoundListOperations;
import org.springframework.data.redis.core.RedisConnectionUtils;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.util.CollectionUtils;

import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class RedisQueue<T> implements InitializingBean,DisposableBean {

    private RedisTemplate redisTemplate;
    private String key;
    private int cap = Short.MAX_VALUE;//最大阻塞的容量，超过容量将会导致清空旧数据
    private byte[] rawKey;
    private RedisConnectionFactory factory;
    private RedisConnection connection;//for blocking
    private BoundListOperations<String, T> listOperations;//noblocking

    private Lock lock = new ReentrantLock();//基于底层IO阻塞考虑

    private RedisQueueListener listener;//异步回调
    private Thread listenerThread;

    private boolean isClosed;

    public void setRedisTemplate(RedisTemplate redisTemplate) {
        this.redisTemplate = redisTemplate;
    }

    public void setListener(RedisQueueListener listener) {
        this.listener = listener;
    }

    public void setKey(String key) {
        this.key = key;
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        factory = redisTemplate.getConnectionFactory();
        connection = RedisConnectionUtils.getConnection(factory);
        rawKey = redisTemplate.getKeySerializer().serialize(key);
        listOperations = redisTemplate.boundListOps(key);
        if(listener != null){
            listenerThread = new ListenerThread();
            listenerThread.setDaemon(true);
            listenerThread.start();
        }
    }

    /**
     * blocking
     * remove and get last item from queue:BRPOP
     * @return
     */
    public T takeFromTail(int timeout) throws InterruptedException{
        lock.lockInterruptibly();
        try{
            List<byte[]> results = connection.bRPop(timeout, rawKey);
            if(CollectionUtils.isEmpty(results)){
                return null;
            }
            return (T)redisTemplate.getValueSerializer().deserialize(results.get(1));
        }finally{
            lock.unlock();
        }
    }

    public T takeFromTail() throws InterruptedException{
        return takeFromHead(0);
    }

    /**
     * 从队列的头，插入
     */
    public void pushFromHead(T value){
        listOperations.leftPush(value);
    }

    public void pushFromTail(T value){
        listOperations.rightPush(value);
    }

    /**
     * noblocking
     * @return null if no item in queue
     */
    public T removeFromHead(){
        return listOperations.leftPop();
    }

    public T removeFromTail(){
        return listOperations.rightPop();
    }

    /**
     * blocking
     * remove and get first item from queue:BLPOP
     * @return
     */
    public T takeFromHead(int timeout) throws InterruptedException{
        lock.lockInterruptibly();
        try{
            List<byte[]> results = connection.bLPop(timeout, rawKey);
            if(CollectionUtils.isEmpty(results)){
                return null;
            }
            return (T)redisTemplate.getValueSerializer().deserialize(results.get(1));
        }finally{
            lock.unlock();
        }
    }

    public T takeFromHead() throws InterruptedException{
        return takeFromHead(0);
    }

    @Override
    public void destroy() throws Exception {
        if(isClosed){
            return;
        }
        shutdown();
        RedisConnectionUtils.releaseConnection(connection, factory);
    }

    private void shutdown(){
        try{
            listenerThread.interrupt();
        }catch(Exception e){
            //
        }
    }

    class ListenerThread extends Thread {

        @Override
        public void run(){
            try{
                while(true){
                    T value = takeFromHead();//cast exception? you should check.
                    //逐个执行
                    if(value != null){
                        try{
                            listener.onMessage(value);
                        }catch(Exception e){
                            //
                        }
                    }
                }
            }catch(InterruptedException e){
                //
            }
        }
    }

}

