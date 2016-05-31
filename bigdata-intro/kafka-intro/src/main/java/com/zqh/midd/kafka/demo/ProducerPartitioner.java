package com.zqh.midd.kafka.demo;

import kafka.producer.Partitioner;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProducerPartitioner implements Partitioner {
    
    public static final Logger LOG=LoggerFactory.getLogger(Keyword.class); 
     
    @Override
    public int partition(Object key, int numPartitions) {
        LOG.info("ProducerPartitioner key:"+key+" partitions:"+numPartitions);
        String strKey = (String)key;
        return strKey.length() % numPartitions;
    }

}
