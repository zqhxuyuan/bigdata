package com.zqh.midd.kafka.demo;

import kafka.producer.Partitioner;
import kafka.utils.VerifiableProperties;

/**
 * Created by hadoop on 14-11-27.
 */
public class SimplePartitioner implements Partitioner {
    public SimplePartitioner (VerifiableProperties props) {
    }

    public int partition(Object key, int a_numPartitions) {
        return ((String)key).length()%a_numPartitions;
    }
}
