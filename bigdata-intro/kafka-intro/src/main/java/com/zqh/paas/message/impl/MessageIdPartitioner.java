package com.zqh.paas.message.impl;

import kafka.producer.Partitioner;
import kafka.utils.VerifiableProperties;

public class MessageIdPartitioner implements Partitioner {

	public MessageIdPartitioner(VerifiableProperties props) {
		
	}
	
	public int partition(Object id, int partitionNumber) {
		long key = Long.parseLong(id.toString());
		return (int)key%partitionNumber;
	}
	
	
}
