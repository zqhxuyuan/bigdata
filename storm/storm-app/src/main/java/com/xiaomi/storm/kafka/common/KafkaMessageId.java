package com.xiaomi.storm.kafka.common;

/*
 * *{hostport with partitionid}
 * 	{message Offset}
 */

public class KafkaMessageId {
	/**
	 * @return the partitionId
	 */
	public GlobalPartitionId getPartitionId() {
		return partitionId;
	}
	/**
	 * @return the offset
	 */
	public long getOffset() {
		return offset;
	}
	public KafkaMessageId(GlobalPartitionId partitionId, long offset) {
		this.partitionId = partitionId;
		this.offset = offset;
	}
	
	private GlobalPartitionId partitionId;
	private long offset;
}
