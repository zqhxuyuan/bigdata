package com.xiaomi.storm.kafka.common;

public class GlobalPartitionId {
	
	/**
	 * @return the hostPort
	 */
	public KafkaHostPort getHostPort() {
		return hostPort;
	}
	/**
	 * @return the partitionId
	 */
	public int getPartitionId() {
		return partitionId;
	}
	/* (non-Javadoc)
	 * @see java.lang.Object#hashCode()
	 */
	@Override
	public int hashCode() {
		// TODO Auto-generated method stub
		return 13 * hostPort.hashCode() + partitionId;
	}
	/* (non-Javadoc)
	 * @see java.lang.Object#equals(java.lang.Object)
	 */
	@Override
	public boolean equals(Object obj) {
		// TODO Auto-generated method stub
		GlobalPartitionId other = (GlobalPartitionId) obj;
		return hostPort.equals(other.hostPort) && partitionId == other.partitionId;
	}
	/* (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		// TODO Auto-generated method stub
		return getId();
	}
	
	public String getId() {
		return hostPort.toString() + ":" + partitionId;
	}
	
	GlobalPartitionId(KafkaHostPort hostPort, int partitionId) {
		this.hostPort = hostPort;
		this.partitionId = partitionId;
	}
	
	private KafkaHostPort hostPort;
	private int partitionId;
}
