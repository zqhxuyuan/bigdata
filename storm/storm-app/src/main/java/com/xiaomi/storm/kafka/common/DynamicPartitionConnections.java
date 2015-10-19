package com.xiaomi.storm.kafka.common;

import java.util.HashMap;
import java.util.Map;

import com.xiaomi.storm.kafka.KafkaUtil;
import kafka.javaapi.consumer.SimpleConsumer;


/*
 * api functions
 * register a broker with multi-partition ids into a kafka consumer
 * unregister multi-partition ids in a broker and close a kafka consumer if no partition ids in a Set
 * batch close all the consumers
 */
public class DynamicPartitionConnections {

	public DynamicPartitionConnections(SpoutConfigParser config) {
		this._config = config; 
	}
	/*
	 * fill ConnectionInfo.class data structure with broker info and partition id
	 * fill kafka consumer with broker info into ConnectionInfo
	 * fill partition id into ConnectionInfo
	 * 
	 * return a filled kafka consumer
	 */
	private SimpleConsumer register(KafkaHostPort host, int partitionId) {
		/*
		 * if statement ensure a broker contains a SimpleConsumer
		 * a SimpleConsumer can add multi-partition ids
		 */
		if(!_connections.containsKey(host)) {
			_connections.put(host, 
					new ConnectionInfo(
                            // TODO add clientName by zqh
							new SimpleConsumer(host.getHost(), host.getPort(), this._config.socketTimeoutMs, this._config.bufferSizeBytes,
                                    KafkaUtil.clientName(_config.kafkaTopic, partitionId))
                    ));
		}
		ConnectionInfo info = _connections.get(host);
		info.partitionIds.add(partitionId);
		return info.get_consumer();
	}
	
	private void unregister(KafkaHostPort host, int partitionId) {
		ConnectionInfo info = _connections.get(host);
		info.partitionIds.remove(partitionId);
		if(info.partitionIds.isEmpty()) {
			info.get_consumer().close();
			_connections.remove(host);
		}
	}
	
	/*
	 * interface
	 * register broker and partition id
	 * return a filled kafka consumer
	 */
	public SimpleConsumer register(GlobalPartitionId id) {
		return register(id.getHostPort(), id.getPartitionId());
	}
	
	/*
	 * interface
	 * unregister partitions in a broker then close this consumer
	 * */
	public void unregister(GlobalPartitionId id) {
		unregister(id.getHostPort(), id.getPartitionId());
	}
	
	/*
	 * batch close consumers
	 */
	public void clear() {
		for(ConnectionInfo info : _connections.values()) {
			info.get_consumer().close();	
		}
	}	
	
	private SpoutConfigParser _config;
	/*
	 * hash Mapping kafkabroker->Message consumer
	 * */
	Map<KafkaHostPort, ConnectionInfo> _connections = new HashMap<KafkaHostPort, ConnectionInfo>();
	
}
