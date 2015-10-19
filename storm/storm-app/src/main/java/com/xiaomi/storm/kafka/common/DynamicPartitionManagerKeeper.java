package com.xiaomi.storm.kafka.common;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DynamicPartitionManagerKeeper {
	
	public static Logger LOG = LoggerFactory.getLogger(DynamicPartitionManagerKeeper.class);
	
	//_taskInstanceId uuid
	public DynamicPartitionManagerKeeper(
			DynamicPartitionConnections connections,
			ZkState zkState,
			SpoutConfigParser config,
			Map stormConf,
			int taskIndex,
			int totalTasks,
			String taskInstanceId) {
		this._connections = connections;	
		this._stormConf = stormConf;
		this._taskIndex = taskIndex;
		this._totalTasks = totalTasks;
		this._taskInstanceId = taskInstanceId;
		this._zkState = zkState;
		this._config = config;
		this._refreshMs = config.pm_refresh_secs * 1000;
		this._brokerFetcher = new DynamicBrokerFetcher(
				this._zkState, 
				this._config);
	}
	
	public List<PartitionManager> getPartitionManagers() {
		if(this._lastUpdateTime == 0L || 
				(System.currentTimeMillis() - this._lastUpdateTime) > this._refreshMs)
			refresh();
		this._lastUpdateTime = System.currentTimeMillis();
		return this._cachedPMs;
		
	}
	
	private void refresh() {
		LOG.info("Refreshing partition manager connections");
		try {
			Map<String, List> brokers = this._brokerFetcher.getBrokerInfo();
			//LOG.info("refreshing ####################$$" + brokers.toString());
			
			//get 
			Set<GlobalPartitionId> sets = new HashSet();
			for(String host : brokers.keySet()) {
				List info = brokers.get(host);
				int port = (Integer) info.get(0);
				int partitions = (Integer) info.get(1);
				//LOG.info("parsing ####################$$ info:" + "port:" + port + "partitions" + partitions);
				
				KafkaHostPort hp = new KafkaHostPort(host, port);
				//partitions in 0.7.x by config not accquire from zookeeper
				partitions = this._config.partitionsPerHost;
				//for 0.8.x not use partitions = this._config.partitionsPerHost;
				for(int i=0; i<partitions; i++) {
					GlobalPartitionId id = new GlobalPartitionId(hp, i);
					//LOG.info("adding ####################$$ id:" + id.toString());
					sets.add(id);
					/*
					 * avoid duplicated insertion or throw any
					 * hash set do not allow duplicated value
					 */
					/*if(myOwnership(id)) {
						LOG.info("adding ####################$$ id:" + id.toString());
						sets.add(id);
					}*/
				}
				
				Set<GlobalPartitionId> currSet = this._managers.keySet();
				Set<GlobalPartitionId> newPartitions = new HashSet<GlobalPartitionId>(sets);
				newPartitions.removeAll(currSet);
				
				Set<GlobalPartitionId> deletedPartitions = new HashSet<GlobalPartitionId>(currSet);
				deletedPartitions.removeAll(sets);
				
				LOG.info("Deleted partition managers: " + deletedPartitions.toString());
				for(GlobalPartitionId id : deletedPartitions) {
					PartitionManager pm = this._managers.remove(id);
					pm.close();//unregister partition manger
				}
				
				for(GlobalPartitionId id : newPartitions) {
					PartitionManager pm = new PartitionManager(
							this._connections,
							this._taskInstanceId,
							this._zkState,
							this._stormConf,
							this._config,
							id);
					this._managers.put(id, pm);
				}
				
			}
		} catch(Exception e) {
			throw new RuntimeException(e);
		}
		
		this._cachedPMs = new ArrayList<PartitionManager>(this._managers.values());
		LOG.info("Finised refreshing");
		
	}
	
	public PartitionManager getManagers(GlobalPartitionId id) {
		return this._managers.get(id);
	}
	
	private boolean myOwnership(GlobalPartitionId id) {
		//skip any duplicated GlobalPartition object
		int val = Math.abs(id.getHostPort().hashCode() + 13 * id.getPartitionId());
		return val % this._totalTasks == this._taskIndex;
	}
	
	private SpoutConfigParser _config;
	private ZkState _zkState;
	private String _taskInstanceId;
	private Map _stormConf;
	private int _totalTasks;
	private int _taskIndex;
	private int _refreshMs;
	private long _lastUpdateTime = 0L;
	private DynamicBrokerFetcher _brokerFetcher;
	private DynamicPartitionConnections _connections;
	private List<PartitionManager> _cachedPMs;
	Map<GlobalPartitionId, PartitionManager> _managers = new HashMap();
}
