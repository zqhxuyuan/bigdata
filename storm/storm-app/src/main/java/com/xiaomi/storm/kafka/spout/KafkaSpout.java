package com.xiaomi.storm.kafka.spout;

import java.io.UnsupportedEncodingException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.xiaomi.storm.kafka.common.CommonUtils;
import com.xiaomi.storm.kafka.common.DynamicPartitionConnections;
import com.xiaomi.storm.kafka.common.DynamicPartitionManagerKeeper;
import com.xiaomi.storm.kafka.common.EmitState;
import com.xiaomi.storm.kafka.common.KafkaMessageId;
import com.xiaomi.storm.kafka.common.PartitionManager;
import com.xiaomi.storm.kafka.common.SpoutConfigParser;
import com.xiaomi.storm.kafka.common.ZkState;

import backtype.storm.Config;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;


@SuppressWarnings("all")
public class KafkaSpout extends BaseRichSpout {
	
	private static final long serialVersionUID = 1L;
	
	/* (non-Javadoc)
	 * @see backtype.storm.topology.base.BaseRichSpout#close()
	 */
	@Override
	public void close() {
		//shut down curator
		this._zkState.close();
	}

	/* (non-Javadoc)
	 * @see backtype.storm.topology.base.BaseRichSpout#activate()
	 */
	@Override
	public void activate() {
		commit();
	}

	/* (non-Javadoc)
	 * @see backtype.storm.topology.base.BaseRichSpout#deactivate()
	 */
	@Override
	public void deactivate() {
		// TODO Auto-generated method stub
		commit();
	}
	
	/* (non-Javadoc)
	 * @see backtype.storm.topology.base.BaseRichSpout#ack(java.lang.Object)
	 */
	@Override
	public void ack(Object msgId) {
		KafkaMessageId id = (KafkaMessageId)msgId;
		PartitionManager pm = this._pmKeeper.getManagers(id.getPartitionId());
		if(null != pm) {
			/*
			 * remove pending offset
			 * done emit and no need to commit
			 */
			pm.ack(id.getOffset());
		}		
	}

	/* (non-Javadoc)
	 * @see backtype.storm.topology.base.BaseRichSpout#fail(java.lang.Object)
	 */
	@Override
	public void fail(Object msgId) {
		/*
		 * collector.emit(tup, new KafkaMessageId(this._id, toEmitMsg.getOffset()));
		 * so i can get from here
		 */
		KafkaMessageId id = (KafkaMessageId)msgId;
		PartitionManager pm = this._pmKeeper.getManagers(id.getPartitionId());
		if(null != pm) {
			//rollback to recommit
			pm.fail(id.getOffset());
		}
	}

	public KafkaSpout(SpoutConfigParser _configParser) {
		this._configParser = _configParser;
	}
	
	@Override
	public void open(Map conf, TopologyContext context,
			SpoutOutputCollector collector) {
		// TODO Auto-generated method stub
		this._collector = collector;
		
		/*
		 * init zookeeper [load zk server and port]
		 */
		Map stateConf = new HashMap(conf);
		
		List<String> zkServers = this._configParser.zkServers;
		//System.out.println("$$$$$$$$$$$$$$$$$$$$$$" + zkServers.toString());
		if(zkServers == null) {
			zkServers = (List<String>)conf.get(Config.STORM_ZOOKEEPER_SERVERS);
		}
		Integer zkPort = this._configParser.zkPort;
		if(zkPort == null) {
			zkPort = (Integer) (conf.get(Config.STORM_ZOOKEEPER_PORT));
		}
		String zkRoot = this._configParser.zkRoot;
		stateConf.put(Config.TRANSACTIONAL_ZOOKEEPER_SERVERS, zkServers);
		stateConf.put(Config.TRANSACTIONAL_ZOOKEEPER_PORT, zkPort);
		stateConf.put(Config.TRANSACTIONAL_ZOOKEEPER_ROOT, zkRoot);
		//init zk operator object
		_zkState = new ZkState(stateConf);
		
		//init kafka consumer connection
		//_configParser = new SpoutConfigParser();
		_connections = new DynamicPartitionConnections(this._configParser);
		
		//using Transactions
		int totalTasks = context.getComponentTasks(context.getThisComponentId()).size();
		//System.out.println("!!!!!!!!!!!!!!!" + totalTasks);
		this._pmKeeper = new DynamicPartitionManagerKeeper(
				_connections,
				_zkState,
				this._configParser,
				stateConf,
				context.getThisTaskId(),
				totalTasks,
				CommonUtils.getUUID());
	}

	@Override
	public void nextTuple() {
        //the size of managers keeps the numbers of consumers(hostport, pid)
		List<PartitionManager> managers = this._pmKeeper.getPartitionManagers();
		//System.out.println("%%%%%%%%%%%%%%%%% manger size:" + managers.size());
		for (int i=0; i<managers.size(); i++) {
			this._currPartitionIndex = this._currPartitionIndex % managers.size();
			EmitState state;
			try {
				state = managers.get(this._currPartitionIndex).next(this._collector);
			//no more left to emit then move to next partition
				if(state != EmitState.EMITTED_MORE_LEFT) {
					this._currPartitionIndex = (this._currPartitionIndex + 1) % managers.size();
				}
				if(state != EmitState.NO_EMITTED) {
					//emit end then commit the status
					//System.out.println("############### partition " + this._currPartitionIndex + "break out");
					break;
				}
			} catch (UnsupportedEncodingException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
		long now = System.currentTimeMillis();
		if((now - this._lastUpdateMs) > this._configParser.stateUpdateIntervalsMs) {
			commit();
		} 
	}

	//update state to zk	
	private void commit() {
		this._lastUpdateMs = System.currentTimeMillis();
		for(PartitionManager pm : this._pmKeeper.getPartitionManagers()) {
			pm.commit();
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(this._configParser.scheme.getOutputFields());
	}
	
	private long _lastUpdateMs = 0;
	private int _currPartitionIndex = 0;
	private DynamicPartitionConnections _connections;
	private DynamicPartitionManagerKeeper _pmKeeper;
	private ZkState _zkState; 
	private SpoutConfigParser _configParser;
	private SpoutOutputCollector _collector;

}
