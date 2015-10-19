package com.xiaomi.storm.kafka.common;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.*;

import com.xiaomi.storm.kafka.KafkaUtil;
import kafka.api.FetchRequest;
import kafka.api.FetchRequestBuilder;
import kafka.api.OffsetRequest;
import kafka.api.PartitionOffsetRequestInfo;
import kafka.common.ErrorMapping;
import kafka.common.OffsetOutOfRangeException;
import kafka.common.TopicAndPartition;
import kafka.javaapi.FetchResponse;
import kafka.javaapi.OffsetResponse;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.javaapi.message.ByteBufferMessageSet;
import kafka.message.MessageAndOffset;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableMap;

import backtype.storm.Config;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.utils.Utils;

public class PartitionManager {

	public GlobalPartitionId get_id() {
		return _id;
	}

	public static final Logger LOG = LoggerFactory.getLogger(PartitionManager.class);
	
	public PartitionManager(
			DynamicPartitionConnections connections, 
			String taskInstanceId, 
			ZkState zkState,
			Map stormConf,
			SpoutConfigParser config,
			GlobalPartitionId id) {
		this._connections = connections;
		this._zkState = zkState;
		this._config = config;
		this._stormConf = stormConf;
		this._id = id;
		this._taskInstanceId = taskInstanceId;
		//aquire a consumer contains hostport and partitionid
		this._consumer = this._connections.register(_id);
		
		setCommittedOffset(this._taskInstanceId, this._config, this._zkState);
		System.out.println("partition manager init@@@@@@");
		
	}
	/*
	 * init offset
	 */
	private void setCommittedOffset(String taskInstanceId, SpoutConfigParser config, ZkState _zkState) {
        String clientName = "Client_" + _config.kafkaTopic + "_" + _id.getPartitionId();

		String jsonTaskInstanceId = null;
		Long jsonOffset = null;
		
		Map<Object, Object> json = _zkState.readJson(commitedPath());
		if(null != json) {
			try {
				//notice instance of String obj and Long obj
				jsonTaskInstanceId = (String)((Map<Object, Object>)json.get("topology")).get("id");
				jsonOffset = (Long)json.get("offset");
			} catch(Throwable e) {
				LOG.warn("Error reading at ZkNode: " + commitedPath(), e);
			}
		}
		//force start with offset u set
		//if( this._config.forceFromStart) {
		if(this._taskInstanceId.equals(jsonTaskInstanceId) 
				&& this._config.forceFromStart) {
			/* get max offset in a consumer
			 * getOffsetsBefore(...)
			 * Get a list of valid offsets (up to maxSize) before the given time. 
			 * The result is a list of offsets, in descending order.
			 */
			//this._committedTo = this._consumer.getOffsetsBefore(this._config.kafkaTopic, this._id.getPartitionId(), this._config.startOffset, 1)[0];
            // TODO mod by zqh
            this._committedTo = KafkaUtil.getOffsetBefore(_consumer, this._config.kafkaTopic, this._id.getPartitionId(), this._config.startOffset);

			LOG.info("using start offsetTime to set last commit offset.");
		//force offset to -2 to get msg from kafka first offset
		}else if(jsonTaskInstanceId == null || jsonOffset == null) {
			//this._committedTo = this._consumer.getOffsetsBefore(this._config.kafkaTopic, this._id.getPartitionId(), -2,1)[0];  //OffsetRequest.LatestTime(),
            this._committedTo = KafkaUtil.getOffsetBefore(_consumer, this._config.kafkaTopic, this._id.getPartitionId(), kafka.api.OffsetRequest.EarliestTime());

			LOG.info("using last commit offset id. -1");
		//offset get from zookeeper
		}else {
			this._committedTo = jsonOffset;
			LOG.info("using start offset from zookeeper :" + this._committedTo);
		}
		//record current offset has been emitted
		this._emittedToOffset = this._committedTo;
	}
	
	private String commitedPath() {
		return this._config.zkRoot 
				+ "/" 
				+ _config.kafkaGroupId 
				+ "/" 
				+ this._id;
	}
	
	private void fill() throws IOException{
		long startTime = System.nanoTime();
		ByteBufferMessageSet msgs = null;

		try {
			//msgs = this._consumer.fetch(new FetchRequest(this._config.kafkaTopic, this._id.getPartitionId(), this._emittedToOffset, this._config.fetchSizeBytes));

            FetchRequest req = new FetchRequestBuilder().clientId(KafkaUtil.clientName(_config.kafkaTopic, _id.getPartitionId()))
                    .addFetch(this._config.kafkaTopic, this._id.getPartitionId(), this._emittedToOffset, this._config.fetchSizeBytes).build();
            FetchResponse fetchResponse = _consumer.fetch(req);
            msgs = fetchResponse.messageSet(_config.kafkaTopic, _id.getPartitionId());

            /*
			long endTime = System.nanoTime();
			long millis = (endTime - startTime)/1000000;*/
		
			//int numMessages = msgs.underlying().size();
			int numMessages = msgs.sizeInBytes();
			if(numMessages > 0) {
				LOG.info("Fetched " 
					+ numMessages 
					+ " messages from kafka " 
					+ this._consumer.host() 
					+ ":" + this._id.getPartitionId());
			}
			/*
			 * update offset already fetched
			 */
			for(MessageAndOffset msg : msgs) {
				this._pendingOffsets.add(this._emittedToOffset);
				this._waitingToEmit.add(new MessageAndRealOffset(msg.message(), this._emittedToOffset));
				this._emittedToOffset = msg.offset();	
			}
			if(numMessages > 0) {
				LOG.info("Added " + numMessages + " messages from kafka " + this._consumer.host() + ":" + this._id.getPartitionId() + " to internal buffers");
			}
		//Error handling kafka with detecting error codes
		}catch (OffsetOutOfRangeException _) {
//			if(msgs.getErrorCode() != ErrorMapping.NoError()) {
//				long[] offsets = this._consumer.getOffsetsBefore(
//						this._config.kafkaTopic,
//						this._id.getPartitionId(),
//						this._config.startOffset,
//						1);
//				if (offsets != null && offsets.length > 0) {
//					this._emittedToOffset = offsets[0];
//					ErrorMapping.maybeThrowException(msgs.getErrorCode());
//				}
//			}
		}
	}
	
	
	/*
	 * fill kafka message and return Emit state
	 */
	public EmitState next(SpoutOutputCollector collector) throws UnsupportedEncodingException {
		//emit all kafka messages to bolt and fill again
		if(this._waitingToEmit.isEmpty()) {
			try{
				fill();
			}catch(IOException ex) {
				LOG.error("Unable to communicate with a kafka partition:", ex);
				return EmitState.NO_EMITTED;
			}
		}
		//have messages and pull one and send all
		while(true) {
			MessageAndRealOffset toEmitMsg = this._waitingToEmit.pollFirst();
			//no messages has been acquired
			if(null == toEmitMsg) {
				return EmitState.NO_EMITTED;
			}
			
			Iterable<List<Object>> tups = this._config.scheme.deserialize(Utils.toByteArray(toEmitMsg.getMsg().payload()));
			if(null != tups) {
				for(List<Object> tup: tups) {
					//LOG.info("@@@@@@@@@@@@@@@@@@@@@@@storm-emitting" + this._id.toString());
					collector.emit(tup, new KafkaMessageId(this._id, toEmitMsg.getOffset()));
				}
				break;
			} else {
				//delete this message's offset, this message is null
				ack(toEmitMsg.getOffset());
			}
		}
		if(!this._waitingToEmit.isEmpty()) {
			return EmitState.EMITTED_MORE_LEFT;
		} else {
			return EmitState.EMITTED_END;
		}
	} 
	
	public void ack(Long offset) {
		_pendingOffsets.remove(offset);
	}
	
	public void fail(Long offset) {
		if(this._emittedToOffset > offset) {
			this._emittedToOffset = offset;
			this._pendingOffsets.tailSet(offset).clear();
		}
	}
	
	public void commit() {
		LOG.info("Committing offset for " + this._id);
		long commitedToOffset = 0;
		if(this._pendingOffsets.isEmpty()) {
			commitedToOffset = this._emittedToOffset;
		} else {
			//get latest offset ready to be send to bolt and commit to zk
			commitedToOffset = this._pendingOffsets.first();
		}
		if(commitedToOffset != this._committedTo) {
			LOG.info("Writing committed offset to zk: " + commitedToOffset);
			//write zk
			Map<Object, Object > data = (Map<Object, Object>)ImmutableMap.builder()
					.put("topology", ImmutableMap.of("id", this._taskInstanceId,
													 "name", this._stormConf.get(Config.TOPOLOGY_NAME)))
					.put("offset", commitedToOffset)
					.put("partition", this._id.getPartitionId())
					.put("broker", ImmutableMap.of("host", this._id.getHostPort().getHost(),
												   "port", this._id.getHostPort().getPort()))
					.put("topic", this._config.kafkaTopic)
					.build();
			this._zkState.writeJson(this.commitedPath(), data);
			LOG.info("Wrote commited offset to zk: " + commitedToOffset);
			this._committedTo = commitedToOffset;
		}
	}
	
	public long lastCommittedOffset() {
		return this._committedTo;
	}
	
	public long lastFetchedOffset() {
		if(this._pendingOffsets.isEmpty()) {
			return this._emittedToOffset;
		} else {
			return this._pendingOffsets.first();
		}
	}
	
	public void close() {
		this._connections.unregister(_id);
	}
	
	private DynamicPartitionConnections _connections;
	private ZkState _zkState;
	private Map _stormConf;
	private SpoutConfigParser _config;
	private GlobalPartitionId _id;
	private String _taskInstanceId;
	SimpleConsumer _consumer;
	//record commit offset
	long _committedTo;
	//record offset has already emitted
	long _emittedToOffset;
	//record msg ready to emit to bolt
	LinkedList<MessageAndRealOffset> _waitingToEmit = new LinkedList<MessageAndRealOffset>();
	//set to record all the commited offsets
	SortedSet<Long> _pendingOffsets = new TreeSet<Long>(); 
}
