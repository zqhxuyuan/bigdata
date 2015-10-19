/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package storm.kafka;

import backtype.storm.Config;
import backtype.storm.metric.api.CombinedMetric;
import backtype.storm.metric.api.CountMetric;
import backtype.storm.metric.api.MeanReducer;
import backtype.storm.metric.api.ReducedMetric;
import backtype.storm.spout.SpoutOutputCollector;
import com.google.common.collect.ImmutableMap;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.javaapi.message.ByteBufferMessageSet;
import kafka.message.MessageAndOffset;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.kafka.KafkaSpout.EmitState;
import storm.kafka.KafkaSpout.MessageAndRealOffset;
import storm.kafka.trident.MaxMetric;

import java.util.*;

/**
 * 分区管理器: 用于管理一个partiton的读取状态
 *
 * kafka对于一个partition，一定是从offset从小到大按顺序读的，并且这里为了保证不读丢数据，会定期的将当前状态即offset写入zk
 *
 */
public class PartitionManager {
    public static final Logger LOG = LoggerFactory.getLogger(PartitionManager.class);

    private final CombinedMetric _fetchAPILatencyMax;
    private final ReducedMetric _fetchAPILatencyMean;
    private final CountMetric _fetchAPICallCount;
    private final CountMetric _fetchAPIMessageCount;

    //从kafka读到的offset. emitted表示已经发射过了
    Long _emittedToOffset;
    // _pending key = Kafka offset, value = time at which the message was first submitted to the topology
    private SortedMap<Long,Long> _pending = new TreeMap<Long,Long>();

    //已经写入zk的offset,需要定期将lastCompletedOffset，写入zk，否则crash后，我们不知道上次读到哪儿了.
    //所以_committedTo <= lastCompletedOffset. 因为记录到zk中的committedTo发生的时间比lastCompletedOffset要晚一些
    Long _committedTo;
    //从kafka读到的messages会放入_waitingToEmit[等待发射的消息]，放入这个list，我们就认为一定会被emit发射出去
    LinkedList<MessageAndRealOffset> _waitingToEmit = new LinkedList<MessageAndRealOffset>();
    // retryRecords key = Kafka offset, value = retry info for the given message
    private final FailedMsgRetryManager _failedMsgRetryManager;

    Partition _partition;           //要管理的分区
    SimpleConsumer _consumer;       //分区所属的消费者
    SpoutConfig _spoutConfig;
    String _topologyInstanceId;
    DynamicPartitionConnections _connections;
    ZkState _state;
    Map _stormConf;
    long numberFailed, numberAcked;

    //初始化: 关键就是注册partition，然后初始化offset，以知道从哪里开始读
    public PartitionManager(DynamicPartitionConnections connections, String topologyInstanceId, ZkState state, Map stormConf, SpoutConfig spoutConfig, Partition id) {
        _partition = id;
        _connections = connections;
        _spoutConfig = spoutConfig;
        _topologyInstanceId = topologyInstanceId;
        //一个分区对应一个分区管理器,一个消费者消费一个分区. 所以一个分区管理器能够得到消费这个分区的消费者
        _consumer = connections.register(id.host, id.partition);
        _state = state;
        _stormConf = stormConf;
        numberAcked = numberFailed = 0;
        _failedMsgRetryManager = new ExponentialBackoffMsgRetryManager(_spoutConfig.retryInitialDelayMs, _spoutConfig.retryDelayMultiplier, _spoutConfig.retryDelayMaxMs);

        //读取zk中的topology信息和消费者读取到的位置
        String jsonTopologyId = null;
        Long jsonOffset = null;
        String path = committedPath();
        try {
            Map<Object, Object> json = _state.readJSON(path);
            LOG.info("Read partition information from: " + path +  "  --> " + json );
            if (json != null) {
                jsonTopologyId = (String) ((Map<Object, Object>) json.get("topology")).get("id");
                jsonOffset = (Long) json.get("offset");  // 从zk中读出commited offset
            }
        } catch (Throwable e) {
            LOG.warn("Error reading and/or parsing at ZkNode: " + path, e);
        }

        //消费者在当前分区已经读取到的位置
        Long currentOffset = KafkaUtils.getOffset(_consumer, spoutConfig.topic, id.partition, spoutConfig);

        // zk中没有记录，那么根据spoutConfig.startOffsetTime设置offset，Earliest或Latest
        /**
         我们在读取kafka时，
         首先是关心，每个partition的读取状况，这个通过取得KafkaOffset Metrics就可以知道
         再者，我们需要replay数据，使用high-level接口的时候可以通过系统提供的工具，这里如何搞？

         看下下面的代码，
         第一个if，是从配置文件里面没有读到配置的情况
         第二个else if，当topologyInstanceId发生变化时，并且forceFromStart为true时，就会取startOffsetTime指定的offset(Latest或Earliest)
         这个topologyInstanceId, 每次KafkaSpout对象生成的时候随机产生，
         String _uuid = UUID.randomUUID().toString();
         Spout对象是在topology提交时，在client端生成一次的，所以如果topology停止，再重新启动，这个id一定会发生变化

         所以应该是只需要把forceFromStart设为true，再重启topology，就可以实现replay
         */
        if (jsonTopologyId == null || jsonOffset == null) { // failed to parse JSON?
            _committedTo = currentOffset;
            LOG.info("No partition information found, using configuration to determine offset");
        } else if (!topologyInstanceId.equals(jsonTopologyId) && spoutConfig.forceFromStart) {
            _committedTo = KafkaUtils.getOffset(_consumer, spoutConfig.topic, id.partition, spoutConfig.startOffsetTime);
            LOG.info("Topology change detected and reset from start forced, using configuration to determine offset");
        } else {
            //jsonOffset是读取zk中的offset, _committedTo表示写到zk中的offset, 所以在初始化时设置committedTo=jsonOffset
            _committedTo = jsonOffset;
            LOG.info("Read last commit offset from zookeeper: " + _committedTo + "; old topology_id: " + jsonTopologyId + " - new topology_id: " + topologyInstanceId );
        }

        //currentOffset和_committedTo不相等. currentOffset表示消费者当前消费的位置, 而committedTo表示写到zk中的消费位置.
        //committedTo落后于currentOffset. 如果committedTo落后于currentOffset太多了(maxOffsetBehind)
        if (currentOffset - _committedTo > spoutConfig.maxOffsetBehind || _committedTo <= 0) {
            LOG.info("Last commit offset from zookeeper: " + _committedTo);  //最近一次写到(commit)zk中的offset
            Long lastCommittedOffset = _committedTo;
            _committedTo = currentOffset;
            LOG.info("Commit offset " + lastCommittedOffset + " is more than " +
                    spoutConfig.maxOffsetBehind + " behind latest offset " + currentOffset + ", resetting to startOffsetTime=" + spoutConfig.startOffsetTime);
        }

        LOG.info("Starting Kafka " + _consumer.host() + ":" + id.partition + " from offset " + _committedTo);
        _emittedToOffset = _committedTo;  // 初始化时，中间状态都是一致的

        _fetchAPILatencyMax = new CombinedMetric(new MaxMetric());
        _fetchAPILatencyMean = new ReducedMetric(new MeanReducer());
        _fetchAPICallCount = new CountMetric();
        _fetchAPIMessageCount = new CountMetric();
    }

    public Map getMetricsDataMap() {
        Map ret = new HashMap();
        ret.put(_partition + "/fetchAPILatencyMax", _fetchAPILatencyMax.getValueAndReset());
        ret.put(_partition + "/fetchAPILatencyMean", _fetchAPILatencyMean.getValueAndReset());
        ret.put(_partition + "/fetchAPICallCount", _fetchAPICallCount.getValueAndReset());
        ret.put(_partition + "/fetchAPIMessageCount", _fetchAPIMessageCount.getValueAndReset());
        return ret;
    }

    //获取kafka中的消息,填充到等待发送的队列waitingToEmit和正在处理的队列pending,并更新_emittedToOffset
    private void fill() {
        long start = System.nanoTime();
        Long offset;

        // Are there failed tuples? If so, fetch those first. 如果tuple处理失败, 说明offset位置的消息没有被成功处理, 则要重新处理
        offset = this._failedMsgRetryManager.nextFailedMessageToRetry();
        final boolean processingNewTuples = (offset == null);
        if (processingNewTuples) {
            //_emittedToOffset表示从kafka中读取的[最新消息的]offset
            offset = _emittedToOffset;
        }

        //从kafka中读到数据ByteBufferMessageSet: 本次要处理的消息. 注意offset是当前读取的第一条消息的offset, fetchMessages可以获取多条消息, 都是在offset之后
        ByteBufferMessageSet msgs = null;
        try {
            //获取从offset开始的多条消息
            msgs = KafkaUtils.fetchMessages(_spoutConfig, _consumer, _partition, offset);
        } catch (TopicOffsetOutOfRangeException e) {
            _emittedToOffset = KafkaUtils.getOffset(_consumer, _spoutConfig.topic, _partition.partition, kafka.api.OffsetRequest.EarliestTime());
            LOG.warn("Using new offset: {}", _emittedToOffset);
            // fetch failed, so don't update the metrics
            return;
        }
        long end = System.nanoTime();
        long millis = (end - start) / 1000000;
        _fetchAPILatencyMax.update(millis);
        _fetchAPILatencyMean.update(millis);
        _fetchAPICallCount.incr();
        if (msgs != null) {
            int numMessages = 0;

            //一次可以处理多条消息.
            for (MessageAndOffset msg : msgs) {
                //每条消息的offset
                final Long cur_offset = msg.offset();
                if (cur_offset < offset) {
                    // Skip any old offsets. 当前读取的最新消息的offset, msgs里有多条消息,每条消息的offset都应该比offset大!
                    continue;
                }
                //处理新消息,或者允许重试
                if (processingNewTuples || this._failedMsgRetryManager.shouldRetryMsg(cur_offset)) {
                    numMessages += 1;

                    //在处理一条新的消息时, 会同时往waitingToEmit[准备发射]和pending[正在处理]中添加数据
                    //准备发射是发生在Spout任务工作之前, pending表示Storm的topology正在处理这条消息


                    //把没完成的offset放到pending. 如果已经pending中已经存在offset,则不放入.
                    if (!_pending.containsKey(cur_offset)) {
                        //pending里存放的是offset和第一次从队列中获取的时间
                        _pending.put(cur_offset, System.currentTimeMillis());
                    }

                    //把需要emit的msg:MessageAndRealOffset，放到_waitingToEmit
                    _waitingToEmit.add(new MessageAndRealOffset(msg.message(), cur_offset));

                    //更新emittedToOffset
                    _emittedToOffset = Math.max(msg.nextOffset(), _emittedToOffset);

                    if (_failedMsgRetryManager.shouldRetryMsg(cur_offset)) {
                        this._failedMsgRetryManager.retryStarted(cur_offset);
                    }
                }
                //如果不是新的消息或者重试次数超过阈值, 则不进行任何处理
            }
            _fetchAPIMessageCount.incrBy(numMessages);
        }
    }

    //returns false if it's reached the end of current batch
    //从_waitingToEmit中取到msg，转换成tuple，然后通过collector.emit发出去
    public EmitState next(SpoutOutputCollector collector) {
        //如果等待发射的为空, 则从kafka中再去取消息来处理. 如果不为空, 说明上次获取的消息还没处理完
        if (_waitingToEmit.isEmpty()) {
            fill();
        }
        while (true) {
            //取出队列中的第一条消息. 因为按照offset升序进入队列,处理时也从最小的offset开始.即FIFO
            MessageAndRealOffset toEmit = _waitingToEmit.pollFirst();
            if (toEmit == null) {
                return EmitState.NO_EMITTED;
            }
            Iterable<List<Object>> tups = KafkaUtils.generateTuples(_spoutConfig, toEmit.msg);
            if (tups != null) {
                for (List<Object> tup : tups) {
                    //Storm的component发射tuple时, 可以指定消息id. 表示这个tuple在全局流中的唯一id
                    collector.emit(tup, new KafkaMessageId(_partition, toEmit.offset));
                }
                break;
            } else {
                //tuple为空, 直接ack
                ack(toEmit.offset);
            }
        }
        if (!_waitingToEmit.isEmpty()) {
            return EmitState.EMITTED_MORE_LEFT;
        } else {
            return EmitState.EMITTED_END;
        }
    }

    //消息从kafka中取出之后, 首先都同时加入到waitingToEmit和pending中
    //当spout工作时, 从waitingToEmit中移除队首消息
    //整个消息都处理完比后,调用ack, 从pending中移除

    /**
     * fill时往队列中添加数据, 成功处理则要从队列中移除
     * @param offset
     */
    public void ack(Long offset) {
        if (!_pending.isEmpty() && _pending.firstKey() < offset - _spoutConfig.maxOffsetBehind) {
            // Too many things pending!
            _pending.headMap(offset - _spoutConfig.maxOffsetBehind).clear();
        }
        //已经处理完了, 从正在处理的队列中删除. 不需要从waitingToEmit中移除, 因为poll会删除!
        _pending.remove(offset);
        this._failedMsgRetryManager.acked(offset);
        numberAcked++;
    }

    public void fail(Long offset) {
        if (offset < _emittedToOffset - _spoutConfig.maxOffsetBehind) {
            LOG.info(
                    "Skipping failed tuple at offset=" + offset +
                            " because it's more than maxOffsetBehind=" + _spoutConfig.maxOffsetBehind +
                            " behind _emittedToOffset=" + _emittedToOffset
            );
        } else {
            LOG.debug("failing at offset=" + offset + " with _pending.size()=" + _pending.size() + " pending and _emittedToOffset=" + _emittedToOffset);
            numberFailed++;
            if (numberAcked == 0 && numberFailed > _spoutConfig.maxOffsetBehind) {
                throw new RuntimeException("Too many tuple failures");
            }

            this._failedMsgRetryManager.failed(offset);
        }
    }

    /**
     * 向zk中写入消费者的消费位置
     */
    public void commit() {
        long lastCompletedOffset = lastCompletedOffset();
        if (_committedTo != lastCompletedOffset) {
            LOG.debug("Writing last completed offset (" + lastCompletedOffset + ") to ZK for " + _partition + " for topology: " + _topologyInstanceId);
            Map<Object, Object> data = (Map<Object, Object>) ImmutableMap.builder()
                    .put("topology", ImmutableMap.of("id", _topologyInstanceId, "name", _stormConf.get(Config.TOPOLOGY_NAME)))
                    .put("offset", lastCompletedOffset)
                    .put("partition", _partition.partition)
                    .put("broker", ImmutableMap.of("host", _partition.host.host, "port", _partition.host.port))
                    .put("topic", _spoutConfig.topic).build();
            _state.writeJSON(committedPath(), data);

            //成功写入zk后, 更新committedTo为已经写入的offset
            _committedTo = lastCompletedOffset;
            LOG.debug("Wrote last completed offset (" + lastCompletedOffset + ") to ZK for " + _partition + " for topology: " + _topologyInstanceId);
        } else {
            LOG.debug("No new offset for " + _partition + " for topology: " + _topologyInstanceId);
        }
    }

    private String committedPath() {
        return _spoutConfig.zkRoot + "/" + _spoutConfig.id + "/" + _partition.getId();
    }

    /**
     * 由于message是要在storm里面处理的，其中是可能fail的，所以正在处理的offset是缓存在_pending中的
     * 如果_pending为空，那么lastCompletedOffset=_emittedToOffset 没有等待处理的消息了
     * 如果_pending不为空，那么lastCompletedOffset为pending list里面第一个offset，因为后面都还在等待ack
     * @return 已经成功处理的offset. 即kafka中的消息成功被storm的spout emit出去,才算成功处理了一个消息.
     */
    public long lastCompletedOffset() {
        if (_pending.isEmpty()) {
            return _emittedToOffset;    //从kafka读取到的offset就是已经成功处理的offset
        } else {
            return _pending.firstKey();
        }
    }

    public Partition getPartition() {
        return _partition;
    }

    public void close() {
        _connections.unregister(_partition.host, _partition.partition);
    }

    //Kafka的消息编号. 用于Storm发射tuple的stream-id.
    static class KafkaMessageId {
        public Partition partition;
        public long offset;

        public KafkaMessageId(Partition partition, long offset) {
            this.partition = partition;
            this.offset = offset;
        }
    }
}
