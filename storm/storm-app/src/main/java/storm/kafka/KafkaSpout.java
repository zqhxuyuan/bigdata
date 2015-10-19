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
import backtype.storm.metric.api.IMetric;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import kafka.message.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.kafka.PartitionManager.KafkaMessageId;

import java.util.*;

// TODO: need to add blacklisting
// TODO: need to make a best effort to not re-emit messages if don't have to
public class KafkaSpout extends BaseRichSpout {
    public static final Logger LOG = LoggerFactory.getLogger(KafkaSpout.class);

    //消息正文和消息在Broker里的分区日志中的offset
    public static class MessageAndRealOffset {
        public Message msg;
        public long offset;

        public MessageAndRealOffset(Message msg, long offset) {
            this.msg = msg;
            this.offset = offset;
        }
    }

    //Storm的Spout发射Tuple的状态
    static enum EmitState {
        EMITTED_MORE_LEFT,
        EMITTED_END,
        NO_EMITTED
    }

    String _uuid = UUID.randomUUID().toString();
    SpoutConfig _spoutConfig;
    SpoutOutputCollector _collector;
    PartitionCoordinator _coordinator;
    DynamicPartitionConnections _connections;
    ZkState _state;

    long _lastUpdateMs = 0;

    int _currPartitionIndex = 0;

    public KafkaSpout(SpoutConfig spoutConf) {
        _spoutConfig = spoutConf;
    }

    @Override
    public void open(Map conf, final TopologyContext context, final SpoutOutputCollector collector) {
        _collector = collector;

        Map stateConf = new HashMap(conf);
        List<String> zkServers = _spoutConfig.zkServers;
        if (zkServers == null) {
            zkServers = (List<String>) conf.get(Config.STORM_ZOOKEEPER_SERVERS);
        }
        Integer zkPort = _spoutConfig.zkPort;
        if (zkPort == null) {
            zkPort = ((Number) conf.get(Config.STORM_ZOOKEEPER_PORT)).intValue();
        }
        stateConf.put(Config.TRANSACTIONAL_ZOOKEEPER_SERVERS, zkServers);
        stateConf.put(Config.TRANSACTIONAL_ZOOKEEPER_PORT, zkPort);
        stateConf.put(Config.TRANSACTIONAL_ZOOKEEPER_ROOT, _spoutConfig.zkRoot);
        _state = new ZkState(stateConf);

        //动态的分区连接信息. 因为Spout是Consumer消费者客户端级别的, 所以连接信息表示这个客户端的所有消费连接.
        //一个消费者客户端可以消费多个Broker, 并且可以消费一个Broker里的多个日志分区
        //但是一个Broker里的一个日志分区只能被一个消费者消费, 不能被多个消费者消费! Consumer[1] --> [*] Broker.Partition
        _connections = new DynamicPartitionConnections(_spoutConfig, KafkaUtils.makeBrokerReader(conf, _spoutConfig));

        // using TransactionalState like this is a hack
        //Spout一共有多少个任务. 在Topology中可以指定spout的并行度和任务数.
        int totalTasks = context.getComponentTasks(context.getThisComponentId()).size();
        if (_spoutConfig.hosts instanceof StaticHosts) {
            //componentToTasks记录了component和task-id的映射关系. 即一个component可以分成多个task
            //context是一次任务执行的上下文信息, getThisTaskIndex表示获取本次任务执行的task-id编号
            //因为一个task可以消费kafka中的多个日志分区, 而一个task是由一个consumer控制的. 即consumer会发起task处理kafka中的分区日志.
            _coordinator = new StaticCoordinator(_connections, conf, _spoutConfig, _state, context.getThisTaskIndex(), totalTasks, _uuid);
        } else {
            //client->consumer->task->partitions
            _coordinator = new ZkCoordinator(_connections, conf, _spoutConfig, _state, context.getThisTaskIndex(), totalTasks, _uuid);
        }

        //反映出每个partition的消费情况
        context.registerMetric("kafkaOffset", new IMetric() {
            KafkaUtils.KafkaOffsetMetric _kafkaOffsetMetric = new KafkaUtils.KafkaOffsetMetric(_spoutConfig.topic, _connections);

            @Override
            public Object getValueAndReset() {
                List<PartitionManager> pms = _coordinator.getMyManagedPartitions();
                Set<Partition> latestPartitions = new HashSet();
                for (PartitionManager pm : pms) {
                    latestPartitions.add(pm.getPartition());
                }
                //根据最新的partition信息删除metric中已经不存在的partition的统计信息
                _kafkaOffsetMetric.refreshPartitions(latestPartitions);
                //更新metric中每个partition的已经完成的offset
                for (PartitionManager pm : pms) {
                    _kafkaOffsetMetric.setLatestEmittedOffset(pm.getPartition(), pm.lastCompletedOffset());
                }
                return _kafkaOffsetMetric.getValueAndReset();
            }
        }, _spoutConfig.metricsTimeBucketSizeInSecs);

        //反映出从Kafka fetch数据的情况
        context.registerMetric("kafkaPartition", new IMetric() {
            @Override
            public Object getValueAndReset() {
                List<PartitionManager> pms = _coordinator.getMyManagedPartitions();
                Map concatMetricsDataMaps = new HashMap();
                for (PartitionManager pm : pms) {
                    concatMetricsDataMaps.putAll(pm.getMetricsDataMap());
                }
                return concatMetricsDataMaps;
            }
        }, _spoutConfig.metricsTimeBucketSizeInSecs);
    }

    @Override
    public void close() {
        _state.close();
    }

    @Override
    public void nextTuple() {
        //获取当前Task的所有Partition. ZkCoordinator中的taskIndex就是指Spout的当前Task.
        //虽然一个Partition只能被一个Spout的Task处理. 但是一个Spout的Task可以处理多个Partition.
        List<PartitionManager> managers = _coordinator.getMyManagedPartitions();

        //其实只要从一个partition读成功一次
        //之所以要for，是当EmitState.NO_EMITTED时，需要遍历后面的partition以保证读成功一次
        for (int i = 0; i < managers.size(); i++) {
            try {
                // in case the number of managers decreased. _currPartitionIndex初始为0，每次依次读一个
                _currPartitionIndex = _currPartitionIndex % managers.size();
                //发射Tuple
                EmitState state = managers.get(_currPartitionIndex).next(_collector);
                //当EMITTED_MORE_LEFT时，还有数据，可以继续读，不需要+1
                if (state != EmitState.EMITTED_MORE_LEFT) {
                    _currPartitionIndex = (_currPartitionIndex + 1) % managers.size();
                }
                //当EmitState.NO_EMITTED时，表明partition的数据已经读完，也就是没有读到数据，所以不能break
                if (state != EmitState.NO_EMITTED) {
                    break;
                }
            } catch (FailedFetchException e) {
                LOG.warn("Fetch failed", e);
                _coordinator.refresh();
            }
        }

        //成功处理完完当前Tuple(已经ack了), 定期提交
        long now = System.currentTimeMillis();
        if ((now - _lastUpdateMs) > _spoutConfig.stateUpdateIntervalMs) {
            commit();
        }
    }

    @Override
    public void ack(Object msgId) {
        KafkaMessageId id = (KafkaMessageId) msgId;
        PartitionManager m = _coordinator.getManager(id.partition);
        if (m != null) {
            m.ack(id.offset);
        }
    }

    @Override
    public void fail(Object msgId) {
        KafkaMessageId id = (KafkaMessageId) msgId;
        PartitionManager m = _coordinator.getManager(id.partition);
        if (m != null) {
            m.fail(id.offset);
        }
    }

    @Override
    public void deactivate() {
        commit();
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(_spoutConfig.scheme.getOutputFields());
    }

    private void commit() {
        _lastUpdateMs = System.currentTimeMillis();
        for (PartitionManager manager : _coordinator.getMyManagedPartitions()) {
            manager.commit();
        }
    }

}
