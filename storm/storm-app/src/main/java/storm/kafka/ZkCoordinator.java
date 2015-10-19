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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.kafka.trident.GlobalPartitionInformation;

import java.util.*;

import static storm.kafka.KafkaUtils.taskId;

/**
 * 一个客户端/消费者, 发起一个Spout任务task要消费多个kafka中的分区日志
 *
 * 看看_coordinator 是干嘛的？
 * 这很关键，因为我们一般都会开多个并发的kafkaspout，类似于high-level中的consumer group，如何保证这些并发的线程不冲突？
 * 使用和highlevel一样的思路，一个partition只会有一个spout消费，这样就避免处理麻烦的访问互斥问题(kafka做访问互斥很麻烦，试着想想)
 * 是根据当前spout的task数和partition数来分配，task和partitioin的对应关系的，并且为每个partition建立PartitionManager
 *
 * 并发的Spout指的是Spout的任务数, 即一个Spout可以有多个Task.
 * kafka中的一个Partition只会被Spout的一个Task处理.
 */
public class ZkCoordinator implements PartitionCoordinator {
    public static final Logger LOG = LoggerFactory.getLogger(ZkCoordinator.class);

    SpoutConfig _spoutConfig;
    int _taskIndex;                             //当前要处理的任务
    int _totalTasks;                            //spout所有的任务数
    String _topologyInstanceId;
    Map<Partition, PartitionManager> _managers = new HashMap();  //每个要消费的分区都对应一个分区管理器
    List<PartitionManager> _cachedList;         //所有的分区管理器
    Long _lastRefreshTime = null;
    int _refreshFreqMs;
    DynamicPartitionConnections _connections;   //连接信息代表了客户端向Broker注册要消费哪些分区
    DynamicBrokersReader _reader;               //读取zk上的Broker和分区信息
    ZkState _state;
    Map _stormConf;

    public ZkCoordinator(DynamicPartitionConnections connections, Map stormConf, SpoutConfig spoutConfig, ZkState state, int taskIndex, int totalTasks, String topologyInstanceId) {
        this(connections, stormConf, spoutConfig, state, taskIndex, totalTasks, topologyInstanceId, buildReader(stormConf, spoutConfig));
    }

    public ZkCoordinator(DynamicPartitionConnections connections, Map stormConf, SpoutConfig spoutConfig, ZkState state, int taskIndex, int totalTasks, String topologyInstanceId, DynamicBrokersReader reader) {
        _spoutConfig = spoutConfig;
        _connections = connections;
        _taskIndex = taskIndex;
        _totalTasks = totalTasks;
        _topologyInstanceId = topologyInstanceId;
        _stormConf = stormConf;
        _state = state;
        ZkHosts brokerConf = (ZkHosts) spoutConfig.hosts;
        _refreshFreqMs = brokerConf.refreshFreqSecs * 1000;
        _reader = reader;
    }

    private static DynamicBrokersReader buildReader(Map stormConf, SpoutConfig spoutConfig) {
        ZkHosts hosts = (ZkHosts) spoutConfig.hosts;
        return new DynamicBrokersReader(stormConf, hosts.brokerZkStr, hosts.brokerZkPath, spoutConfig.topic);
    }

    @Override
    public List<PartitionManager> getMyManagedPartitions() {
        if (_lastRefreshTime == null || (System.currentTimeMillis() - _lastRefreshTime) > _refreshFreqMs) {
            refresh();
            _lastRefreshTime = System.currentTimeMillis();
        }
        return _cachedList;
    }

    @Override
    public void refresh() {
        try {
            LOG.info(taskId(_taskIndex, _totalTasks) + "Refreshing partition manager connections");
            GlobalPartitionInformation brokerInfo = _reader.getBrokerInfo();

            //计算当前任务task所管理的所有分区partitions
            List<Partition> mine = KafkaUtils.calculatePartitionsForTask(brokerInfo, _totalTasks, _taskIndex);

            //已经存在的:a,b,c
            Set<Partition> curr = _managers.keySet();
            //本次分配的:a,c,d,e, 可能包含已经存在的:a,c
            Set<Partition> newPartitions = new HashSet<Partition>(mine);
            //新分配的:[a,c,d,e]-[a,c]=d,e
            newPartitions.removeAll(curr);

            //需要删除的.[a,b,c]-[a,c,d,e]=[b]
            //已经存在
            Set<Partition> deletedPartitions = new HashSet<Partition>(curr);
            //但是在本次分配中没有了,则要从已经存在的里面删除掉
            deletedPartitions.removeAll(mine);

            LOG.info(taskId(_taskIndex, _totalTasks) + "Deleted partition managers: " + deletedPartitions.toString());
            for (Partition id : deletedPartitions) {
                PartitionManager man = _managers.remove(id);
                man.close();
            }

            LOG.info(taskId(_taskIndex, _totalTasks) + "New partition managers: " + newPartitions.toString());
            for (Partition id : newPartitions) {
                PartitionManager man = new PartitionManager(_connections, _topologyInstanceId, _state, _stormConf, _spoutConfig, id);
                //每个分区对应一个分区管理器
                _managers.put(id, man);
            }

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        _cachedList = new ArrayList<PartitionManager>(_managers.values());
        LOG.info(taskId(_taskIndex, _totalTasks) + "Finished refreshing");
    }

    @Override
    public PartitionManager getManager(Partition partition) {
        return _managers.get(partition);
    }
}
