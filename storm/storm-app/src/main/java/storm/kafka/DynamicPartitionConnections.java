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

import kafka.javaapi.consumer.SimpleConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.kafka.trident.IBrokerReader;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * 动态分区的连接
 */
public class DynamicPartitionConnections {
    public static final Logger LOG = LoggerFactory.getLogger(DynamicPartitionConnections.class);

    //连接信息. 消费者连接到消息队列中进行消费消息
    static class ConnectionInfo {
        //一个消费者
        SimpleConsumer consumer;
        //可以消费一个主题的多个日志分区. 但是一个日志分区只能被一个消费者消费!
        Set<Integer> partitions = new HashSet();

        public ConnectionInfo(SimpleConsumer consumer) {
            this.consumer = consumer;
        }
    }

    //因为我们是针对一个主题的. 一个消费者可以消费一个主题的多个分区
    Map<Broker, ConnectionInfo> _connections = new HashMap();
    KafkaConfig _config;
    IBrokerReader _reader;

    public DynamicPartitionConnections(KafkaConfig config, IBrokerReader brokerReader) {
        _config = config;
        _reader = brokerReader;
    }

    public SimpleConsumer register(Partition partition) {
        Broker broker = _reader.getCurrentBrokers().getBrokerFor(partition.partition);
        return register(broker, partition.partition);
    }

    /**
     * 当consumer起来时，要注册相应的信息到zookeeper中.
     * 记录consumer要消费哪几个分区. 这里限制了一个分区只能被一个消费者消费.
     *
     * VIP: Kafka的分布式协调要求: 在同一时间，每个topic中的不同分区只能被消费组中的一个消费者消费
     * 多个消费者消费相同的分区时会导致额外的开销（比如要协调哪个消费者消费哪个消息，还有锁及状态的开销）
     * 消费进程只需要在重新负载（重新指定哪些分区分配给哪些消费者消费）时进行一次协调（这种协调不是经常性的，故可以忽略开销）
     * 也就是说在一开始就确定了哪些消费者能够消费哪些分区. 并且在同一时间,不能有不同的消费者消费同一个分区,但是允许同一时间不同的消费者消费不同的分区.
     *
     * 在同一时间，每个topic中的不同分区只能被消费组中的一个消费者消费 这句话的意思:
     * 因为消费组可以有多个消费者. 同一个分区不能分配给同一个消费组的多个消费者, 只能分配给消费组中的一个消费者.
     * 假设系统只有2个消费组, 则分区最多只能分给2个消费者, 即只能分给每个消费组中的一个消费者
     * kafka只在对应的分区提供全局顺序。不对全局消息提供顺序
     * @param host 要向哪个Broker注册消费者
     * @param partition 分区编号
     * @return 发起注册动作的消费者
     */
    public SimpleConsumer register(Broker host, int partition) {
        if (!_connections.containsKey(host)) {
            _connections.put(host, new ConnectionInfo(new SimpleConsumer(host.host, host.port, _config.socketTimeoutMs, _config.bufferSizeBytes, _config.clientId)));
        }
        //如果connections中已经含有了Broker, 则直接将partition加入partitions中
        //也就是说一个Broker只能被一个消费者消费! 假设开始时Broker1被Consumer1消费,会根据Consumer的clientId创建SimpleConsumer
        //当有另外的Consumer2也要消费Broker1中的分区日志时,因为Broker host已经在connections中.不会执行上面的new ConnectionInfo. 也就不会创建Consunmer2
        //所以向同一个Broker host注册的partition日志分区都必须是属于同一个消费者.
        ConnectionInfo info = _connections.get(host);
        info.partitions.add(partition);
        //返回发起注册动作的消费者
        return info.consumer;
    }

    //获取指定的分区是被哪个消费者消费的
    public SimpleConsumer getConnection(Partition partition) {
        ConnectionInfo info = _connections.get(partition.host);
        if (info != null) {
            return info.consumer;
        }
        return null;
    }

    //当消费完一个分区的日志后, 取消注册, 释放连接信息.
    public void unregister(Broker port, int partition) {
        ConnectionInfo info = _connections.get(port);
        info.partitions.remove(partition);
        if (info.partitions.isEmpty()) {
            info.consumer.close();
            _connections.remove(port);
        }
    }

    public void unregister(Partition partition) {
        unregister(partition.host, partition.partition);
    }

    public void clear() {
        for (ConnectionInfo info : _connections.values()) {
            info.consumer.close();
        }
        _connections.clear();
    }
}
