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

import backtype.storm.metric.api.IMetric;
import backtype.storm.utils.Utils;
import com.google.common.base.Preconditions;
import kafka.api.FetchRequest;
import kafka.api.FetchRequestBuilder;
import kafka.api.PartitionOffsetRequestInfo;
import kafka.common.TopicAndPartition;
import kafka.javaapi.FetchResponse;
import kafka.javaapi.OffsetRequest;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.javaapi.message.ByteBufferMessageSet;
import kafka.message.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.kafka.trident.GlobalPartitionInformation;
import storm.kafka.trident.IBrokerReader;
import storm.kafka.trident.StaticBrokerReader;
import storm.kafka.trident.ZkBrokerReader;

import java.io.IOException;
import java.net.ConnectException;
import java.net.SocketTimeoutException;
import java.nio.ByteBuffer;
import java.nio.channels.UnresolvedAddressException;
import java.util.*;


public class KafkaUtils {

    public static final Logger LOG = LoggerFactory.getLogger(KafkaUtils.class);
    private static final int NO_OFFSET = -5;


    public static IBrokerReader makeBrokerReader(Map stormConf, KafkaConfig conf) {
        if (conf.hosts instanceof StaticHosts) {
            return new StaticBrokerReader(((StaticHosts) conf.hosts).getPartitionInformation());
        } else {
            return new ZkBrokerReader(stormConf, conf.topic, (ZkHosts) conf.hosts);
        }
    }

    /**
     * 获取消费者在指定主题的指定分区上的下一个消费位置
     * @param consumer 消费者
     * @param topic 主题
     * @param partition 分区
     * @param config
     * @return 下一个消费位置
     */
    public static long getOffset(SimpleConsumer consumer, String topic, int partition, KafkaConfig config) {
        long startOffsetTime = kafka.api.OffsetRequest.LatestTime();
        if ( config.forceFromStart ) {
            startOffsetTime = config.startOffsetTime;
        }
        return getOffset(consumer, topic, partition, startOffsetTime);
    }

    public static long getOffset(SimpleConsumer consumer, String topic, int partition, long startOffsetTime) {
        //定位到特定主题的特定分区上
        TopicAndPartition topicAndPartition = new TopicAndPartition(topic, partition);

        Map<TopicAndPartition, PartitionOffsetRequestInfo> requestInfo = new HashMap<TopicAndPartition, PartitionOffsetRequestInfo>();
        requestInfo.put(topicAndPartition, new PartitionOffsetRequestInfo(startOffsetTime, 1));

        OffsetRequest request = new OffsetRequest(
                requestInfo, kafka.api.OffsetRequest.CurrentVersion(), consumer.clientId());

        long[] offsets = consumer.getOffsetsBefore(request).offsets(topic, partition);
        if (offsets.length > 0) {
            return offsets[0];
        } else {
            return NO_OFFSET;
        }
    }

    public static class KafkaOffsetMetric implements IMetric {
        Map<Partition, Long> _partitionToOffset = new HashMap<Partition, Long>();
        Set<Partition> _partitions;
        String _topic;
        DynamicPartitionConnections _connections;

        public KafkaOffsetMetric(String topic, DynamicPartitionConnections connections) {
            _topic = topic;
            _connections = connections;
        }

        public void setLatestEmittedOffset(Partition partition, long offset) {
            _partitionToOffset.put(partition, offset);
        }

        @Override
        public Object getValueAndReset() {
            try {
                long totalSpoutLag = 0;
                long totalEarliestTimeOffset = 0;
                long totalLatestTimeOffset = 0;
                long totalLatestEmittedOffset = 0;
                HashMap ret = new HashMap();
                if (_partitions != null && _partitions.size() == _partitionToOffset.size()) {
                    for (Map.Entry<Partition, Long> e : _partitionToOffset.entrySet()) {
                        Partition partition = e.getKey();
                        SimpleConsumer consumer = _connections.getConnection(partition);
                        if (consumer == null) {
                            LOG.warn("partitionToOffset contains partition not found in _connections. Stale partition data?");
                            return null;
                        }
                        long latestTimeOffset = getOffset(consumer, _topic, partition.partition, kafka.api.OffsetRequest.LatestTime());
                        long earliestTimeOffset = getOffset(consumer, _topic, partition.partition, kafka.api.OffsetRequest.EarliestTime());
                        if (latestTimeOffset == KafkaUtils.NO_OFFSET) {
                            LOG.warn("No data found in Kafka Partition " + partition.getId());
                            return null;
                        }
                        long latestEmittedOffset = e.getValue();
                        long spoutLag = latestTimeOffset - latestEmittedOffset;
                        ret.put(_topic + "/" + partition.getId() + "/" + "spoutLag", spoutLag);
                        ret.put(_topic + "/" + partition.getId() + "/" + "earliestTimeOffset", earliestTimeOffset);
                        ret.put(_topic + "/" + partition.getId() + "/" + "latestTimeOffset", latestTimeOffset);
                        ret.put(_topic + "/" + partition.getId() + "/" + "latestEmittedOffset", latestEmittedOffset);
                        totalSpoutLag += spoutLag;
                        totalEarliestTimeOffset += earliestTimeOffset;
                        totalLatestTimeOffset += latestTimeOffset;
                        totalLatestEmittedOffset += latestEmittedOffset;
                    }
                    ret.put(_topic + "/" + "totalSpoutLag", totalSpoutLag);
                    ret.put(_topic + "/" + "totalEarliestTimeOffset", totalEarliestTimeOffset);
                    ret.put(_topic + "/" + "totalLatestTimeOffset", totalLatestTimeOffset);
                    ret.put(_topic + "/" + "totalLatestEmittedOffset", totalLatestEmittedOffset);
                    return ret;
                } else {
                    LOG.info("Metrics Tick: Not enough data to calculate spout lag.");
                }
            } catch (Throwable t) {
                LOG.warn("Metrics Tick: Exception when computing kafkaOffset metric.", t);
            }
            return null;
        }

        public void refreshPartitions(Set<Partition> partitions) {
            _partitions = partitions;
            Iterator<Partition> it = _partitionToOffset.keySet().iterator();
            while (it.hasNext()) {
                if (!partitions.contains(it.next())) {
                    it.remove();
                }
            }
        }
    }

    public static ByteBufferMessageSet fetchMessages(KafkaConfig config, SimpleConsumer consumer, Partition partition, long offset)
            throws TopicOffsetOutOfRangeException, FailedFetchException,RuntimeException {
        ByteBufferMessageSet msgs = null;
        String topic = config.topic;
        int partitionId = partition.partition;

        FetchRequestBuilder builder = new FetchRequestBuilder();
        //Builder链式调用. fetchSizeBytes表示一次要获取多少数据量. 所以从offset开始,一次获取不止一条消息
        FetchRequest fetchRequest = builder.addFetch(topic, partitionId, offset, config.fetchSizeBytes).
                clientId(config.clientId).maxWait(config.fetchMaxWait).build();
        FetchResponse fetchResponse;
        try {
            //消费者传入获取请求,得到获取响应
            fetchResponse = consumer.fetch(fetchRequest);
        } catch (Exception e) {
            if (e instanceof ConnectException ||
                    e instanceof SocketTimeoutException ||
                    e instanceof IOException ||
                    e instanceof UnresolvedAddressException
                    ) {
                LOG.warn("Network error when fetching messages:", e);
                throw new FailedFetchException(e);
            } else {
                throw new RuntimeException(e);
            }
        }
        // 主要处理offset outofrange的case，通过getOffset从earliest或latest读
        if (fetchResponse.hasError()) {
            KafkaError error = KafkaError.getError(fetchResponse.errorCode(topic, partitionId));
            if (error.equals(KafkaError.OFFSET_OUT_OF_RANGE) && config.useStartOffsetTimeIfOffsetOutOfRange) {
                String msg = "Got fetch request with offset out of range: [" + offset + "]";
                LOG.warn(msg);
                throw new TopicOffsetOutOfRangeException(msg);
            } else {
                String message = "Error fetching data from [" + partition + "] for topic [" + topic + "]: [" + error + "]";
                LOG.error(message);
                throw new FailedFetchException(message);
            }
        } else {
            msgs = fetchResponse.messageSet(topic, partitionId);
        }
        return msgs;
    }


    public static Iterable<List<Object>> generateTuples(KafkaConfig kafkaConfig, Message msg) {
        Iterable<List<Object>> tups;
        //消息的负载是字节缓冲区, 转为序列化后的字节数组
        ByteBuffer payload = msg.payload();
        if (payload == null) {
            return null;
        }
        ByteBuffer key = msg.key();
        if (key != null && kafkaConfig.scheme instanceof KeyValueSchemeAsMultiScheme) {
            tups = ((KeyValueSchemeAsMultiScheme) kafkaConfig.scheme).deserializeKeyAndValue(Utils.toByteArray(key), Utils.toByteArray(payload));
        } else {
            tups = kafkaConfig.scheme.deserialize(Utils.toByteArray(payload));
        }
        return tups;
    }

    /**
     * 为task计算分区. 因为日志保存在多个分区里, spout的task要从多个日志分区里获取消息
     * 即一个task负责消费多个日志分区. 可以为一个task分配多个日志分区.
     * @param partitionInformation 全局的分区信息, 记录了kafka的所有日志分区
     * @param totalTasks 总的任务数
     * @param taskIndex 当前的任务编号
     * @return 为任务分配要消费的多个日志分区
     */
    public static List<Partition> calculatePartitionsForTask(GlobalPartitionInformation partitionInformation, int totalTasks, int taskIndex) {
        Preconditions.checkArgument(taskIndex < totalTasks, "task index must be less that total tasks");
        List<Partition> partitions = partitionInformation.getOrderedPartitions();
        //kafka的日志分区数和spout的任务数.
        int numPartitions = partitions.size();
        //如果spout的任务数比日志分区数还要多, 说明有些task会空闲. 一般要一个任务处理多个日志分区, 即totalTasks <= partitions
        if (numPartitions < totalTasks) {
            LOG.warn("there are more tasks than partitions (tasks: " + totalTasks + "; partitions: " + numPartitions + "), some tasks will be idle");
        }

        List<Partition> taskPartitions = new ArrayList<Partition>();
        //假设有10个分区日志,有5个任务. taskIndex=0, 分配日志分区=0,5
        //taskIndex=1, 分配日志分区=1,6. 实际上是顺序为task分配一个日志分区: 每个task分配一个顺序的日志分区
        //如果还有剩余的日志分区没有指定task,再从头开始为所有的task继续顺序分配
        for (int i = taskIndex; i < numPartitions; i += totalTasks) {
            Partition taskPartition = partitions.get(i);
            taskPartitions.add(taskPartition);
        }
        logPartitionMapping(totalTasks, taskIndex, taskPartitions);
        return taskPartitions;
    }

    private static void logPartitionMapping(int totalTasks, int taskIndex, List<Partition> taskPartitions) {
        String taskPrefix = taskId(taskIndex, totalTasks);
        if (taskPartitions.isEmpty()) {
            LOG.warn(taskPrefix + "no partitions assigned");
        } else {
            LOG.info(taskPrefix + "assigned " + taskPartitions);
        }
    }

    public static String taskId(int taskIndex, int totalTasks) {
        return "Task [" + (taskIndex + 1) + "/" + totalTasks + "] ";
    }
}
