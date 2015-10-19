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
package storm.starter.bolt;

import backtype.storm.Config;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import org.apache.log4j.Logger;
import storm.starter.tools.NthLastModifiedTimeTracker;
import storm.starter.tools.SlidingWindowCounter;
import storm.starter.util.TupleHelpers;
import storm.starter.util.WindowConstant;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

public class RollingDistinctCountBolt extends BaseRichBolt {
    private static final long serialVersionUID = 5537727428628598519L;
    private static final Logger LOG = Logger.getLogger(RollingDistinctCountBolt.class);

    private static final int NUM_WINDOW_CHUNKS = 5;
    private static final int DEFAULT_SLIDING_WINDOW_IN_SECONDS = NUM_WINDOW_CHUNKS * 60;
    private static final int DEFAULT_EMIT_FREQUENCY_IN_SECONDS = DEFAULT_SLIDING_WINDOW_IN_SECONDS / NUM_WINDOW_CHUNKS;

    private final SlidingWindowCounter<Object> counter;
    private final int windowLengthInSeconds;
    private final int emitFrequencyInSeconds;
    private OutputCollector collector;
    private NthLastModifiedTimeTracker lastModifiedTracker;

    private String key = "";
    private boolean detail = false; //是否显示详细信息,比如IP关联的账户数为3个, 具体有哪几个账户

    public RollingDistinctCountBolt() {
        this(DEFAULT_SLIDING_WINDOW_IN_SECONDS, DEFAULT_EMIT_FREQUENCY_IN_SECONDS);
    }

    public RollingDistinctCountBolt(int windowLengthInSeconds, int emitFrequencyInSeconds) {
        this.windowLengthInSeconds = windowLengthInSeconds;
        this.emitFrequencyInSeconds = emitFrequencyInSeconds;
        counter = new SlidingWindowCounter<Object>(deriveNumWindowChunksFrom(this.windowLengthInSeconds, this.emitFrequencyInSeconds));
    }

    public RollingDistinctCountBolt(int windowLengthInSeconds, int emitFrequencyInSeconds, String leftField, String rightField) {
        this(windowLengthInSeconds, emitFrequencyInSeconds);
        this.key = leftField + WindowConstant.splitKey + rightField + WindowConstant.splitKey;
    }
    public RollingDistinctCountBolt(int windowLengthInSeconds, int emitFrequencyInSeconds, String leftField, String rightField, boolean detail) {
        this(windowLengthInSeconds, emitFrequencyInSeconds, leftField, rightField);
        this.detail = detail;
    }

    private int deriveNumWindowChunksFrom(int windowLengthInSeconds, int windowUpdateFrequencyInSeconds) {
        return windowLengthInSeconds / windowUpdateFrequencyInSeconds;
    }

    @SuppressWarnings("rawtypes")
    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        lastModifiedTracker = new NthLastModifiedTimeTracker(deriveNumWindowChunksFrom(this.windowLengthInSeconds, this.emitFrequencyInSeconds));
    }

    @Override
    public void execute(Tuple tuple) {
        if (TupleHelpers.isTickTuple(tuple)) {
            LOG.debug("Received tick tuple, triggering emit of current window counts");
            emitCurrentWindowCounts();
        } else {
            countObjAndAck(tuple);
        }
    }

    private void countObjAndAck(Tuple tuple) {
        //获取接收到的对象. 针对WordCount示例来说,Spout只发送了一个字段的Word,如果是多个字段呢?显然这里的Obj需要更改.
        String left = tuple.getString(0);
        String right = tuple.getString(1);

        //为这个对象的计数器+1
        counter.incrementCount(left + WindowConstant.splitKey + right);
        collector.ack(tuple);
    }

    //发送当前窗口的计数
    private void emitCurrentWindowCounts() {
        //对于DistinctCount, Object对象是个Pair. 我们需要知道同一个left有多少个right,因此不需要关心counts的value.
        Map<Object, Long> counts = counter.getCountsThenAdvanceWindow();
        //计算当前窗口实际的时间间隔. 注意这个窗口内的所有对象的counts都是一样的
        int actualWindowLengthInSeconds = lastModifiedTracker.secondsSinceOldestModification();
        lastModifiedTracker.markAsModified();

        Set<String> keySet = (Set)counts.keySet();
        Map<String, Long> distSet = new HashMap<>();
        Map<String, String> distDetail = new HashMap<>();
        for (String p : keySet){
            String[] leftAndRight = p.split(WindowConstant.splitKey);
            String left = leftAndRight[0];
            //TODO 如果想查看主维度, 具体关联了哪几个从维度值呢?. 如果想再知道具体某个从维度的次数,就要用到最开始的counts了.
            String right = leftAndRight[1];
            String outputKey = key + left;

            Long count = distSet.get(outputKey);
            if(count == null){
                distSet.put(outputKey, 1L);
                distDetail.put(outputKey, right);
            }else{
                count += 1;
                distSet.put(outputKey, count);
                distDetail.put(outputKey, distDetail.get(outputKey)+","+right);
            }
        }
        emit(distSet, distDetail, actualWindowLengthInSeconds);
    }

    private void emit(Map<String, Long> counts, Map<String, String> distDetail, int actualWindowLengthInSeconds) {
        for (Entry<String, Long> entry : counts.entrySet()) {
            //obj: 主维度名称::从维度名称::主维度值, count:去重的从维度个数
            String obj = entry.getKey();
            Long count = entry.getValue();
            if(detail){
                String assocValues = distDetail.get(obj);
                collector.emit(new Values(obj, count, assocValues, actualWindowLengthInSeconds));
            }else{
                collector.emit(new Values(obj, count, actualWindowLengthInSeconds));
            }
        }
    }

    //类似于[word,count],但多了一个实际的窗口大小
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        if(detail){
            declarer.declare(new Fields("outputKey", "count", "assocValues", "actualWindowLengthInSeconds"));
        }else{
            declarer.declare(new Fields("outputKey", "count", "actualWindowLengthInSeconds"));
        }
    }

    /**
     * Storm的TickTuple特性可以用在: 比如数据库批量存储, 或者这里的时间窗口的统计等应用
     * "__system" component会定时往task发送 "__tick" stream的tuple
     *
     * 发送频率由TOPOLOGY_TICK_TUPLE_FREQ_SECS来配置, 可以在default.ymal里面配置
     * 也可以在代码里面通过getComponentConfiguration()来进行配置
     *
     * 配置完成后, storm就会定期的往task发送tickTuple
     * 只需要通过isTickTuple来判断是否为tickTuple, 就可以完成定时触发的功能
     */
    @Override
    public Map<String, Object> getComponentConfiguration() {
        Map<String, Object> conf = new HashMap<String, Object>();
        conf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, emitFrequencyInSeconds);
        return conf;
    }
}
