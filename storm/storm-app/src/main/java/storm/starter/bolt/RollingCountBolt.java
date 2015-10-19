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

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

/**
 * 基于时间窗口的counting: 对不断到来的对象的滚动计数
 * This bolt performs rolling counts of incoming objects, i.e. sliding window based counting.滑动窗口计数器
 * <p/>
 * The bolt is configured by two parameters, the length of the sliding window in seconds (which influences影响 the output
 * data of the bolt, i.e. how it will count objects) and the emit frequency in seconds (which influences how often the
 * bolt will output the latest window counts). For instance, if the window length is set to an equivalent of five
 * minutes and the emit frequency to one minute, then the bolt will output the latest five-minute sliding window every minute.
 * <p/>
 * The bolt emits a rolling count tuple per object, consisting of the object itself, its latest rolling count, and the
 * actual duration of the sliding window. The latter(后者) is included in case the expected sliding window length (as
 * configured by the user) is different from the actual length, e.g. due to high system load. Note that the actual
 * window length is tracked and calculated for the window, and not individually for each object within a window.
 * 在系统高负荷时,实际的滑动窗口大小会和配置的窗口大小不一样
 * 注意实际的窗口大小是对整个窗口进行跟踪和计算,而不是为一个窗口中的每隔对象进行单独计数
 * 也就是说在一个窗口中所有对象的窗口大小都是一样的.
 * <p/>
 * Note: During the startup phase you will usually observe that the bolt warns you about the actual sliding window
 * length being smaller than the expected length. This behavior is expected and is caused by the way the sliding window
 * counts are initially "loaded up". You can safely ignore this warning during startup (e.g. you will see this warning
 * during the first ~ five minutes of startup time if the window length is set to five minutes).
 * 刚开始时会提示说窗口大小比期望值小,因为这个时候所有的大小都还没达到配置值
 */
public class RollingCountBolt extends BaseRichBolt {
    private static final long serialVersionUID = 5537727428628598519L;
    private static final Logger LOG = Logger.getLogger(RollingCountBolt.class);

    private static final int NUM_WINDOW_CHUNKS = 5;
    private static final int DEFAULT_SLIDING_WINDOW_IN_SECONDS = NUM_WINDOW_CHUNKS * 60;
    private static final int DEFAULT_EMIT_FREQUENCY_IN_SECONDS = DEFAULT_SLIDING_WINDOW_IN_SECONDS / NUM_WINDOW_CHUNKS;
    private static final String WINDOW_LENGTH_WARNING_TEMPLATE =
            "Actual window length is %d seconds when it should be %d seconds"
                    + " (you can safely ignore this warning during the startup phase)";

    private final SlidingWindowCounter<Object> counter;
    private final int windowLengthInSeconds;
    private final int emitFrequencyInSeconds;
    private OutputCollector collector;
    private NthLastModifiedTimeTracker lastModifiedTracker;

    public RollingCountBolt() {
        this(DEFAULT_SLIDING_WINDOW_IN_SECONDS, DEFAULT_EMIT_FREQUENCY_IN_SECONDS);
    }

    /**
     * 每隔emitFrequencyInSeconds秒,计算过去windowLengthInSeconds大小的数据.
     * 比如: output the latest 9 minutes sliding window every 3 minutes
     * @param windowLengthInSeconds the length of the sliding window in seconds 窗口大小,比如9min
     * @param emitFrequencyInSeconds the emit frequency in seconds 每隔多少秒发送一次,比如3min
     */
    public RollingCountBolt(int windowLengthInSeconds, int emitFrequencyInSeconds) {
        this.windowLengthInSeconds = windowLengthInSeconds;
        this.emitFrequencyInSeconds = emitFrequencyInSeconds;
        counter = new SlidingWindowCounter<Object>(
                deriveNumWindowChunksFrom(this.windowLengthInSeconds, this.emitFrequencyInSeconds));
    }

    private int deriveNumWindowChunksFrom(int windowLengthInSeconds, int windowUpdateFrequencyInSeconds) {
        return windowLengthInSeconds / windowUpdateFrequencyInSeconds;
    }

    @SuppressWarnings("rawtypes")
    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        lastModifiedTracker = new NthLastModifiedTimeTracker(
                deriveNumWindowChunksFrom(this.windowLengthInSeconds, this.emitFrequencyInSeconds));
    }

    /**
     * 对于Trending Topic,一个Topic被认为是一个Tuple,也就是说不会有多个Topic调用同一个execute()方法
     * 这样保证了Topic之间即Tuple之间计数的独立性. 来一个Topic Tuple,只对这个Topic计数+1
     *
     * 如何定义slot数? 对于9 min的时间窗口, 每3 min emit一次数据, 那么就需要9/3=3个slot
     * 那么在3 min以内, 不停的调用countObjAndAck(tuple)来递增所有对象该slot上的计数
     * 每3分钟会触发调用emitCurrentWindowCounts, 用于滑动窗口(通过getCountsThenAdvanceWindow), 并emit (Map<obj, 窗口内的计数和>, 实际使用时间)
     * 因为实际emit触发时间, 不可能刚好是3 min, 会有误差, 所以需要给出实际使用时间
     *
     * 使用TickTuple实现定时发送: 超过发送的时间间隔后,把过去这个时间窗口大小的所有Tuple都发送出去,因此不是针对某一个Tuple了
     * 在getComponentConfiguration中配置了tick tuple的时间频率=emitFrequencyInSeconds
     * 所以当emitFrequencyInSeconds时间过去之后(每隔emitFrequencyInSeconds时间),就应该触发emit动作了.
     *
     * 如果不是TickTuple,说明还没超过发送的时间间隔,保持计数的增长:每收到一个Tuple,计数器+1
     * 它的底层实现会保存这条tuple记录,这样emitCurrentWindowCounts就可以找到过去一段时间内窗口内的全部记录.
     * @param tuple
     */
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
        //获取接收到的对象
        Object obj = tuple.getValue(0);
        //为这个对象的计数器+1
        counter.incrementCount(obj);
        collector.ack(tuple);
    }

    //发送当前窗口的计数
    private void emitCurrentWindowCounts() {
        Map<Object, Long> counts = counter.getCountsThenAdvanceWindow();
        //计算当前窗口实际的时间间隔. 注意这个窗口内的所有对象的counts都是一样的
        int actualWindowLengthInSeconds = lastModifiedTracker.secondsSinceOldestModification();
        lastModifiedTracker.markAsModified();
        if (actualWindowLengthInSeconds != windowLengthInSeconds) {
            LOG.warn(String.format(WINDOW_LENGTH_WARNING_TEMPLATE, actualWindowLengthInSeconds, windowLengthInSeconds));
        }
        emit(counts, actualWindowLengthInSeconds);
    }

    private void emit(Map<Object, Long> counts, int actualWindowLengthInSeconds) {
        for (Entry<Object, Long> entry : counts.entrySet()) {
            Object obj = entry.getKey();
            Long count = entry.getValue();
            //counts里的每个条目的实际窗口长度都是一样的
            collector.emit(new Values(obj, count, actualWindowLengthInSeconds));
        }
    }

    //类似于[word,count],但多了一个实际的窗口大小
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("obj", "count", "actualWindowLengthInSeconds"));
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
