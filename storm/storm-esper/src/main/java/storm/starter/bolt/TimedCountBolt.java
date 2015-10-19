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
import net.sf.json.JSONObject;
import org.apache.log4j.Logger;
import storm.starter.model.TDMetric;
import storm.starter.tools.SlidingWindowCounter;
import storm.starter.util.TupleHelpers;
import storm.starter.util.WindowConstant;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

/**
 * 主维度出现的次数, 比如账户的登录次数, 每次登陆都是使用同一个账户, 这个账户登陆了几次.
 * 不需要去重, 因为如果去重之后, 当前这个账户,只会有一个值,就是当前账户.
 *
 * counterMap中保存的是所有不同时间的滑动窗口. 可不可以在Topology时,创建不同参数的Bolt??
 */
public class TimedCountBolt extends BaseRichBolt {
    private static final long serialVersionUID = 5537727428628598519L;
    private static final Logger LOG = Logger.getLogger(TimedCountBolt.class);

    private OutputCollector collector;
    public Map<Integer, SlidingWindowCounter<Object>> counterMap = new HashMap<>();
    private SlidingWindowCounter<Object> counter;

    private static boolean debug = false;
    private boolean isAllCounter = true;

    //[1] 一个Bolt内所有时间窗口
    public TimedCountBolt(){
        System.out.println("TimedCountBolt INIT");
        this.counterMap = WindowConstant.initCounterMap();
    }

    //[2] 一个Bolt只有一个时间窗口,由Topology控制多个Bolt
    public TimedCountBolt(int windowLength){
        this(windowLength, WindowConstant.DEFAULT_EMIT_FREQUENCY_IN_SECONDS);
    }
    public TimedCountBolt(int windowLength, int emitFrequency){
        counter = new SlidingWindowCounter<Object>(windowLength/emitFrequency);
        isAllCounter = false;
    }

    public TimedCountBolt(boolean debug) {
        this();
        this.debug = debug;
    }

    @SuppressWarnings("rawtypes")
    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        System.out.println("TimedCountBolt PREPARE");
        this.collector = collector;
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
        if(debug) {
            JSONObject jsonObject = (JSONObject)tuple.getValue(3);
            WindowConstant.debugJSON(jsonObject);
        }
        String masterValue = tuple.getString(0);
        int timeUnit = tuple.getInteger(1);
        TDMetric tdMetric = (TDMetric)tuple.getValue(2);

        if(isAllCounter) counter = counterMap.get(timeUnit);

        //Count只需要主维度即可, 统计主维度出现的次数
        String key = tdMetric.getKey() +
                timeUnit + WindowConstant.splitKey +
                tdMetric.getMasterField() + WindowConstant.splitKey +
                tdMetric.getMasterValue();

        counter.incrementCount(key);
        collector.ack(tuple);
    }

    private void emitCurrentWindowCounts() {
        if(debug) System.out.println(System.currentTimeMillis()/1000 + "~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~");

        if(isAllCounter){
            for(Entry<Integer, SlidingWindowCounter<Object>> counter: counterMap.entrySet()){
                Map<Object, Long> counts = counter.getValue().getCountsThenAdvanceWindow();
                emit(counts);
            }
        }else{
            Map<Object, Long> counts = counter.getCountsThenAdvanceWindow();
            emit(counts);
        }
    }

    private void emit(Map<Object, Long> counts) {
        for (Entry<Object, Long> entry : counts.entrySet()) {
            Object obj = entry.getKey();
            Long count = entry.getValue();
            collector.emit(new Values(obj, count));
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("obj", "count"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        Map<String, Object> conf = new HashMap<String, Object>();
        //TODO 对于每种时间窗口, 都使用统一的发射频率(比如10s). 对于3个月这么长的时间窗口, 需要自定义吗? 或者说针对不同长度的时间窗口, 定义不同的发射频率?
        conf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, WindowConstant.DEFAULT_EMIT_FREQUENCY_IN_SECONDS);
        return conf;
    }
}
