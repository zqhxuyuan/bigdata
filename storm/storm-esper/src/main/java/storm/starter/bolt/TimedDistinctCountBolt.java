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
import storm.starter.model.TDMetric;
import storm.starter.tools.SlidingWindowCounter;
import storm.starter.util.TupleHelpers;
import storm.starter.util.WindowConstant;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

public class TimedDistinctCountBolt extends BaseRichBolt {
    private static final long serialVersionUID = 5537727428628598519L;
    private static final Logger LOG = Logger.getLogger(TimedDistinctCountBolt.class);

    private OutputCollector collector;

    public Map<Integer, SlidingWindowCounter<Object>> counterMap = new HashMap<>();

    public TimedDistinctCountBolt(){
        counterMap = WindowConstant.initCounterMap();
    }

    @SuppressWarnings("rawtypes")
    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
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
        Object obj = tuple.getValue(2);
        TDMetric tdMetric = (TDMetric)obj;
        int timeUnit = tdMetric.getTimeUnit();
        SlidingWindowCounter<Object> counter = counterMap.get(timeUnit);

        String key = tdMetric.getKey() +
                timeUnit + WindowConstant.splitKey +
                tdMetric.getMasterField() + WindowConstant.splitKey +
                tdMetric.getSlaveField() + WindowConstant.splitKey +
                tdMetric.getMasterValue() + WindowConstant.splitKey +
                tdMetric.getSlaveValue();

        counter.incrementCount(key);
        collector.ack(tuple);
    }

    private void emitCurrentWindowCounts() {
        for(Entry<Integer, SlidingWindowCounter<Object>> counter: counterMap.entrySet()){
            Map<Object, Long> counts = counter.getValue().getCountsThenAdvanceWindow();

            Set<String> keySet = (Set)counts.keySet();
            Map<String, Long> distSet = new HashMap<>();
            for (String key : keySet){
                String left = key.substring(0,key.lastIndexOf(WindowConstant.splitKey));
                Long count = distSet.get(left);
                if(count == null){
                    distSet.put(left, 1L);
                }else{
                    count += 1;
                    distSet.put(left, count);
                }
            }
            emit(distSet);
        }
    }

    private void emit(Map<String, Long> counts) {
        for (Entry<String, Long> entry : counts.entrySet()) {
            String obj = entry.getKey();
            Long count = entry.getValue();
            collector.emit(new Values(obj, count));
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("obj", "distCount"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        Map<String, Object> conf = new HashMap<String, Object>();
        //TODO 对于每种时间窗口, 都使用统一的发射频率(比如10s). 对于3个月这么长的时间窗口, 需要自定义吗? 或者说针对不同长度的时间窗口, 定义不同的发射频率?
        conf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, WindowConstant.DEFAULT_EMIT_FREQUENCY_IN_SECONDS);
        return conf;
    }
}
