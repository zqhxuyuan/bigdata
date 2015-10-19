/*
 * Copyright (C) 2014 The Calrissian Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.calrissian.flowmix.core.storm.bolt;


import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import org.calrissian.flowmix.api.Flow;
import org.calrissian.flowmix.core.model.FlowInfo;
import org.calrissian.flowmix.api.Policy;
import org.calrissian.flowmix.core.model.StreamDef;
import org.calrissian.flowmix.core.model.op.FlowOp;
import org.calrissian.flowmix.core.model.op.JoinOp;
import org.calrissian.flowmix.core.support.window.Window;
import org.calrissian.flowmix.core.support.window.WindowItem;
import org.calrissian.mango.domain.event.BaseEvent;
import org.calrissian.mango.domain.event.Event;

import static com.google.common.collect.Iterables.concat;
import static org.calrissian.flowmix.api.builder.FlowmixBuilder.declareOutputStreams;
import static org.calrissian.flowmix.api.builder.FlowmixBuilder.fields;
import static org.calrissian.flowmix.core.Constants.FLOW_LOADER_STREAM;
import static org.calrissian.flowmix.core.support.Utils.exportsToOtherStreams;
import static org.calrissian.flowmix.core.support.Utils.getFlowOpFromStream;
import static org.calrissian.flowmix.core.support.Utils.getNextStreamFromFlowInfo;
import static org.calrissian.flowmix.core.support.Utils.hasNextOutput;

/**
 * Sliding window join semantics are defined very similar to that of InfoSphere Streams. The join operator,
 * by default, trigger on each single input event from the stream on the right hand side.
 * 滑动窗口的join操作 默认从流的右侧(right stream)在输入的每个事件上触发Join操作.
 *
 * The stream on the right is joined with the stream on the left where the stream on the left
 * is collected into a window which is evicted by the given policy.
 * 参与Join左侧的流会收集事件放到窗口中,并按照指定的策略回收过期的事件
 *
 * The stream on the right has a default eviction policy of COUNT with a threshold of 1.
 * 右侧的流默认的回收策略是Count,阈值为1.
 *
 * Every time a tuple on the right stream is encountered, it is compared against
 * the window on the left and a new tuple is emitted for each find in the join.
 * 每当在右侧的流中收到一条事件tuple, 它会和左侧流的窗口(中全部的事件)比较, 并join出新的tuple.
 *
 * By default, if no partition has been done before the join, every event received on the right stream will
 * be joined with every event currently in the window for the left hand stream.
 * 默认如果在join之前没有partition. 右侧stream接收到的每条事件会和左侧流窗口中的每条事件(所有事件)进行join.
 *
 * It's possible for events to have multi-valued keys, thus it's possible for merged tuples
 * to make a single-valued key into a multi-valued key.
 * 一条事件可能有多值的key-value. 比如相同key会有多个value. 所以将需要合并的tuples将一个单值kv转成多值kv是可行的.
 */
public class JoinBolt extends BaseRichBolt {

    Map<String, Flow> rulesMap;
    Map<String, Cache<String, Window>> windows;

    OutputCollector collector;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;
        rulesMap = new HashMap<String, Flow>();
        windows = new HashMap<String, Cache<String, Window>>();
    }

    @Override
    public void execute(Tuple tuple) {
        //Update rules if necessary
        if(FLOW_LOADER_STREAM.equals(tuple.getSourceStreamId())) {
            Collection<Flow> rules = (Collection<Flow>) tuple.getValue(0);
            Set<String> rulesToRemove = new HashSet<String>();
            // find deleted rules and remove them
            for(Flow rule : rulesMap.values()) {
                if(!rules.contains(rule))
                    rulesToRemove.add(rule.getId());
            }
            //Remove any deleted rules
            for(String ruleId : rulesToRemove) {
                rulesMap.remove(ruleId);
                windows.remove(ruleId);
            }
            for(Flow rule : rules) {
                //If a rule has been updated, let's drop the window windows and start out fresh.
                if(rulesMap.get(rule.getId()) != null && !rulesMap.get(rule.getId()).equals(rule) ||
                        !rulesMap.containsKey(rule.getId())) {
                    rulesMap.put(rule.getId(), rule);
                    windows.remove(rule.getId());
                }
            }
        } else if("tick".equals(tuple.getSourceStreamId())) {
            //Don't bother evaluating if we don't even have any rules
            if(rulesMap.size() > 0) {
                for(Flow rule : rulesMap.values()) {
                    for(StreamDef stream : rule.getStreams()) {
                        int idx = 0;
                        for(FlowOp curOp : stream.getFlowOps()) {
                            if(curOp instanceof JoinOp) {
                                JoinOp op = (JoinOp) curOp;
                                //If we need to trigger any time-based policies, let's do that here.
                                if(op.getEvictionPolicy() == Policy.TIME) {
                                    Cache<String, Window> buffersForRule = windows.get(rule.getId() + "\0" + stream.getName() + "\0" + idx);
                                    if(buffersForRule != null)
                                        for (Window buffer : buffersForRule.asMap().values())
                                            buffer.timeEvict(op.getEvictionThreshold());
                                }
                            }
                            idx++;
                        }
                    }
                }
            }
        } else {
            if (rulesMap.size() > 0) {
                /**
                 * 当前flowInfo是JoinOp在Flow中的Stream. 而不是它的两个参与join操作的输入流.
                 * 假设stream1 join stream2 = stream3. 即stream1和stream2都输出到stream3, 在stream3进行join操作.
                 * 则flowInfo指向的是Stream3. 它的left stream=stream1, right stream=stream2.
                 */
                FlowInfo flowInfo = new FlowInfo(tuple);
                Flow flow = rulesMap.get(flowInfo.getFlowId());
                JoinOp op =  getFlowOpFromStream(flow, flowInfo.getStreamName(), flowInfo.getIdx());

                // do processing on lhs 处理join左边的流.
                if(flowInfo.getPreviousStream().equals(op.getLeftStream())) {
                    Cache<String, Window> cacheWindow = windows.get(flow.getId() + "\0" + flowInfo.getStreamName() + "\0" + flowInfo.getIdx());
                    Window window;
                    if (cacheWindow != null) {
                        window = cacheWindow.getIfPresent(flowInfo.getPartition());
                        if (window != null) {    // if we have a buffer already, process it
                            if(op.getEvictionPolicy() == Policy.TIME)  //If we need to evict any buffered items, let's do it here
                                window.timeEvict(op.getEvictionThreshold());
                        }else{
                            //TODO cache不为空, 但是window为空? 这种情况怎么处理? 这种情况在join时不会存在吗? 因为在Aggregator和SortBolt中都有处理.
                            //这种情况指的是对于在stream中同一个FlowOp(Idx), cache已经存在, 但是cache的key=flowInfo.getPartition. 即分区不一样.
                            //比如按照key3分区, 第一次进来的值是val3. 第二次进来的值是val-3. 则val-3是新的分区.
                        }
                    } else {
                        cacheWindow = CacheBuilder.newBuilder().expireAfterAccess(60, TimeUnit.MINUTES).build(); // just in case we get some rogue data, we don't wan ti to sit for too long.
                        window = op.getEvictionPolicy() == Policy.TIME ? new Window(flowInfo.getPartition()) :
                                new Window(flowInfo.getPartition(), op.getEvictionThreshold()); //如果是COUNT类型的join,则对应的events队列是有界的.
                        cacheWindow.put(flowInfo.getPartition(), window);
                        windows.put(flow.getId() + "\0" + flowInfo.getStreamName() + "\0" + flowInfo.getIdx(), cacheWindow);
                    }
                    //往缓冲区即窗口中添加一条事件
                    //假设在同一个窗口内, 有和之前的事件属于同一个分区的事件进来, 则它们的partition是一样的,则使用同样的Window.
                    window.add(flowInfo.getEvent(), flowInfo.getPreviousStream());

                } else if(flowInfo.getPreviousStream().equals(op.getRightStream())) {
                    //会有两个流数据同时往这个Bolt发送数据. 所以JoinBolt要同时处理两种类型的流.
                    //只有右边的流进来时才会发生join操作. 因为只有左边的数据时,孤掌难鸣.
                    Cache<String, Window> cacheWindow = windows.get(flow.getId() + "\0" + flowInfo.getStreamName() + "\0" + flowInfo.getIdx());
                    Window window;
                    //要求cache事先存在,即left stream会先发数据到JoinBolt. 如果right stream先被接收, 则右边的stream数据会丢失.
                    if (cacheWindow != null) {
                        //What if window is null?? no need to worry about it,because window.getEvents will get null, thus for loop wouldn't work.
                        window = cacheWindow.getIfPresent(flowInfo.getPartition());

                        //缓冲区中的是left stream中的事件.
                        for(WindowItem bufferedEvent : window.getEvents()) {
                          //创建一个新的事件,代表的是join发生的时间
                          Event joined = new BaseEvent(bufferedEvent.getEvent().getId(), bufferedEvent.getEvent().getTimestamp());

                          // the hashcode will filter duplicates 往Map中添加相同的key,会发生覆盖. 但是这里往Event中,表示相同key有多个事件,正好可以用来表示join的含义.
                          joined.putAll(concat(bufferedEvent.getEvent().getTuples()));  //left stream: window events
                          joined.putAll(concat(flowInfo.getEvent().getTuples()));       //right stream: one event
                          String nextStream = getNextStreamFromFlowInfo(flow, flowInfo.getStreamName(), flowInfo.getIdx());

                          if(hasNextOutput(flow, flowInfo.getStreamName(), nextStream))
                              collector.emit(nextStream, new Values(flow.getId(), joined, flowInfo.getIdx(), flowInfo.getStreamName(), bufferedEvent.getPreviousStream()));

                          // send to any other streams that are configured (aside from output)
                          if(exportsToOtherStreams(flow, flowInfo.getStreamName(), nextStream)) {
                            for(String output : flow.getStream(flowInfo.getStreamName()).getOutputs()) {
                              String outputComponent = flow.getStream(output).getFlowOps().get(0).getComponentName();
                              collector.emit(outputComponent, new Values(flow.getId(), joined, -1, output, flowInfo.getStreamName()));
                            }
                          }
                        }
                    }
                } else {
                    throw new RuntimeException("Received event for stream that does not match the join. Flowmix has been miswired.");
                }
            }
        }
        collector.ack(tuple);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        declareOutputStreams(outputFieldsDeclarer, fields);
    }
}
