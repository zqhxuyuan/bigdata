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
import java.util.Comparator;
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
import org.calrissian.flowmix.core.model.op.SortOp;
import org.calrissian.flowmix.core.support.EventSortByComparator;
import org.calrissian.flowmix.core.support.window.SortedWindow;
import org.calrissian.flowmix.core.support.window.Window;
import org.calrissian.flowmix.core.support.window.WindowItem;

import static java.util.Collections.singleton;
import static org.calrissian.flowmix.api.builder.FlowmixBuilder.declareOutputStreams;
import static org.calrissian.flowmix.api.builder.FlowmixBuilder.fields;
import static org.calrissian.flowmix.core.Constants.FLOW_LOADER_STREAM;
import static org.calrissian.flowmix.core.support.Utils.exportsToOtherStreams;
import static org.calrissian.flowmix.core.support.Utils.getNextStreamFromFlowInfo;
import static org.calrissian.flowmix.core.support.Utils.hasNextOutput;

/**
 * Sorts a window. This is similar to the Sort operator in InfoSphere Streams.
 * <p>
 * As in infosphere streams:
 * <p>
 * - Sliding windows only allow count-based trigger and count-based expiration 滑动窗口基于个数的触发和失效
 * - Tumbling windows allows count, delta, and time based trigger 滚动窗口允许基于个数,时间,增量方式
 * - Progressive window: 渐进式窗口
 * <p>
 * TODO: Need to begin enforcing the different accepted trigger vs. eviction policies
 */
public class SortBolt extends BaseRichBolt {

    Map<String, Flow> flowMap;
    //和AggregatorBolt一样,不过内存map的value是SortedWindow
    Map<String, Cache<String, SortedWindow>> windows;

    OutputCollector collector;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;
        flowMap = new HashMap<String, Flow>();
        windows = new HashMap<String, Cache<String, SortedWindow>>();
    }

    @Override
    public void execute(Tuple tuple) {
        //Update rules if necessary
        if (FLOW_LOADER_STREAM.equals(tuple.getSourceStreamId())) {
            Collection<Flow> flows = (Collection<Flow>) tuple.getValue(0);
            Set<String> rulesToRemove = new HashSet<String>();

            // find deleted rules and remove them
            for (Flow flow : flowMap.values()) {
                if (!flows.contains(flow))
                    rulesToRemove.add(flow.getId());
            }

            //Remove any deleted rules
            for (String flowId : rulesToRemove) {
                flowMap.remove(flowId);
                windows.remove(flowId);
            }

            for (Flow flow : flows) {
                //If a rule has been updated, let's drop the window windows and start out fresh.
                if (flowMap.get(flow.getId()) != null && !flowMap.get(flow.getId()).equals(flow) ||
                        !flowMap.containsKey(flow.getId())) {
                    flowMap.put(flow.getId(), flow);
                    windows.remove(flow.getId());
                }
            }
        } else if ("tick".equals(tuple.getSourceStreamId())) {
            //Don't bother evaluating if we don't even have any flows
            if (flowMap.size() > 0) {
                for (Flow flow : flowMap.values()) {
                    for (StreamDef curStream : flow.getStreams()) {
                        int idx = 0;
                        for (FlowOp curFlowOp : curStream.getFlowOps()) {
                            if (curFlowOp instanceof SortOp) {
                                SortOp op = (SortOp) curFlowOp;
                                //If we need to trigger any time-based policies, let's do that here
                                //因为tick tuple是每秒发送一次给所有的Bolt, 所有只有基于时间的触发/失效策略, 才需要判断
                                if (op.getTriggerPolicy() == Policy.TIME || op.getEvictionPolicy() == Policy.TIME) {
                                    Cache<String, SortedWindow> windowCache = windows.get(flow.getId() + "\0" + curStream.getName() + "\0" + idx);
                                    if (windowCache != null) {
                                        for (SortedWindow window : windowCache.asMap().values()) {
                                            if (op.getEvictionPolicy() == Policy.TIME)
                                                window.timeEvict(op.getEvictionThreshold());

                                            if (op.getTriggerPolicy() == Policy.TIME)
                                                window.incrTriggerTicks();

                                            if (window.getTriggerTicks() == op.getTriggerThreshold()) {
                                                FlowInfo flowInfo = new FlowInfo(flow.getId(), curStream.getName(), idx);
                                                emitWindow(flowInfo, flow, op, window);
                                            }
                                        }
                                    }
                                }
                            }
                            idx++;
                        }
                    }
                }
            }
        } else {
            //Short circuit if we don't have any rules.
            if (flowMap.size() > 0) {
                /**
                 * If we've received an event for an flowmix rule, we need to act on it here. Purposefully, the groupBy
                 * fields have been hashed so that we know the buffer exists on this current bolt for the given rule.
                 *
                 * The hashKey was added to the "fieldsGrouping" in an attempt to share pointers where possible. Different
                 * rules with like fields groupings can store the items in their windows on the same node.
                 */
                FlowInfo flowInfo = new FlowInfo(tuple);
                Flow flow = flowMap.get(flowInfo.getFlowId());
                SortOp op = (SortOp) flow.getStream(flowInfo.getStreamName()).getFlowOps().get(flowInfo.getIdx());
                Cache<String, SortedWindow> cacheWindow = windows.get(flow.getId() + "\0" + flowInfo.getStreamName() + "\0" + flowInfo.getIdx());
                SortedWindow window;
                if (cacheWindow != null) {
                    window = cacheWindow.getIfPresent(flowInfo.getPartition());
                    if (window != null) {    // if we have a buffer already, process it
                        if (op.getEvictionPolicy() == Policy.TIME)
                            //不需要cacheWindow.put和windows.put吗? AggregatorBolt中就有. 实际上是不需要的. 因为两个值都不为空.
                            window.timeEvict(op.getEvictionThreshold());
                    } else {
                        //cacheWindow!=null,还要创建??
                        //cacheWindow = CacheBuilder.newBuilder().expireAfterAccess(60, TimeUnit.MINUTES).build(); // just in case we get some rogue data, we don't wan ti to sit for too long.
                        window = buildWindow(flowInfo.getPartition(), op);
                        cacheWindow.put(flowInfo.getPartition(), window);
                        windows.put(flow.getId() + "\0" + flowInfo.getStreamName() + "\0" + flowInfo.getIdx(), cacheWindow);
                    }
                } else {
                    cacheWindow = CacheBuilder.newBuilder().expireAfterAccess(60, TimeUnit.MINUTES).build(); // just in case we get some rogue data, we don't wan ti to sit for too long.
                    window = buildWindow(flowInfo.getPartition(), op);
                    cacheWindow.put(flowInfo.getPartition(), window);
                    windows.put(flow.getId() + "\0" + flowInfo.getStreamName() + "\0" + flowInfo.getIdx(), cacheWindow);
                }

                //1.Window对要加入窗口的事件调用add, 对窗口中即将失效的事件,将事件从窗口中删除:基于时间的失效调用timeEvict,基于次数的失效调用expire
                //TODO:AggregatorBolt为什么没有这个处理逻辑?
                if (op.getEvictionPolicy() == Policy.COUNT && op.getEvictionThreshold() == window.size())
                    window.expire();

                //基于次数/个数的窗口,如果在添加之前窗口已经满了,则要先删除一个(即上面的expire),才能添加新的.
                window.add(flowInfo.getEvent(), flowInfo.getPreviousStream());

                //Perform count-based trigger if necessary
                //2.Window还要负责判断是否达到触发条件,每次调用时递增计数器incrTriggerTicks(基于时间的策略在tick中, 基于次数的在这里)
                if (op.getTriggerPolicy() == Policy.COUNT) {
                    window.incrTriggerTicks();
                    //对于渐进式窗口, 每一条事件进来都会调用emitWindow,但并不一定意味着会调用emitWindow里面的collector.emit.
                    if (window.getTriggerTicks() == op.getTriggerThreshold())
                        emitWindow(flowInfo, flow, op, window);
                } else if (op.getTriggerPolicy() == Policy.TIME_DELTA_LT
                        && window.timeRange() > -1
                        && window.timeRange() <= op.getTriggerThreshold() * 1000)
                    emitWindow(flowInfo, flow, op, window);

                //If we aren't supposed to clear the window right now, then we need to emit
                //else if(!op.isClearOnTrigger()) {
                //  if(op.getEvictionPolicy() != Policy.COUNT || (op.getEvictionPolicy() == Policy.COUNT && op.getTriggerThreshold() == windows.size()))
                //    emitWindow(flow, streamName, op, buffer, idx);
                //}
            }
        }
        collector.ack(tuple);
    }

    /**
     * 1.这里SortedWindow有一个属性是sortOnGet,在获取窗口内的事件时进行排序. 使得结果可以能够根据sortBy进行排序.
     * 事件排序是通过sortBy配置字段和排序方式, 并通过buildWindow中的EventSortByComparator(op.getSortBy())指定comparator.
     * 而getEvents会根据comparator进行排序操作.
     * 2.If the window is set to be cleared, we need to emit everything. Otherwise, just emit the last item in the list.
     * 如果clearOnTrigger,则发送窗口内的全部事件. 如果没有clear且是渐进式窗口时,则只发送最后一条事件!
     * 渐进时窗口的失效策略是COUNT, 并且clearOnTrigger=false
     *
     * 以渐进式窗口为例: 假设指定progressive(long countEvictionThreshold)的参数阈值为10.
     * 对于前10条数据:1,2,3,4...10都不会触发emitWindow事件. 注意到window.expire()是在window.add(event)之前的.
     * 所以第十条事件进来时,还没有调用window.add时, window.size=9!不会调用window.expire(). 只有调用window.add(event)后,window.size=10.
     *
     * 因为progressive的triggerThreshold=1. 所以只要一条事件进来,window.incrTriggerTicks就使得计数器=1,就立马触发了emitWindow方法.
     * 但是调用emitWindow方法并不一定就意味着会触发窗口内的collector.emit发射操作.
     * 因为对于前10条数据还没有达到evictThreshold, 即没有满足下面第一个条件的window.size() == op.getEvictionThreshold(),items=null.
     *
     * 只有第11条数据进来时, 判断到window.size=evictThreshold=10, 会首先evict进入窗口内的第一条事件[1]. 然后添加第11条事件[2].
     * 接着增加计数器,使得计数器值为1,满足触发条件,调用emitWindow[3]. 这时满足if条件, 只取出窗口内的最后一条事件,即最新的第11条事件. --> Really??
     *
     * 问题: window.expire()调用events.removeLast()就一定删除最新/最后的事件吗??
     * 观点1:
     * 只有在get的时候才进行排序, 添加事件到队列中的时候并没有排序! 所以删除也没有! 直接取出最后一条, 即最新加入队列中的事件.
     *
     * 观点2: --> The right Answer!
     * 对于progressive而言,clearOnTrigger=false,创建window:buildWindow时,最后一个参数op.isClearOnTrigger()=false,
     * 对应SortedWindow的sortOnGet=false,创建的是带comparator的事件队列:events=new LimitingPriorityDeque<WindowItem>(size, comparator);
     *
     * 对于带有comparator的队列, 在往队列中添加事件记录时, 事件应该是按照指定的字段顺序排序的!
     * 因此删除最后一条事件记录时, 并不一定是删除最新加入的事件! 假设有10条事件, 按照key3升序排列. 下面是事件的顺序号和key3的值.
     * EventNumber  1   2   3   4   5   6   7   8   9   10
     * key3         3   4   2   6   1   8   2   8   7   5
     * 则events为: [1,2,2,3,4,5,6,7,8,8]
     * 假设添加第11条事件进来, 其key3为3. 则首先会expire:removeLast删除谁呢? --> 删除的是值最大的8
     * 当加入事件11时, 现在events=[2,2,3,3,4,5,6,7,8], expire的还是8!  --> 测试用例为EventTupleTest.testComparatorEvent
     *
     * 那么问题来了, 新加入的事件并不一定会被输出. 这是因为窗口是按照字段排序的.
     * 而渐进式的窗口,每次只输出一条事件, 这条事件应该是这个窗口内的最大值的事件!
     *
     * 总结:
     * clearOnTrigger表示是否在触发调用emitWindow时清除窗口内的事件(即events队列)
     * 如果为true, 则添加事件到队列时没有排序操作, 获取窗口内的所有事件时需要排序.
     * 如果为false,则添加事件到队列时就进行排序, 因为没有clear,所以emit的事件只有队列中的最后一条记录,这条记录是按照排序确定的(升序最大值,降序最小值),不一定是最新加入的事件.
     */
    private void emitWindow(FlowInfo flowInfo, Flow flow, SortOp op, Window window) {
        Iterable<WindowItem> items = null;
        if (!op.isClearOnTrigger()
                && op.isProgressive() && window.size() == op.getEvictionThreshold()){
            // we know if it's a progressive window, the eviction policy is count.
            items = singleton(window.expire());
        }else{
            //clear on trigger:emitWindow方法的最后会调用window.clear()清除窗口内的事件.
            //clearOnTrigger=true时, buildWindow的最后一个参数为true,构造的events队列是没有comparator的.
            //所以添加事件到队列中是按照时间顺序的.只有在获取事件时才进行排序.
            items = window.getEvents();
        }

        if (items != null) {
            for (WindowItem item : items) {
                String nextStream = getNextStreamFromFlowInfo(flow, flowInfo.getStreamName(), flowInfo.getIdx());
                if (hasNextOutput(flow, flowInfo.getStreamName(), nextStream))
                    collector.emit(nextStream, new Values(flow.getId(), item.getEvent(), flowInfo.getIdx(), flowInfo.getStreamName(), item.getPreviousStream()));

                // send directly to any non std output streams
                if (exportsToOtherStreams(flow, flowInfo.getStreamName(), nextStream)) {
                    for (String output : flow.getStream(flowInfo.getStreamName()).getOutputs()) {
                        String outputStream = flow.getStream(output).getFlowOps().get(0).getComponentName();
                        collector.emit(outputStream, new Values(flow.getId(), item.getEvent(), -1, output, flowInfo.getStreamName()));
                    }
                }
            }

            if (op.isClearOnTrigger())
                window.clear();
        }

        //每次都要调用重置计数器.
        window.resetTriggerTicks();
    }

    //创建一个具有排序功能的窗口. 窗口内的事件按照sortBy进行排序
    private SortedWindow buildWindow(String hash, SortOp op) {
        Comparator<WindowItem> comparator = new EventSortByComparator(op.getSortBy());
        //根据失效/回收策略创建不同的窗口. 为什么是根据失效策略, 而不是触发策略? 其实和AggregatorBolt一样.
        //因为Flow中指定evict(Policy.COUNT, 1000), 表示窗口内最多存放1000条事件. 所以是根据evict策略!
        return op.getEvictionPolicy() != Policy.COUNT ?
                //TIME
                new SortedWindow(hash, comparator, op.isClearOnTrigger()) :
                //COUNT: 指定窗口大小=失效阈值
                new SortedWindow(hash, comparator, op.getEvictionThreshold(), op.isClearOnTrigger());
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        declareOutputStreams(outputFieldsDeclarer, fields);
    }

}
