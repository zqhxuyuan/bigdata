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

import java.text.SimpleDateFormat;
import java.util.Collection;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
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
import org.calrissian.flowmix.core.model.event.AggregatedEvent;
import org.calrissian.flowmix.core.model.op.AggregateOp;
import org.calrissian.flowmix.core.model.op.FlowOp;
import org.calrissian.flowmix.core.model.op.PartitionOp;
import org.calrissian.flowmix.api.Aggregator;
import org.calrissian.flowmix.core.support.window.AggregatorWindow;

import static org.apache.commons.lang.StringUtils.join;
import static org.calrissian.flowmix.api.builder.FlowmixBuilder.declareOutputStreams;
import static org.calrissian.flowmix.api.builder.FlowmixBuilder.fields;
import static org.calrissian.flowmix.core.Constants.FLOW_LOADER_STREAM;
import static org.calrissian.flowmix.api.Aggregator.GROUP_BY;
import static org.calrissian.flowmix.api.Aggregator.GROUP_BY_DELIM;
import static org.calrissian.flowmix.core.support.Utils.exportsToOtherStreams;
import static org.calrissian.flowmix.core.support.Utils.getFlowOpFromStream;
import static org.calrissian.flowmix.core.support.Utils.hasNextOutput;

public class AggregatorBolt extends BaseRichBolt {

    Map<String,Flow> flows;
    OutputCollector collector;

    //聚合比其他Bolt多了个聚合窗口. key=flowId.streamName.aggFlowOpsIndex. Cache.key=hash partition
    Map<String, Cache<String, AggregatorWindow>> windows;
    SimpleDateFormat sdf = new SimpleDateFormat("", Locale.SIMPLIFIED_CHINESE);

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;
        flows = new HashMap<String, Flow>();
        windows = new HashMap<String, Cache<String, AggregatorWindow>>();
        sdf.applyPattern("yyyy-MM-dd HH:mm:ss");
    }

    /**
     * 聚合Bolt要解决基于时间和基于次数两种策略的聚合.
     * 1.时间以tick tuple调用一次为一秒钟过去了,计数器+1. 当计数器值达到阈值,触发emitAggregate
     * 2.次数以非tick tuple调用一次为一条事件,计数器也+1. 当计数器值达到阈值,触发emitAggregate
     *
     * 注意点:
     * 1.在还没有满足阈值的触发动作时, tick和非tick如果是时间策略,都需要调用AggregateWindow.timeEvict使过期事件失效
     * 2.非tick tuple,收集事件放到AggregateWindow中.除了将Event加入events队列中,还调用Aggregator.add将本次事件和聚合结果合并(比如count+1).
     * 3.当触发聚合动作时,emitAggregate会调用AggregatorWindow.getAggregate,后台调用Aggregate.aggregate将合并所有结果并包装为一个事件(比如返回count)给下一个Bolt处理
     * @param tuple
     */
    @Override
    public void execute(Tuple tuple) {
        if(FLOW_LOADER_STREAM.equals(tuple.getSourceStreamId())) {
            for(Flow flow : (Collection<Flow>)tuple.getValue(0))
                flows.put(flow.getId(), flow);
        } else if("tick".equals(tuple.getSourceStreamId())) {
            //tick spout每隔一秒发送发送一个空值.每个Bolt都能收到这样的tick.因为在FlowmixBuilder.declarebolt中定义Bolt时的tick分组策略是allGrouping
            //Don't bother evaluating if wwe don't even have any flows
            if(flows.size() > 0) {
                for(Flow flow : flows.values()) {
                    for(StreamDef curStream : flow.getStreams()) {
                        //一个Stream中会有多个AggOp. 从0开始找这样的AggOp. 即使不是AggOp,idx++.这样找到的AggOp,此时的idx表示AggOp在flowOps中第几个.
                        int idx = 0;
                        for(FlowOp curFlowOp : curStream.getFlowOps()) {
                            if(curFlowOp instanceof AggregateOp) {
                                //只有聚合操作符才需要统计动作,即触发窗口内的所有事件的聚合动作(当然不一定是立即计算)
                                //对于窗口内的所有事件,每当tick一次,基于时间的失效策略需要移除过期的事件.
                                AggregateOp op = (AggregateOp)curFlowOp;
                                //If we need to trigger any time-based policies, let's do that here 基于时间的策略(触发策略和失效策略)的动作
                                if(op.getTriggerPolicy() == Policy.TIME || op.getEvictionPolicy() == Policy.TIME) {
                                    Cache<String, AggregatorWindow> windowCache = windows.get(flow.getId() + "\0" + curStream.getName() + "\0" + idx);
                                    //Cache中必须有,如果Cache为空,说明这个时间段内都没有事件进来.没有事件就没有Cache,就不需要移除事件的失效动作和统计结果的发射动作.
                                    if(windowCache != null) {
                                        //windowCache怎么存放数据在非tick中:<flowInfo.getPartition(), AggregatorWindow>.
                                        //如果没有partition,则只有一个AggregatorWindow. 则在满足ticks后,调用一次emitAggregate
                                        //如果有多个partition,则每个partition都有一个Window,并且每个partition在满足ticks后都会调用emitAggregate
                                        //是否达到ticks是由每个AggregatorWindow的计数器确定的.
                                        for(AggregatorWindow window : windowCache.asMap().values()) {
                                            //失效策略,每tick一秒,移除过期的事件
                                            if(op.getEvictionPolicy() == Policy.TIME)
                                                window.timeEvict(op.getEvictionThreshold());

                                            //触发策略,每tick一秒,增加时间tick计数器. 每隔多长时间触发一次统计动作是由这个计数器引起的!
                                            if(op.getTriggerPolicy() == Policy.TIME)
                                                window.incrTriggerTicks();

                                            //因为每秒都会增加一次tick计数器. 最后计数器的值会达到阈值,触发聚合动作
                                            //假设Stream设置trigger(Policy.TIME, 5)即每5秒触发一次统计(单位是秒,所以计数器每秒+1,加到阈值后,刚好满足触发条件).
                                            //则tick spout发送5次tick tuple后.AggregatorBolt会统计5秒内的事件并发射给下一个Bolt
                                            if(window.getTriggerTicks() == op.getTriggerThreshold()){
                                                System.out.println("TIME emitAggregate..");
                                                emitAggregate(flow, op, curStream.getName(), idx, window);
                                            }
                                        }
                                    }
                                }
                            }
                            //FlowOp的索引
                            idx++;
                        }
                    }
                }
            }
        } else if(!"tick".equals(tuple.getSourceStreamId())){
            //在tick spout每秒都会发射tick tuple时.其他Bolt结合事件会发射到这里.在没有达到触发条件时,Aggregate负责收集事件到窗口内.在满足触发条件时,统计窗口内收集到的所有事件.
            FlowInfo flowInfo = new FlowInfo(tuple);
            //如果select一个字段,则打印出的都是1,说明调用一次execute,只有一条事件进来.尽管一秒钟(tick中)会有很多条事件.
            //如果select了多个字段,则一个event因为有多个Tuple,size!=1
            //System.out.println("events:" + flowInfo.getEvent().getTuples().size());
            //System.out.println("events:" + flowInfo.getEvent());
            Flow flow = flows.get(flowInfo.getFlowId());
            if(flow != null) {
                AggregateOp op = getFlowOpFromStream(flow, flowInfo.getStreamName(), flowInfo.getIdx());
                Cache<String, AggregatorWindow> windowCache = windows.get(flowInfo.getFlowId() + "\0" + flowInfo.getStreamName() + "\0" + flowInfo.getIdx());

                AggregatorWindow window = null;
                if(windowCache != null) {
                    window = windowCache.getIfPresent(flowInfo.getPartition());
                    // if we have a window already constructed, proces it
                    if(window != null) {
                        //If we need to evict any buffered items, let's do that here 这里只有失效策略移除事件,没有触发策略增加计数器,计数器是在tick中
                        if(op.getEvictionPolicy() == Policy.TIME)
                            window.timeEvict(op.getEvictionThreshold());
                    } else {
                        //may be exist window but expire after windowEvictMillis, or non exist window which is a new partition.
                        window = buildWindow(op, flowInfo.getStreamName(), flowInfo.getIdx(), flowInfo.getPartition(), flowInfo.getFlowId(), windowCache);
                    }
                } else {
                    /**
                     * Cache不存在,先创建这样的Cache,让后往Cache中put Key Value.
                     * TODO: default windowEvict=60min, what about long time window such as 1day,1week,even 1month,3month.after window evict, does data lost too?
                     * after window expire, windowCache!=null, but window=null. see EventTupleTest.testCacheExpire. so buildWindow again.
                     * window是针对partition的. 当这个partition的window在1个小时之内都没有任何事件再进来, 这个窗口就会被关闭!
                     * 假设我们要统计过去2个小时的数据, 假设key3=val3有两条事件发生的时间是: 1min和65min. 在第一条事件发生的一个小时之后即61min时,window被关闭.
                     * 当第二条事件进来时,windowCache!=null & window=null. 创建了一个新的window. 然后往这个新的window添加了第二条事件.
                     * 注意:还要考虑tick操作,因为是TIME-Based统计,所以在61min之后,在windowCache中不会存在key2=val3这个Window了.
                     * 这样第二条事件只会放入新的Window中, 而旧的Window已经不复存在了. 所以旧的Window中的第一条事件信息也会丢失. 那么统计就不准确了.
                     * 一个解决办法是如果TIME=2hour,则设置windowEvictMillis=2hour! 保证窗口不会过期. 但是如果时间跨度很大,window在内存中就存活地更长了.
                     */
                    windowCache = CacheBuilder.newBuilder().expireAfterWrite(op.getWindowEvictMillis(), TimeUnit.MILLISECONDS).build();
                    //先往windowCache中put partitionKey -> 新build的AggregatorWindow
                    window = buildWindow(op, flowInfo.getStreamName(), flowInfo.getIdx(), flowInfo.getPartition(), flowInfo.getFlowId(), windowCache);
                }

                //AggregatorWindow中put事件. 在窗口还没关闭时,一直往这个窗口内添加数据
                window.add(flowInfo.getEvent(), flowInfo.getPreviousStream());
                //window eviction is on writes, so we need to write to the window to reset our expiration.
                //如果Aggregator前一个不是Partition,则partition=null.
                //NOTICE: 通常设置partition字段相当于SQL中的group by.相同partition的事件会进入同一个Window被聚合函数处理.
                windowCache.put(flowInfo.getPartition(), window);

                //Perform count-based trigger if necessary 基于次数:当窗口内的事件数量达到阈值时,开始聚合操作
                //emitAggregate动作有两处:基于时间在tick中, 基于次数在非tick中. 因为tick一秒钟可能有多条事件,所以用tick计数器每秒+1
                //非tick的tuple进来时,execute方法一次只可能接收一条事件,所以基于次数的统计,可以在这里增加计数器
                //因为一个AggregateOp只可能定义要么时间要么次数的策略,所以可以共用triggerTicks计数器,不会说既在tick中也在这里都往同一个计数器+1
                if(op.getTriggerPolicy() == Policy.COUNT) {
                    window.incrTriggerTicks();
                    if(window.getTriggerTicks() == op.getTriggerThreshold()){
                        System.out.println("COUNT emitAggregate..");
                        emitAggregate(flow, op, flowInfo.getStreamName(), flowInfo.getIdx(), window);
                    }
                }
            }
        }
        collector.ack(tuple);
    }

    //window=null或者windowCache=null,都会创建新的聚合窗口. 并添加到windows map中. 而windows内层的KV中,因为value是新的聚合窗口,所以windowCache也要放一份.
    private AggregatorWindow buildWindow(AggregateOp op, String stream, int idx, String hash, String flowId, Cache<String, AggregatorWindow> windowCache) {
      try {
        Aggregator agg = op.getAggregatorClass().newInstance();
        AggregatorWindow window = op.getEvictionPolicy() == Policy.TIME || op.getEvictionPolicy() == Policy.TIME_DELTA_LT ?
                new AggregatorWindow(agg, hash) :
                //如果是Count,后台会使用带有容量限制的双端队列. 它的事件失效策略由队列自己控制:比如添加事件进来时,如果队列满了,自动删除队列头事件.所以不需要代码中手动控制.
                new AggregatorWindow(agg, hash, op.getEvictionThreshold());

        //上一个是PartitionOp,则根据PartitionOp的字段进行GroupBy
        FlowOp flowOp = getFlowOpFromStream(flows, flowId, stream, idx-1);
        if(flowOp instanceof PartitionOp) {
            PartitionOp partitionOp = (PartitionOp)flowOp;
            Map<String,String> aggConfig = op.getConfig();
            aggConfig.put(GROUP_BY, join(partitionOp.getFields(), GROUP_BY_DELIM));
            //手动调用更新配置
            agg.configure(aggConfig);
        }

        //新创建的聚合窗口AggregatorWindow,里面还没有填充数据,比如将事件加入到窗口中. 所以最后返回这个聚合窗口, 在execute中往window中填充事件.
        windowCache.put(hash, window);
        //windows被看做是key -> (key -> value)的结构. 内层的key->value即windowCache. 由于是创建新的聚合窗口,说明这是第一次加入到windows中.
        //idx是AggregatorBolt在streams中的位置,注意key并没有和hash(partition)相关. idx更外层,hash为内层map的key.
        windows.put(flowId + "\0" + stream + "\0" + idx, windowCache);
        return window;
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }

    private void emitAggregate(Flow flow, AggregateOp op, String stream, int idx, AggregatorWindow window) {
        //从窗口获取到的也是事件,这样可以继续将聚合结果下发给下一个组件.
        //while(window.getEvents().iterator().hasNext()){
        //    System.out.println("AggregateEvents:"+window.getEvents().iterator().next().getEvent());
        //}
        System.out.println(sdf.format(System.currentTimeMillis())+" Begin Aggregate....[aggOpIndex:"+idx+", groupIndex:" + window.getGroupedIndex()+"]");

        Collection<AggregatedEvent> eventsToEmit = window.getAggregate();
        String nextStream = idx+1 < flow.getStream(stream).getFlowOps().size() ? getFlowOpFromStream(flow, stream, idx + 1).getComponentName() : "output";

        if(hasNextOutput(flow, stream, nextStream)) {
          for(AggregatedEvent event : eventsToEmit) {
            String previousStream = event.getPreviousStream() != null ? event.getPreviousStream() : stream;
            // Note: If aggregated event isn't keeping the previous stream, it's possible it could be lost
            collector.emit(nextStream, new Values(flow.getId(), event.getEvent(), idx, stream, previousStream));
          }
        }

        // send to any other streams that are configured (aside from output)
        if(exportsToOtherStreams(flow, stream, nextStream)) {
          for(String output : flow.getStream(stream).getOutputs()) {
            for(AggregatedEvent event : eventsToEmit) {
              String outputComponent = flow.getStream(output).getFlowOps().get(0).getComponentName();
              collector.emit(outputComponent, new Values(flow.getId(), event.getEvent(), -1, output, stream));
            }
          }
        }

        //计数器达到阈值后,触发emitAggregate调用,上面对窗口内的事件统计完毕后,清空计数器.使得下一个窗口继续使用新的计数器.
        if(op.isClearOnTrigger()) window.clear();
        window.resetTriggerTicks();
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        declareOutputStreams(outputFieldsDeclarer, fields);
    }
}
