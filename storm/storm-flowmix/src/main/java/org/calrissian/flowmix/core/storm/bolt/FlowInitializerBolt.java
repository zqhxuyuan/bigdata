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
import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import org.calrissian.flowmix.api.Flow;
import org.calrissian.flowmix.api.builder.FlowmixBuilder;
import org.calrissian.flowmix.core.model.StreamDef;
import org.calrissian.mango.domain.event.Event;

import static org.calrissian.flowmix.api.builder.FlowmixBuilder.fields;
import static org.calrissian.flowmix.core.Constants.FLOW_LOADER_STREAM;

public class FlowInitializerBolt extends BaseRichBolt {

    Map<String,Flow> flows;  //flow-id -> Flow
    OutputCollector collector;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;
        flows = new HashMap<String, Flow>();
    }

    /**
     * 一个bolt可以使用emit(streamId, tuple)把元组分发到多个流，其中参数streamId是一个用来标识流的字符串。然后，你可以在TopologyBuilder决定由哪个流订阅它
     * emit的3个参数: 发送到的streamid, anchors(来源tuples), tuple(values list)
     */
    @Override
    public void execute(Tuple tuple) {
        //Spout的emit方法的stream-id=FLOW_LOADER_STREAM,tuple=flows
        if(FLOW_LOADER_STREAM.equals(tuple.getSourceStreamId())) {
            //因为只有一个字段:new Values(flows),所以bolt通过getValue(0)可以获取到flows
            for(Flow flow : (Collection<Flow>)tuple.getValue(0))
                flows.put(flow.getId(), flow);
        } else if(!"tick".equals(tuple.getSourceStreamId())){
            //因为有flows和events两个Spout发送tuple(实际上还有一个tick spout).下面是接收到事件tuple的处理
            if(flows.size() > 0) {
                //一个flowLoaderSpout会有多个flows
                for(Flow flow : flows.values()) {
                    Collection<Event> events = (Collection<Event>) tuple.getValue(0);
                    for(Event event : events) {
                        //一个Flow会有多个Stream.初始Bolt是发射给一个Flow中所有Stream的第一个FlowOp.注意这些Stream是平等的(非标准输入的Stream除外).
                        //即初始Bolt发射的Tuple会往所有Stream发射. 而不是说Stream之间是链接的,只发给第一个Stream,第一个Stream完成后将发给第二个Stream.
                        //这实际上是AllGrouping的策略.即相同的数据发给所有其他的Bolt.
                        for(StreamDef stream : flow.getStreams()) {
                            //取Flow的stream的第一个component(FlowOp).因为这是初始化Bolt.
                            String streamid = stream.getFlowOps().get(0).getComponentName();
                            String streamName = stream.getName();

                            //JoinExample中stream3:.stream("stream3", false).因为它不是从源数据过来的,而是从其他Stream发射数据给它使用.所以不是标准的输入.
                            //标准的输入即InitBolt发射的Tuple不会发射给非标准输入的Stream.因为非标准输入的Stream不是来自于InitBolt,而是来自其他Stream.
                            if(stream.isStdInput())
                              //tuple还是events,不会发生变化. 第二个参数tuple是anchor锚点.在最后进行ack.
                              //FLOW_OP_IDX=-1,接收到的Bolt会根据这些字段构造FlowInfo,并且idx+1.所以下一个Bolt的FlowInfo实际IDX=0
                              collector.emit(streamid, tuple, new Values(flow.getId(), event, -1, streamName, streamName));
                        }
                    }
                }
            }
            collector.ack(tuple);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        FlowmixBuilder.declareOutputStreams(outputFieldsDeclarer, fields);
    }
}
