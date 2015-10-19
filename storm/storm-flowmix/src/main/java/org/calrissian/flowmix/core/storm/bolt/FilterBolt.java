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
import org.calrissian.flowmix.core.model.FlowInfo;
import org.calrissian.flowmix.core.model.op.FilterOp;

import static org.calrissian.flowmix.api.builder.FlowmixBuilder.declareOutputStreams;
import static org.calrissian.flowmix.api.builder.FlowmixBuilder.fields;
import static org.calrissian.flowmix.core.Constants.FLOW_LOADER_STREAM;
import static org.calrissian.flowmix.core.support.Utils.exportsToOtherStreams;
import static org.calrissian.flowmix.core.support.Utils.getFlowOpFromStream;
import static org.calrissian.flowmix.core.support.Utils.getNextStreamFromFlowInfo;
import static org.calrissian.flowmix.core.support.Utils.hasNextOutput;

public class FilterBolt extends BaseRichBolt {

    Map<String,Flow> flows;
    OutputCollector collector;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;
        flows = new HashMap<String, Flow>();
    }

    @Override
    public void execute(Tuple tuple) {
        if(FLOW_LOADER_STREAM.equals(tuple.getSourceStreamId())) {
            for(Flow flow : (Collection<Flow>)tuple.getValue(0))
                flows.put(flow.getId(), flow);
        } else if(!"tick".equals(tuple.getSourceStreamId())){
            //假设Filter是Stream的第一个Component(FlowOp).从FlowInitializerBolt可以看到它emit的数据刚好组成FlowInfo
            FlowInfo flowInfo = new FlowInfo(tuple);
            Flow flow = flows.get(flowInfo.getFlowId());
            if(flow != null) {
                FilterOp filterOp = getFlowOpFromStream(flow, flowInfo.getStreamName(), flowInfo.getIdx());

                //将事件运用到Flow的FilterOp上,验证事件是否符合过滤器.
                if(filterOp.getFilter().accept(flowInfo.getEvent())) {
                  //下一个FlowOp的stream-id
                  String nextStream = getNextStreamFromFlowInfo(flow, flowInfo.getStreamName(), flowInfo.getIdx());

                  //还有下一个Component,则继续将自己处理完的tuple发射给下一个Component(Bolt)
                  if(hasNextOutput(flow, flowInfo.getStreamName(), nextStream))
                    //和InitBolt发射的Values一样,是构造FlowInfo的所有字段.
                    collector.emit(nextStream, tuple, new Values(flow.getId(), flowInfo.getEvent(), flowInfo.getIdx(), flowInfo.getStreamName(), flowInfo.getPreviousStream()));

                  // send directly to any non std output streams.
                  // 如果不是Stream中的最后一个FlowOp,不会满足条件的.因为只有Stream中的最后一个FlowOp才有机会发射结果给其他Stream(指定output的那些streams)
                  if(exportsToOtherStreams(flow, flowInfo.getStreamName(), nextStream)) {
                    //一个Stream可以指定多个输出,比如Stream1输出给Stream2,Stream3.输出给别的Stream,是发射给其他Stream的第一个FlowOp.
                    //正如InitBolt是发射给Flow中所有Stream的第一个FlowOp开始处理. output指定的是要输出到的目标Stream的名称
                    for (String output : flow.getStream(flowInfo.getStreamName()).getOutputs()) {
                      //同样从flow中根据streamName获取Stream,并定位到这个Stream的第一个FlowOp上.
                      //这里的outputStream参数不是指Flow中的Stream这个含义,而是Storm的stream-id.
                      String outputStream = flow.getStream(output).getFlowOps().get(0).getComponentName();
                      //最后一个参数previousStream:当前Stream:flowInfo.getStreamName作为下一个Stream:output的前一个streamName.
                      collector.emit(outputStream, tuple, new Values(flowInfo.getFlowId(), flowInfo.getEvent(), -1, output, flowInfo.getStreamName()));
                    }
                  }
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
