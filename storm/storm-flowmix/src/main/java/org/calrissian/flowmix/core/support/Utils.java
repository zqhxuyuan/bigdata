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
package org.calrissian.flowmix.core.support;

import java.util.*;

import org.calrissian.flowmix.api.Flow;
import org.calrissian.flowmix.core.model.op.FlowOp;
import org.calrissian.mango.domain.Tuple;
import org.calrissian.mango.domain.event.Event;
import org.calrissian.mango.types.TypeRegistry;

import static org.apache.commons.lang.StringUtils.join;
import static org.calrissian.flowmix.core.Constants.FLOW_LOADER_STREAM;
import static org.calrissian.flowmix.core.Constants.OUTPUT;
import static org.calrissian.mango.types.LexiTypeEncoders.LEXI_TYPES;

public class Utils {

    private static final TypeRegistry<String> registry = LEXI_TYPES;

    private Utils() {
    }

    public static String buildKeyIndexForEvent(Event event, List<String> groupBy) {
        StringBuffer stringBuffer = new StringBuffer();
        if (groupBy == null || groupBy.size() == 0)
            return stringBuffer.toString();  // default partition when no groupBy fields are specified.

        //groupBy可以定义多个字段.
        for (String groupField : groupBy) {
            //一条事件Event可以有多个Tuples.
            Collection<Tuple> tuples = event.getAll(groupField);
            SortedSet<String> values = new TreeSet<String>();

            if (tuples == null) {
                values.add("");
            } else {
                for (Tuple tuple : tuples)
                    values.add(registry.encode(tuple.getValue()));
            }
            stringBuffer.append(groupField + join(values, "") + "|");
        }
        try {
            return stringBuffer.toString();
        } catch (Exception e) {
            return null;
        }
    }

    //flowId|groupField1.xxxxx|groupField2.yyyyy
    public static String buildKeyIndexForEvent(String flowId, Event event, List<String> groupBy) {
        return flowId + buildKeyIndexForEvent(event, groupBy);
    }

    //Flow的Stream的FlowOps集合的下一个FlowOp. 因为组成Stream的flowOps是有序的,即根据定义顺序依次加入
    //假设FlowOpsSize=5,idx从0开始.0,1,2,3.第四个(idx=3)的nextStream为最后一个(idx=4)Op的name.
    //当处理的是最后一个时,idx+1=5 !< 5,所以最后一个FlowOp的nextStream=OUTPUT.
    public static String getNextStreamFromFlowInfo(Flow flow, String streamName, int idx) {
        return idx + 1 < flow.getStreamFlowOpsSize(streamName) ?
                //如果当前FlowOp在Stream中还有下一个FlowOp,则nextStream为下一个FlowOp的name.
                //如果当前FlowOp在Stream中是最后一个FlowOp,则nextStream=OUTPUT
                flow.getFlowOp(streamName,idx+1).getComponentName() : OUTPUT;
    }

    //是否有下一个输出: 当处理Stream的最后一个FlowOp时,nextStream=OUTPUT,同时stdOutput要为true. 一般直接调用endStream()时stdOutput=true
    //如果还是在当前Stream中且非最后一个FlowOp,即还有下一个FlowOp,也满足条件.
    public static boolean hasNextOutput(Flow flow, String streamName, String nextStream) {
        return (nextStream.equals(OUTPUT) && flow.getStream(streamName).isStdOutput()) || !nextStream.equals(OUTPUT);
    }

    //是否导出给其他Stream: Stream流的下一个组件是Output,并且Stream的输出不为空
    //比如JoinExample中stream1,2的endStream第一个参数为false,并指定给stream3进行join(stream3是stream1,2的outputs).
    //那么问题是stream1,2的最后一个FlowOp都会发射给stream3的第一个FlowOp处理.而stream1会不会发射给stream2?
    //NO,因为stream1的output指向stream3.除非stream1的output再指向stream2,这样stream2也能收到stream1的结果,并继续处理.
    public static boolean exportsToOtherStreams(Flow flow, String streamName, String nextStream) {
        return nextStream.equals(OUTPUT) && flow.getStream(streamName).getOutputs() != null;
    }

    //获取Flow的Stream的第idx个FlowOp
    public static <T extends FlowOp> T getFlowOpFromStream(Flow flow, String stream, int idx) {
        return (T) flow.getStream(stream).getFlowOps().get(idx);
    }

    public static <T extends FlowOp> T getFlowOpFromStream(Map<String, Flow> flows, String flowId, String stream, int idx) {
        return (T) flows.get(flowId).getStream(stream).getFlowOps().get(idx);
    }

    //几乎每个Bolt在nextTuple的开始都会这样判断.
    public static void flowsMap(backtype.storm.tuple.Tuple tuple, Map<String,Flow> flows){
        if(FLOW_LOADER_STREAM.equals(tuple.getSourceStreamId())) {
            for(Flow flow : (Collection<Flow>)tuple.getValue(0))
                flows.put(flow.getId(), flow);
        }
    }
}
