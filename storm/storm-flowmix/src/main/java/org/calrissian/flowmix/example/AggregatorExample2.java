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
package org.calrissian.flowmix.example;

import org.calrissian.flowmix.api.Flow;
import org.calrissian.flowmix.api.Policy;
import org.calrissian.flowmix.api.aggregator.CountAggregator;
import org.calrissian.flowmix.api.aggregator.DistCountAggregator;
import org.calrissian.flowmix.api.builder.FlowBuilder;
import org.calrissian.flowmix.example.support.ExampleRunner2;
import org.calrissian.flowmix.example.support.FlowProvider;

import java.util.List;

import static java.util.Arrays.asList;

/**
 * 求关联的几种错误方法. 正确的方式在AggregatorExample.assocDistCount
 */
public class AggregatorExample2 implements FlowProvider {
    @Override
    public List<Flow> getFlows() {
        return assocDistCount();
    }

    public static void main(String args[]) {
        new ExampleRunner2(new AggregatorExample2()).run();
    }

    //6.×
    // select key1, count(*) accountCount, sum(count) accountSum from (
    //   select key1,key3,count(*) count from tbl group by key1 key3
    // ) group by key1
    /**
     *
     2015-09-13 20:41:03 Begin Aggregate....[aggOpIndex:2, groupIndex:flow1key1val1|key3val-3|]
     2015-09-13 20:41:03 Begin Aggregate....[aggOpIndex:2, groupIndex:flow1key1val1|key3val3|]

     2015-09-13 20:41:13 Begin Aggregate....[aggOpIndex:2, groupIndex:flow1key1val1|key3val3|]
     2015-09-13 20:41:13 Begin Aggregate....[aggOpIndex:4, groupIndex:flow1key1val1|]
     INPUT: source: aggregate:9, stream: output, id: {}, [flow1, BaseEvent{type='', id='bb2b0b9d-1f4a-4cfd-a7c1-b76544b5600c', timestamp=1442148073993, tuples=[Tuple{key='key1', value=val1, metadata={}}, Tuple{key='count', value=2, metadata={}}]}, 4, stream1, stream1]
     2015-09-13 20:41:13 Begin Aggregate....[aggOpIndex:2, groupIndex:flow1key1val1|key3val-3|]

     2015-09-13 20:41:23 Begin Aggregate....[aggOpIndex:2, groupIndex:flow1key1val1|key3val3|]
     2015-09-13 20:41:23 Begin Aggregate....[aggOpIndex:4, groupIndex:flow1key1val1|]
     INPUT: source: aggregate:9, stream: output, id: {}, [flow1, BaseEvent{type='', id='82cbb39d-6177-4513-a607-127a28cbec48', timestamp=1442148083999, tuples=[Tuple{key='key1', value=val1, metadata={}}, Tuple{key='count', value=2, metadata={}}]}, 4, stream1, stream1]
     2015-09-13 20:41:24 Begin Aggregate....[aggOpIndex:2, groupIndex:flow1key1val1|key3val-3|]

     2015-09-13 20:41:34 Begin Aggregate....[aggOpIndex:2, groupIndex:flow1key1val1|key3val3|]
     2015-09-13 20:41:34 Begin Aggregate....[aggOpIndex:4, groupIndex:flow1key1val1|]
     INPUT: source: aggregate:9, stream: output, id: {}, [flow1, BaseEvent{type='', id='1c680fad-9c6c-4ceb-b554-920cf70f96ba', timestamp=1442148094002, tuples=[Tuple{key='key1', value=val1, metadata={}}, Tuple{key='count', value=2, metadata={}}]}, 4, stream1, stream1]
     2015-09-13 20:41:34 Begin Aggregate....[aggOpIndex:2, groupIndex:flow1key1val1|key3val-3|]

     2015-09-13 20:41:44 Begin Aggregate....[aggOpIndex:2, groupIndex:flow1key1val1|key3val3|]
     2015-09-13 20:41:44 Begin Aggregate....[aggOpIndex:4, groupIndex:flow1key1val1|]
     INPUT: source: aggregate:9, stream: output, id: {}, [flow1, BaseEvent{type='', id='37290777-a9cb-4aef-aa26-7a6403be9469', timestamp=1442148104004, tuples=[Tuple{key='key1', value=val1, metadata={}}, Tuple{key='count', value=2, metadata={}}]}, 4, stream1, stream1]
     2015-09-13 20:41:44 Begin Aggregate....[aggOpIndex:2, groupIndex:flow1key1val1|key3val-3|]
     */
    public  List<Flow> assocCount_1() {
        Flow flow = new FlowBuilder()
                .id("flow1")
                .flowDefs()
                    .stream("stream1")
                        .select().fields("key1","key3").end()
                        .partition().fields("key1", "key3").end()
                        .aggregate().aggregator(CountAggregator.class)
                            .config("operatedField", "key3")
                            .evict(Policy.COUNT, 500000)
                            .trigger(Policy.TIME, 10)
                            .clearOnTrigger().end()         //select key1,key3,count(*) count from tbl group by key1 key3
                        .partition().fields("key1").end()
                        .aggregate().aggregator(CountAggregator.class)
                            .config("operatedField", "key1")
                            .evict(Policy.COUNT, 500000)
                            .trigger(Policy.TIME, 10)
                            .clearOnTrigger().end()
                    .endStream()
                .endDefs()
                .createFlow();

        return asList(new Flow[]{flow});
    }

    /**
     * ×
     * 怎么让不同的partition:key1,key3, 同时计算(立即计算)key1的distinct count(key3).
     * 上面的方式在时间上存在差距, 而下面的方式COUNT=1也不行!
     * select key1,distinct count(key3) accountCount accountCount from tbl group by key1
     *
     2015-09-13 20:52:32 Begin Aggregate....[aggOpIndex:2, groupIndex:flow1key1val1|key3val-3|]
     2015-09-13 20:52:32 Begin Aggregate....[aggOpIndex:2, groupIndex:flow1key1val1|key3val3|]
     2015-09-13 20:52:32 Begin Aggregate....[aggOpIndex:4, groupIndex:flow1key1val1|]
     2015-09-13 20:52:32 Begin Aggregate....[aggOpIndex:4, groupIndex:flow1key1val1|]
     INPUT: source: aggregate:9, stream: output, id: {}, [flow1, BaseEvent{type='', id='4e3c6dd5-d966-41c2-909f-f537b71cdfa1', timestamp=1442148752962, tuples=[Tuple{key='key1', value=val1, metadata={}}, Tuple{key='count', value=1, metadata={}}]}, 4, stream1, stream1]
     INPUT: source: aggregate:9, stream: output, id: {}, [flow1, BaseEvent{type='', id='5314a8bc-0203-4871-b623-6a0956313309', timestamp=1442148752962, tuples=[Tuple{key='key1', value=val1, metadata={}}, Tuple{key='count', value=1, metadata={}}]}, 4, stream1, stream1]
     */
    public  List<Flow> assocCount_2() {
        Flow flow = new FlowBuilder()
                .id("flow1")
                .flowDefs()
                    .stream("stream1")
                        .select().fields("key1","key3").end()
                        .partition().fields("key1", "key3").end()
                        .aggregate().aggregator(CountAggregator.class)
                            .config("operatedField", "key3")
                            .evict(Policy.COUNT, 500000)
                            .trigger(Policy.TIME, 10)
                            .clearOnTrigger().end()         //select key1,key3,count(*) count from tbl group by key1 key3
                        .partition().fields("key1").end()
                        .aggregate().aggregator(CountAggregator.class)
                            .config("operatedField", "key1")
                            .evict(Policy.COUNT, 500000)
                            .trigger(Policy.COUNT, 1)
                            .clearOnTrigger().end()
                    .endStream()
                .endDefs()
                .createFlow();

        return asList(new Flow[]{flow});
    }

    /**
     * × 这种方式跟assocAcount差不多. 结果不是我们想要的.
     2015-09-13 21:23:58 Begin Aggregate....[aggOpIndex:2, groupIndex:flow1key1val1|key3val-3|]
     2015-09-13 21:23:59 Begin Aggregate....[aggOpIndex:2, groupIndex:flow1key1val1|key3val3|]

     2015-09-13 21:24:08 Begin Aggregate....[aggOpIndex:2, groupIndex:flow1key1val1|key3val-3|]
     2015-09-13 21:24:08 Begin Aggregate....[aggOpIndex:2, groupIndex:flow1key1val1|]
     INPUT: source: aggregate:9, stream: output, id: {}, [flow1, BaseEvent{type='', id='5d72c08d-153c-419e-8d37-8e3990f88f25', timestamp=1442150648642, tuples=[Tuple{key='key1', value=val1, metadata={}}, Tuple{key='count', value=2, metadata={}}]}, 2, stream2, stream2]
     2015-09-13 21:24:09 Begin Aggregate....[aggOpIndex:2, groupIndex:flow1key1val1|key3val3|]

     2015-09-13 21:24:18 Begin Aggregate....[aggOpIndex:2, groupIndex:flow1key1val1|]
     INPUT: source: aggregate:9, stream: output, id: {}, [flow1, BaseEvent{type='', id='a8b3df8a-8d69-4e36-9f3d-c29dfdb977da', timestamp=1442150658643, tuples=[Tuple{key='key1', value=val1, metadata={}}, Tuple{key='count', value=2, metadata={}}]}, 2, stream2, stream2]
     2015-09-13 21:24:18 Begin Aggregate....[aggOpIndex:2, groupIndex:flow1key1val1|key3val-3|]
     2015-09-13 21:24:19 Begin Aggregate....[aggOpIndex:2, groupIndex:flow1key1val1|key3val3|]
     */
    public  List<Flow> assocCount_3() {
        Flow flow = new FlowBuilder()
                .id("flow1")
                .flowDefs()
                    .stream("stream1")
                        .select().fields("key1","key3").end()
                        .partition().fields("key1", "key3").end()
                        .aggregate().aggregator(CountAggregator.class)
                            .config("operatedField", "key3")
                            .evict(Policy.COUNT, 500000)
                            .trigger(Policy.TIME, 10)
                            .clearOnTrigger().end()         //select key1,key3,count(*) count from tbl group by key1 key3
                    .endStream(false, "stream2")
                    .stream("stream2", false)
                        .select().fields("key1").end()
                        .partition().fields("key1").end()
                        .aggregate().aggregator(CountAggregator.class)
                            .config("operatedField", "key1")
                            .evict(Policy.COUNT, 500000)
                            .trigger(Policy.TIME, 10)
                            .clearOnTrigger().end()
                    .endStream()
                .endDefs()
                .createFlow();

        return asList(new Flow[]{flow});
    }

    /**
     * Count+assocField, 仅供测试使用
     2015-09-13 21:42:44 Begin Aggregate....[aggOpIndex:2, groupIndex:flow1key1val1|]
     INPUT: source: aggregate:9, stream: output, id: {}, [flow1, BaseEvent{type='', id='5e64f1e5-2a29-4e69-a3f1-36e0cbb22e1c', timestamp=1442151764189, tuples=[Tuple{key='key1', value=val1, metadata={}}, Tuple{key='count', value=47, metadata={}}]}, 2, stream1, stream1]

     2015-09-13 21:42:54 Begin Aggregate....[aggOpIndex:2, groupIndex:flow1key1val1|]
     INPUT: source: aggregate:9, stream: output, id: {}, [flow1, BaseEvent{type='', id='f0c00c2d-430f-49cb-80b0-6120c58016c8', timestamp=1442151774191, tuples=[Tuple{key='key1', value=val1, metadata={}}, Tuple{key='count', value=107, metadata={}}]}, 2, stream1, stream1]

     2015-09-13 21:43:04 Begin Aggregate....[aggOpIndex:2, groupIndex:flow1key1val1|]
     INPUT: source: aggregate:9, stream: output, id: {}, [flow1, BaseEvent{type='', id='81469ddc-b3ce-4fd0-9358-101d67c391be', timestamp=1442151784194, tuples=[Tuple{key='key1', value=val1, metadata={}}, Tuple{key='count', value=167, metadata={}}]}, 2, stream1, stream1]
     */
    public  List<Flow> assocCount() {
        Flow flow = new FlowBuilder()
                .id("flow1")
                .flowDefs()
                    .stream("stream1")
                        .select().fields("key1","key3").end()
                        .partition().fields("key1").end()
                        .aggregate().aggregator(CountAggregator.class)
                            .config("assocField", "key1,key3")
                            .evict(Policy.COUNT, 500000)
                            .trigger(Policy.TIME, 10)
                            .clearOnTrigger().end()         //select key1,key3,count(*) count from tbl group by key1 key3
                    .endStream()
                .endDefs()
                .createFlow();

        return asList(new Flow[]{flow});
    }

    /**
     * × DistCount + assocField[2]. DistCount只会使用assocField的第一个字段, 还不如使用DistCount + operatedField[1]
     * 注意下面的日志 distCount=1, 而实际上应该是2
     2015-09-13 21:46:09 Begin Aggregate....[aggOpIndex:2, groupIndex:flow1key1val1|]
     INPUT: source: aggregate:9, stream: output, id: {}, [flow1, BaseEvent{type='', id='690fa5bd-9bbe-4082-bc9e-f0a6a1d04808', timestamp=1442151969302, tuples=[Tuple{key='key1', value=val1, metadata={}}, Tuple{key='distCount', value=1, metadata={}}]}, 2, stream1, stream1]

     2015-09-13 21:46:19 Begin Aggregate....[aggOpIndex:2, groupIndex:flow1key1val1|]
     INPUT: source: aggregate:9, stream: output, id: {}, [flow1, BaseEvent{type='', id='0988d337-9e02-4a19-86d6-e4c75200dc08', timestamp=1442151979304, tuples=[Tuple{key='key1', value=val1, metadata={}}, Tuple{key='distCount', value=1, metadata={}}]}, 2, stream1, stream1]

     2015-09-13 21:46:29 Begin Aggregate....[aggOpIndex:2, groupIndex:flow1key1val1|]
     INPUT: source: aggregate:9, stream: output, id: {}, [flow1, BaseEvent{type='', id='f560d242-b243-424b-834a-54a04de5991a', timestamp=1442151989307, tuples=[Tuple{key='key1', value=val1, metadata={}}, Tuple{key='distCount', value=1, metadata={}}]}, 2, stream1, stream1]
     */
    public  List<Flow> assocDistCount() {
        Flow flow = new FlowBuilder()
                .id("flow1")
                .flowDefs()
                    .stream("stream1")
                        .select().fields("key1","key3").end()
                        .partition().fields("key1").end()
                        .aggregate().aggregator(DistCountAggregator.class)
                            //IP(key1)的关联账户数(key3), 去重
                            .config("assocField", "key1,key3") //这里有问题! 用key3的话就没问题!
                            .evict(Policy.COUNT, 500000)
                            .trigger(Policy.TIME, 10)
                            .clearOnTrigger().end()         //select key1,key3,count(*) count from tbl group by key1 key3
                    .endStream()
                .endDefs()
                .createFlow();

        return asList(new Flow[]{flow});
    }
}
