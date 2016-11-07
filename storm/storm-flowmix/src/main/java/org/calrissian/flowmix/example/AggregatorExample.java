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

import org.calrissian.flowmix.api.aggregator.*;
import org.calrissian.flowmix.example.support.ExampleRunner;
import org.calrissian.flowmix.example.support.ExampleRunner2;
import org.calrissian.flowmix.example.support.FlowProvider;
import org.calrissian.flowmix.api.Flow;
import org.calrissian.flowmix.api.Policy;
import org.calrissian.flowmix.api.builder.FlowBuilder;

import java.util.List;

import static java.util.Arrays.asList;

/**
 * 1.求一个字段总个数: count
 * select(key3).count
 * => select count(*) from tbl where key3 is not null
 *
 * 2.对一个字段的值进行分组,求每个值的个数: partitionCount
 * select(key3).partition(key3).count
 * => select key3, count(*) from tbl group by key3
 *
 * 3.求一个字段去重后的总个数: distCount
 * ip       count       ipCount
 * 1.1.1.1  3      =>   2
 * 1.1.1.2  2
 *
 * => select distinct count(key3) from tbl
 * 账户数,要求去重, 可以把上面的结果组成Map(key3, count),然后求map.size即为key3的去重个数
 * 但是上面的结果是在不同的聚合操作中完成. 可以再次聚合上面的结果,求key3的count.
 * select count(*) from (
 *  select key3,count(*) from tbl group by key3
 * )
 * 所以我们可以在partition count后再接上一个没有partition的count.
 * 怎么做: 是在一个Stream中,还是在不同的stream使用output?
 *
 * 4.求两个字段分组后, 每个分组的个数: partitionCount2
 * select(key1,key3).partition(key1,key3).count
 * => select key1,key3,count(*) from tbl group by key1,key3
 *
 * 5.选择两个字段, 只有一个分区字段
 * select(key1,key3).partition(key1).count
 *
 * 6.有两个字段(主维度,从维度), 计算从维度在主维度上的个数
 *        partitionCount                    IP关联的账户数(去重) IP : accountDistinctCount : accountTotalCount
 * ip           account     count           ip        accountCount  accountSum
 * 1.1.1.1      AA          5               1.1.1.1     2           7
 * 1.1.1.1      BB          2               1.1.1.2     3           8
 * 1.1.1.2      AA          3          ==>
 * 1.1.1.2      CC          2
 * 1.1.1.2      DD          3
 *
 * select key1, count(*) accountCount, sum(count) accountSum from (
 *   select key1,key3,count(*) count from tbl group by key1 key3  --partitionCount
 * ) group by key1
 * 等价与
 * select key1,distinct count(key3) accountCount accountCount from tbl group by key1
 */
public class AggregatorExample implements FlowProvider {
    @Override
    public List<Flow> getFlows() {
        return assocDistCount();
    }

    public static void main(String args[]) {
        new ExampleRunner2(new AggregatorExample()).run();
    }

    //1.
    // A.十分钟的登陆次数, key3能代表这是一条登陆事件;
    // B.一天的交易次数,key3代表一条交易事件;
    // C.一天的调用量,key3代表一条事件,可以用sequence_id字段表示
    //   适用场景:比如在大屏上实时显示系统的调用事件数量,每隔10秒钟刷新一次.
    // select count(*) from tbl where key3 is not null

    public  List<Flow> count() {
        Flow flow = new FlowBuilder()
                .id("flow1")
                .flowDefs()
                    .stream("stream1")
                        .select().fields("key3").end()
                        .aggregate().aggregator(CountAggregator.class)
                            .config("operatedField", "key3")
                            .evict(Policy.COUNT, 50000)
                            .trigger(Policy.TIME, 5)
                            .windowEvictMillis(3600000) //60min window
                            .clearOnTrigger().end()
                    .endStream()
                .endDefs()
                .createFlow();

        return asList(new Flow[]{flow});
    }

    //2.一天内各个合作方的调用量, key3代表一条调用记录,比如sequence_id
    // select key3, count(*) from tbl group by key3
    public  List<Flow> partitionCount() {
        Flow flow = new FlowBuilder()
                .id("flow1")
                .flowDefs()
                    .stream("stream1")
                        .select().fields("key3").end()
                        /**
                         * Every 5 seconds, emit the counts of events grouped by the key3 field.
                         * Don't allow more than 50000 items to exist in the window at any point in time (maxCount = 50000)
                         * remove this to get the total number of events
                         */
                        .partition().fields("key3").end()
                        .aggregate().aggregator(CountAggregator.class)
                            .config("operatedField", "key3")
                            .evict(Policy.COUNT, 50000)
                            .trigger(Policy.TIME, 5)
                            .clearOnTrigger().end()
                    .endStream()
                .endDefs()
                .createFlow();

        return asList(new Flow[]{flow});
    }

    //3. 账户数,去重
    public  List<Flow> distCount() {
        // select distinct count(key3) from tbl
        Flow flow = new FlowBuilder()
                .id("flow1")
                .flowDefs()
                    .stream("stream1")
                        .select().fields("key3").end()
                        //上面的count的partition字段为key3,表示对key3的不同值进行分组求每一组的个数.
                        //假设有四条事件,每条事件中key3的值分别是v3,v3,v_3,v_3. 则v3的两条记录会被放到一个Window中, v_3的两条记录会被放入另外一个Window中
                        //所以对于Window1,v3的count=2, 对于Window2,v_3的count=2
                        //所以最终结果是: (v3,2), (v_3,2). 其中第一个值是key3的partition value, 第二个值是这个partition value的count.

                        //这里和上面的count不同, 没有partition.所以是对全局而言,求key3不同值的个数,相当于:
                        // select count(*) from (
                        //  select key3,count(*) from tbl group by key3
                        // )
                        //同样假设事件key3的值为v3,v3,v_3,v_3. 对于DistCountAggregator使用了map来保存,所以每条事件进来时map分别是:
                        //(v3->1) -- (v3->2) -- (v3->2, v_3->1) -- (v3->2, v_3->2)
                        //所以求key3的distinct count只要求map的size即可,这里因为只有两个v3,v_3,所以dist count=2
                        .aggregate().aggregator(DistCountAggregator.class)
                            .config("operatedField", "key3")
                            .evict(Policy.COUNT, 50000)
                            .trigger(Policy.TIME, 5)
                            .clearOnTrigger().end()
                    .endStream()
                .endDefs()
                .createFlow();
        return asList(new Flow[]{flow});
    }

    //4.select key1, key3, count(*) from tbl group by key1, key3
    public  List<Flow> partitionCount2() {
        Flow flow = new FlowBuilder()
                .id("flow1")
                .flowDefs()
                    .stream("stream1")
                        .select().fields("key1","key3").end()
                        .partition().fields("key1","key3").end()
                        .aggregate().aggregator(CountAggregator.class)
                            .config("operatedField", "key3")
                            .evict(Policy.COUNT, 50000)
                            .trigger(Policy.TIME, 5)
                            .clearOnTrigger().end()
                        .endStream()
                .endDefs()
                .createFlow();

        return asList(new Flow[]{flow});
    }

    /**
     * 6.key1关联的key3的去重个数
     * select key1, count(*) accountCount, sum(count) accountSum from (
     *   select key1,key3,count(*) count from tbl group by key1 key3
     * ) group by key1
     *
     * select key1,distinct count(key3) from tbl group by key1
     *
     * 不需要使用AssocCountAggregator就能实现主维度关联从维度的个数,而且从维度是去重的.
     2015-09-13 22:11:52 Begin Aggregate....[aggOpIndex:2, groupIndex:flow1key1val1|]
     INPUT: source: aggregate:9, stream: output, id: {}, [flow1, BaseEvent{type='', id='34dbf3ca-53ed-4f29-af80-e9370accee30', timestamp=1442153512381, tuples=[Tuple{key='key1', value=val1, metadata={}}, Tuple{key='distCount', value=2, metadata={}}]}, 2, stream1, stream1]

     2015-09-13 22:12:02 Begin Aggregate....[aggOpIndex:2, groupIndex:flow1key1val1|]
     INPUT: source: aggregate:9, stream: output, id: {}, [flow1, BaseEvent{type='', id='1fc5b708-d1c4-4572-b174-0c56404b92fb', timestamp=1442153522384, tuples=[Tuple{key='key1', value=val1, metadata={}}, Tuple{key='distCount', value=2, metadata={}}]}, 2, stream1, stream1]

     2015-09-13 22:12:12 Begin Aggregate....[aggOpIndex:2, groupIndex:flow1key1val1|]
     INPUT: source: aggregate:9, stream: output, id: {}, [flow1, BaseEvent{type='', id='fb881999-cac9-44ec-a8ec-51a5cff10aec', timestamp=1442153532391, tuples=[Tuple{key='key1', value=val1, metadata={}}, Tuple{key='distCount', value=2, metadata={}}]}, 2, stream1, stream1]
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
                            .config("operatedField", "key3")
                            .evict(Policy.COUNT, 500000)
                            .trigger(Policy.TIME, 10)
                            .clearOnTrigger().end()
                    .endStream()
                .endDefs()
                .createFlow();

        return asList(new Flow[]{flow});
    }

    public List<Flow> assocDistCount2() {
        Flow flow = new FlowBuilder()
                .id("flow1")
                    .flowDefs()
                        .stream("stream1")
                            .select().fields("key1","key3").end()
                            //.partition().fields("key1","key3").end()
                            .partition().fields("key1").end()
                            .aggregate().aggregator(AssocCountAggregator.class)
                                .config("assocField", "key1,key3")
                                .evict(Policy.COUNT, 50000)
                                .trigger(Policy.TIME, 10)
                                .clearOnTrigger().end()
                        .endStream()
                    .endDefs()
                .createFlow();

        return asList(new Flow[]{flow});
    }
}
