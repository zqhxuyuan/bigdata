package org.calrissian.flowmix.example;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.generated.StormTopology;
import org.calrissian.flowmix.api.Flow;
import org.calrissian.flowmix.api.Order;
import org.calrissian.flowmix.api.Policy;
import org.calrissian.flowmix.api.aggregator.CountAggregator;
import org.calrissian.flowmix.api.aggregator.LongSumAggregator;
import org.calrissian.flowmix.api.builder.FlowBuilder;
import org.calrissian.flowmix.api.builder.FlowmixBuilder;
import org.calrissian.flowmix.api.filter.CriteriaFilter;
import org.calrissian.flowmix.api.kryo.EventSerializer;
import org.calrissian.flowmix.api.storm.bolt.PrinterBolt;
import org.calrissian.flowmix.api.storm.spout.MockEventGeneratorSpout;
import org.calrissian.flowmix.api.storm.spout.SimpleFlowLoaderSpout;
import org.calrissian.mango.criteria.builder.QueryBuilder;
import org.calrissian.mango.domain.Tuple;
import org.calrissian.mango.domain.event.BaseEvent;
import org.calrissian.mango.domain.event.Event;

import java.util.ArrayList;
import java.util.List;

import static org.calrissian.mango.criteria.support.NodeUtils.criteriaFromNode;

/**
 * Created by zhengqh on 15/8/27.
 *
 * Some number of events are expected in the standard input stream that contain a country with a value USA and an age field.
 * For each event that comes in, group by the age and country fields, aggregate and count the number of events
 * received for each age/country combination and emit the counts every 5 seconds (clearing the buffers).
 *
 * Aggregate the counts to formulate sums of each count and emit the sums every 10 that are received
 * (that means for each age/country grouping, wait for 10 * 5 seconds or 50 seconds).
 *
 * Collect the sums into an ongoing Top N which will emit every 10 minutes.
 *
 *       5s         5s        5s     ...
 * |----------|----------|----------|...
 *  20,USA,3   21,USA,2    20,USA,5
 *  25,USA,1   23,USA,3    25,USA,2
 *
 *     5s[1]      5s[2]      5s[3]      5s[4]      5s[5]      5s[6]    ...    5s[10]
 * |----------|----------|----------|----------|----------|----------| ... |----------|
 *  20,USA,3   21,USA,2    20,USA,5
 *  25,USA,1   23,USA,3    25,USA,2
 *
 * |<----------------------------------10 counts(50s)-------------------------------->|
 *             select age,country,sum(count) from xx group by by age,country
 * 20,USA,(3+5)
 * 25,USA,(1+2)
 * 21,USA,2
 * 23,USA,3
 *
 * 每个5秒都根据age,country分组count:
 *   select age,country,count(*) as count from xx group by age,country every 5s
 *
 * 5s发生了10次,即每50秒,根据age,country对上面的结果count求和:
 *   select age,country,sum(count) as sum from xx group by age,country every 50s
 *
 * 每隔10分钟,根据sum从大到小排序,输出topN:
 *   select age,country,sum(count) as sum from xx group by age,country order by sum(count) desc limit 10 every 10m
 *
 *
 * evict(Policy.COUNT, 1000)是为了不让分组内的数据过多.
 * 比如对age,country进行分组count,如果分组个数太多,则只保留1000个.
 * 移除的策略是根据count数量. 即(age,country)分组后的聚合函数count(*)值最小的会被删除.
 * 其他策略还有TIME:最早进入的(age,country)分组会被删除
 *
 * trigger表示触发时机,触发策略同样有TIME根据时间, COUNT根据次数. 实现了基于时间和基于次数/长度的时间窗口统计
 * 比如(Policy.TIME,5)表示统计过去5秒内的数据,或者每隔5秒钟统计一次
 * (Policy.COUNT,10)表示统计过去10条记录,或者每收到10条记录就统计一次.
 *
 * clearOnTrigger表示当触发导致的计算结果计算完毕后,清空缓冲区.
 * 假设对5秒内收到的events进行count统计. 在5秒这个时间点计算完毕.
 * 为了重用这个缓冲区,对过去5秒接收到的这些events数据清空.
 * 如果没有使用这种方式,则每个5秒钟都会占用一个内存区域. 这样随着时间推移,这种窗口占用的内存会越来越大.
 * 虽然上面的图例是每个5秒钟都画了一个窗口, 实际上是只有一个5秒的时间窗口的.
 * 过了5秒后,对这5秒的时间窗口计算完毕后,把这个窗口内的缓冲数据都清空,这样接下来5秒钟的数据还是在这个缓冲区内.
 *
 * 对于每个Action操作,比如filter,select,partition,aggregate,最后都调用end方法,就可以链式调用.
 */
public class Hello {

    public static void main(String[] args) {
        //定义Flow:事件流
        final Flow flow = new FlowBuilder()
            .id("myFlow")
            .name("My first flow")
            .description("This flow is just an example")
            .flowDefs()
                .stream("stream1")
                    .filter()
                        .filter(new CriteriaFilter(criteriaFromNode(
                                new QueryBuilder().eq("country", "USA").build())))
                    .end()
                    .select().fields("age", "country").end()
                    .partition().fields("age", "country").end()
                    .aggregate().aggregator(CountAggregator.class)
                        .config("outputField", "count")
                        .evict(Policy.COUNT, 1000)
                        .trigger(Policy.TIME, 5)
                        .clearOnTrigger().end()
                    .partition().fields("age", "country").end()
                    .aggregate().aggregator(LongSumAggregator.class)
                        .config("sumField", "count")
                        .config("outputField", "sum")
                        .evict(Policy.COUNT, 1000)
                        .trigger(Policy.COUNT, 10)
                        .clearOnTrigger().end()
                    .sort().sortBy("sum", Order.DESC)
                        //统计10秒内的top10的sum. 最后一个参数clearOnTrigger=false,表示topN是有状态的.
                        .topN(10, Policy.TIME, 1000 * 10, false).end()
                .endStream()
            .endDefs()
        .createFlow();

        List<Flow> flows = new ArrayList(){{
            add(flow);
        }};// create list of flows using the FlowBuilder
        List<Event> events = new ArrayList(){{
            for(int i=0;i<100000;i++){
                Event event = new BaseEvent(""+i);
                event.put(new Tuple("",""));
                add(event);
            }
        }};// create a list of mock events to send through Flowmix

        StormTopology topology = new FlowmixBuilder()
            .setFlowLoader(new SimpleFlowLoaderSpout(flows, 60000))    // spout to provide the flows
            .setEventsLoader(new MockEventGeneratorSpout(events, 10))  // spout to provide the events
            .setOutputBolt(new PrinterBolt())                          // standard output bolt
            .setParallelismHint(6)                                     // set the amount of parallelism
            .create()
            .createTopology();

        Config conf = new Config();
        conf.setNumWorkers(1);
        conf.setMaxSpoutPending(5000);
        conf.registerSerialization(BaseEvent.class, EventSerializer.class);
        conf.setSkipMissingKryoRegistrations(false);

        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("example-flowmix-topology", conf, topology);
    }
}
