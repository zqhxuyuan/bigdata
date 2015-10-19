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
package org.calrissian.flowmix.api.builder;

import backtype.storm.topology.BoltDeclarer;
import backtype.storm.topology.IComponent;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import org.calrissian.flowmix.api.storm.bolt.EventsLoaderBaseBolt;
import org.calrissian.flowmix.api.storm.bolt.FlowLoaderBaseBolt;
import org.calrissian.flowmix.api.storm.spout.EventsLoaderBaseSpout;
import org.calrissian.flowmix.api.storm.spout.FlowLoaderBaseSpout;
import org.calrissian.flowmix.core.storm.bolt.AggregatorBolt;
import org.calrissian.flowmix.core.storm.bolt.EachBolt;
import org.calrissian.flowmix.core.storm.bolt.FilterBolt;
import org.calrissian.flowmix.core.storm.bolt.FlowInitializerBolt;
import org.calrissian.flowmix.core.storm.bolt.JoinBolt;
import org.calrissian.flowmix.core.storm.bolt.PartitionBolt;
import org.calrissian.flowmix.core.storm.bolt.SelectorBolt;
import org.calrissian.flowmix.core.storm.bolt.SortBolt;
import org.calrissian.flowmix.core.storm.bolt.SplitBolt;
import org.calrissian.flowmix.core.storm.bolt.SwitchBolt;
import org.calrissian.flowmix.core.storm.spout.TickSpout;

import static org.calrissian.flowmix.core.Constants.BROADCAST_STREAM;
import static org.calrissian.flowmix.core.Constants.EVENT;
import static org.calrissian.flowmix.core.Constants.FLOW_ID;
import static org.calrissian.flowmix.core.Constants.FLOW_LOADER_STREAM;
import static org.calrissian.flowmix.core.Constants.FLOW_OP_IDX;
import static org.calrissian.flowmix.core.Constants.INITIALIZER;
import static org.calrissian.flowmix.core.Constants.LAST_STREAM;
import static org.calrissian.flowmix.core.Constants.OUTPUT;
import static org.calrissian.flowmix.core.Constants.STREAM_NAME;
import static org.calrissian.flowmix.core.model.op.AggregateOp.AGGREGATE;
import static org.calrissian.flowmix.core.model.op.EachOp.EACH;
import static org.calrissian.flowmix.core.model.op.FilterOp.FILTER;
import static org.calrissian.flowmix.core.model.op.JoinOp.JOIN;
import static org.calrissian.flowmix.core.model.op.PartitionOp.PARTITION;
import static org.calrissian.flowmix.core.model.op.SelectOp.SELECT;
import static org.calrissian.flowmix.core.model.op.SortOp.SORT;
import static org.calrissian.flowmix.core.model.op.SplitOp.SPLIT;
import static org.calrissian.flowmix.core.model.op.SwitchOp.SWITCH;

/**
 * Builds the base flowmix topology configuration. The topology builder is returned so that it can be further customized.
 * Most often, it will be useful to further provision(预备) a downstream(下游) bolt that will process the data even after the output.
 * The output stream and component id provisioned on the output of the builder are both "output".
 */

public class FlowmixBuilder {

  private IComponent flowLoaderSpout;
  private IComponent eventsComponent;
  private IRichBolt outputBolt;
  private int parallelismHint = 1;
  private int eventLoaderParallelism = -1;

  /**
   * @param flowLoader A spout that feeds rules into flowmix. This just needs to emit a Collection<Flow> in each tuple
   *                  at index 0 with a field name of "flows".
   * @param eventsSpout A spout that provides the events to std input.
   * @param outputBolt  A bolt to accept the output events (with the field name "event")
   * @param parallelismHint The number of executors to run the parallel streams.
   */
  public FlowmixBuilder FlowmixBuilder(FlowLoaderBaseSpout flowLoader, EventsLoaderBaseSpout eventsSpout,
                                       IRichBolt outputBolt, int parallelismHint){
      this.flowLoaderSpout = flowLoader;
      this.eventsComponent = eventsSpout;
      this.outputBolt = outputBolt;
      this.parallelismHint = parallelismHint;
      return this;
  }

  public FlowmixBuilder setFlowLoader(FlowLoaderBaseSpout flowLoader) {
    this.flowLoaderSpout = flowLoader;
    return this;
  }

  public FlowmixBuilder setFlowLoader(FlowLoaderBaseBolt flowLoader) {
    this.flowLoaderSpout = flowLoader;
    return this;
  }

  public FlowmixBuilder setEventsLoader(EventsLoaderBaseBolt eventsLoader) {
    this.eventsComponent = eventsLoader;
    return this;
  }

  public FlowmixBuilder setEventsLoader(EventsLoaderBaseSpout eventsLoader) {
    this.eventsComponent = eventsLoader;
    return this;
  }

  public FlowmixBuilder setEventLoaderParallelism(int eventLoaderParallelism) {
    this.eventLoaderParallelism = eventLoaderParallelism;
    return this;
  }

  public FlowmixBuilder setOutputBolt(IRichBolt outputBolt) {
    this.outputBolt = outputBolt;
    return this;
  }

  public FlowmixBuilder setParallelismHint(int parallelismHint) {
    this.parallelismHint = parallelismHint;
    return this;
  }

  private void validateOptions() {
    String errorPrefix = "Error constructing Flowmix: ";
    if(flowLoaderSpout == null)
      throw new RuntimeException(errorPrefix + "A flow loader component needs to be set.");
    else if(eventsComponent == null)
      throw new RuntimeException(errorPrefix + "An event loader component needs to be set.");
    else if(outputBolt == null)
      throw new RuntimeException(errorPrefix + "An output bolt needs to be set.");
  }

  /**
   * @return A topology builder than can further be customized.
   */
  public TopologyBuilder create() {
      //Topology的构造器
      TopologyBuilder builder = new TopologyBuilder();

      //事件和Flows分别作为数据源Spout
      if(eventsComponent instanceof IRichSpout)
        builder.setSpout(EVENT, (IRichSpout) eventsComponent, eventLoaderParallelism == -1 ? parallelismHint : eventLoaderParallelism);
      else if(eventsComponent instanceof IRichBolt)
        builder.setBolt(EVENT, (IRichBolt) eventsComponent, eventLoaderParallelism == -1 ? parallelismHint : eventLoaderParallelism);
      else
        throw new RuntimeException("The component for events is not valid. Must be IRichSpout or IRichBolt");

      if(flowLoaderSpout instanceof IRichSpout)
        builder.setSpout(FLOW_LOADER_STREAM, (IRichSpout) flowLoaderSpout, 1);
      else if(flowLoaderSpout instanceof IRichBolt)
        builder.setBolt(FLOW_LOADER_STREAM, (IRichBolt) flowLoaderSpout, 1);
      else
        throw new RuntimeException("The component for rules is not valid. Must be IRichSpout or IRichBolt");

      //还有一个时钟的Spout.可以用于定时或者sleep.
      builder.setSpout("tick", new TickSpout(1000), 1);

      //初始化Bolt,启动Flow
      //虽然Flow流是静态信息,但是事件是动态的.我们要把事件通过Flow流转换为新的事件(比如filter,select),或者进行聚合计算
      builder.setBolt(INITIALIZER, new FlowInitializerBolt(), parallelismHint)  // kicks off a flow determining where to start
              //事件流可以通过shuffle负载到Bolt的Tasks上
              .localOrShuffleGrouping(EVENT)
                      //Flow流通过All Grouping策略发射给Bolt的每个Task
              .allGrouping(FLOW_LOADER_STREAM, FLOW_LOADER_STREAM);

      //CEP Bolt Definition
      declarebolt(builder, FILTER, new FilterBolt(), parallelismHint, true);
      declarebolt(builder, SELECT, new SelectorBolt(), parallelismHint, true);
      declarebolt(builder, PARTITION, new PartitionBolt(), parallelismHint, true);
      declarebolt(builder, SWITCH, new SwitchBolt(), parallelismHint, true);
      declarebolt(builder, AGGREGATE, new AggregatorBolt(), parallelismHint, true);
      declarebolt(builder, JOIN, new JoinBolt(), parallelismHint, true);
      declarebolt(builder, EACH, new EachBolt(), parallelismHint, true);
      declarebolt(builder, SORT, new SortBolt(), parallelismHint, true);
      declarebolt(builder, SPLIT, new SplitBolt(), parallelismHint, true);
      declarebolt(builder, OUTPUT, outputBolt, parallelismHint, false);

      return builder;
  }

  /**
   * 声明一个Bolt. Bolt的grouping分组策略,一般和前一个Bolt/Spout有关,这样组成一共DAG. 比如WordCountTopology: A->B->C
   * builder.setSpout(SENTENCE_SPOUT_ID, new SentenceSpout1())                                              A.数据源,获得句子
   * builder.setBolt(SPLIT_BOLT_ID, new SplitSentenceBolt()).shuffleGrouping(SENTENCE_SPOUT_ID)             B.对句子按照空格分隔
   * builder.setBolt(COUNT_BOLT_ID, new WordCountBolt()).fieldsGrouping(SPLIT_BOLT_ID, new Fields("word"))  C.单词计数
   *
   * 这里用链式调用定义了多个分组策略,并且每个自定义的Bolt都有所有其他Bolt的component-id.
   * 以FilterBolt为例:
   * localOrShuffleGrouping(INITIALIZER, FILTER)
   * localOrShuffleGrouping(FILTER, FILTER)
   * fieldsGrouping(PARTITION, FILTER, new Fields(FLOW_ID, PARTITION)
   * localOrShuffleGrouping(AGGREGATE, FILTER)
   * localOrShuffleGrouping(SELECT, FILTER)
   * FilterBolt会接收FlowInitializerBolt,FilterBolt,PartitionBolt,AggregatorBolt,SelectorBolt通过shuffle发射的Tuple.
   *
   * 为什么每个Bolt都要接收其他所有Bolt的Tuple?而不是像WordCount那样只有一个输入呢?
   * 因为自定义的Flow中Stream的FlowOp是没有规律而言的,可以随便组合FlowOp. 所以最好的办法就是把其他所有FlowOp对应的Bolt都加进来
   *
   * 虽然这里每个Bolt都定义了其他所有Bolt作为分组策略,但是实际中要以Flow中Stream的FlowOp组合顺序来构成Storm的DAG.
   * 并不是说这么定义了,那么在实际的业务开发中,所有的Bolt之间并不是都能互相发射Tuple的.
   * 比如一个Stream的顺序是Filter->Select->Partition->Aggregate. 那么实际的DAG图就是:
   * FlowInitializerBolt -> FilterBolt -> SelectBolt -> PartitionBolt -> AggregateBolt
   * 不能说因为这里SelectBolt定义了其他所有的Grouping策略,FlowInitializerBolt就能直接发射Tuple给SelectBolt处理了,不对的,是有顺序的,才叫DAG.
   *
   * 实现方式: 通过collector.emit(stream-id,tuple)的第一个参数stream-id来发送tuple到对应stream-id的Bolt上.
   */
  private static void declarebolt(TopologyBuilder builder, String boltName, IRichBolt bolt, int parallelism, boolean control) {
      BoltDeclarer declarer = builder.setBolt(boltName, bolt, parallelism)
          //XXXGrouping的两个参数: component-id, stream-id
          .allGrouping(FLOW_LOADER_STREAM, FLOW_LOADER_STREAM)
          .allGrouping("tick", "tick")
          .localOrShuffleGrouping(INITIALIZER, boltName)
          .localOrShuffleGrouping(FILTER, boltName)
          .fieldsGrouping(PARTITION, boltName, new Fields(FLOW_ID, PARTITION))    // guaranteed partitions will always group the same flow for flows that have joins with default partitions.
          .localOrShuffleGrouping(AGGREGATE, boltName)
          .localOrShuffleGrouping(SELECT, boltName)
          .localOrShuffleGrouping(EACH, boltName)
          .localOrShuffleGrouping(SORT, boltName)
          .localOrShuffleGrouping(SWITCH, boltName)
          .localOrShuffleGrouping(SPLIT, boltName)
          .localOrShuffleGrouping(JOIN, boltName);


          if(control) {
            // control stream is all-grouped
            //component-id和上面一样,但是stream-id添加了control前缀
            declarer.allGrouping(INITIALIZER, BROADCAST_STREAM + boltName)
                    .allGrouping(FILTER, BROADCAST_STREAM + boltName)
                    .allGrouping(PARTITION, BROADCAST_STREAM + boltName)
                    .allGrouping(AGGREGATE, BROADCAST_STREAM + boltName)
                    .allGrouping(SELECT, BROADCAST_STREAM + boltName)
                    .allGrouping(EACH, BROADCAST_STREAM + boltName)
                    .allGrouping(SORT, BROADCAST_STREAM + boltName)
                    .allGrouping(SWITCH, BROADCAST_STREAM + boltName)
                    .allGrouping(SPLIT, BROADCAST_STREAM + boltName)
                    .allGrouping(JOIN, BROADCAST_STREAM + boltName);

          }
  }

  public static Fields fields = new Fields(FLOW_ID, EVENT, FLOW_OP_IDX, STREAM_NAME, LAST_STREAM);
  public static Fields partitionFields = new Fields(FLOW_ID, EVENT, FLOW_OP_IDX, STREAM_NAME, PARTITION, LAST_STREAM);

  /**
   * Storm中GeneralTopologyContext的componentToStreamToFields表示: component包含哪些streams, 每个stream包含哪些fields
   * 分组策略定义了component-id -> stream-id
   * declareStream定义了stream-id -> Fields
   */
  public static void declareOutputStreams(OutputFieldsDeclarer declarer, Fields fields) {
      //由于设置Grouping策略时使用了component-id,stream-id. 所以声明输出字段时,也要加上stream-id
      declarer.declareStream(PARTITION, fields);
      declarer.declareStream(FILTER, fields);
      declarer.declareStream(SELECT, fields);
      declarer.declareStream(AGGREGATE, fields);
      declarer.declareStream(SWITCH, fields);
      declarer.declareStream(SORT, fields);
      declarer.declareStream(JOIN, fields);
      declarer.declareStream(SPLIT, fields);
      declarer.declareStream(EACH, fields);
      declarer.declareStream(OUTPUT, fields);

      declarer.declareStream(BROADCAST_STREAM + PARTITION, fields);
      declarer.declareStream(BROADCAST_STREAM + FILTER, fields);
      declarer.declareStream(BROADCAST_STREAM + SELECT, fields);
      declarer.declareStream(BROADCAST_STREAM + AGGREGATE, fields);
      declarer.declareStream(BROADCAST_STREAM + SWITCH, fields);
      declarer.declareStream(BROADCAST_STREAM + SORT, fields);
      declarer.declareStream(BROADCAST_STREAM + JOIN, fields);
      declarer.declareStream(BROADCAST_STREAM + EACH, fields);
      declarer.declareStream(BROADCAST_STREAM + SPLIT, fields);
  }
}
