package org.calrissian.flowmix.api.storm.spout;

import java.util.Collection;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import org.calrissian.flowmix.api.Flow;

import static org.calrissian.flowmix.core.Constants.FLOW_LOADER_STREAM;

/**
 * 实现Storm的BaseRichSpout.
 * OutputFieldsDeclarer.declareStream和SpoutOutputCollector.emit的stream-id都是FLOW_LOADER_STREAM
 * 输出字段为Fields封装的flows, 输出值为Values封装的单个对象flows
 */
public abstract class FlowLoaderBaseSpout extends BaseRichSpout {

  @Override
  public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
    //outputFieldsDeclarer.declare(new Fields("flows"));
    //指定stream-id,好让topology中当前spout的下一个bolt得到这个stream-id
    outputFieldsDeclarer.declareStream(FLOW_LOADER_STREAM, new Fields("flows"));
  }

  //Spout的下一个组件是Bolt,如果Bolt的SourceStreamId是FLOW_LOADER_STREAM,这个Bolt的上一个是Spout数据源
  //如果Bolt的Source StreamId不是FLOW_LOADER_STREAM,说明它的上一个还是Bolt.
  //因为在Storm中,Spout和Bolt都可以发送Tuple给Bolt.
  protected void emitFlows(SpoutOutputCollector collector, Collection<Flow> flows) {
    //flow是传给Spout的流程定义对象.Flow中定义了FlowOp操作符.
    collector.emit(FLOW_LOADER_STREAM, new Values(flows));
  }
}
