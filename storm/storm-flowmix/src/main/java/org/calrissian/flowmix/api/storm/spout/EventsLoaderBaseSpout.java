package org.calrissian.flowmix.api.storm.spout;

import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;

//和FlowLoaderBaseSpout一样,发射的是事件Tuple.不过没有stream-id.
public abstract class EventsLoaderBaseSpout extends BaseRichSpout {

  @Override
  public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
    outputFieldsDeclarer.declare(new Fields("event"));
  }

}
