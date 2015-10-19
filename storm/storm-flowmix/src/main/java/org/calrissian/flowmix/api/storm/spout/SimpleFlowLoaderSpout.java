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
package org.calrissian.flowmix.api.storm.spout;

import java.util.Collection;
import java.util.Map;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import org.calrissian.flowmix.api.Flow;

/**
 * A spout to load a predefined set of {@link Flow} objects. This is the most basic (and very static) way
 * of getting a set of flows into a topology.
 * 加载预先定义好的Flow对象集合,Storm的Topology需要获取到这些Flows.
 * 在构造函数中初始化flows,nextTuple中通过emit将flows发射出去
 */
public class SimpleFlowLoaderSpout extends FlowLoaderBaseSpout {

    //Flow的Spout可以有多个,比如两个Flow进行join
    Collection<Flow> flows;
    long pauseBetweenLoads = -1;

    boolean loaded = false;

    SpoutOutputCollector collector;

    //构造函数由App调用
    public SimpleFlowLoaderSpout(Collection<Flow> flows, long pauseBetweenLoads) {
        this.flows = flows;
        this.pauseBetweenLoads = pauseBetweenLoads;
    }

    //open方法的初始化由Storm框架调用
    @Override
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        this.collector = spoutOutputCollector;
    }

    //数据源每当收到一条数据,就往Topology中发射
    @Override
    public void nextTuple() {
      if(!loaded || pauseBetweenLoads > -1) {
        emitFlows(collector, flows);
        loaded = true;

        if(pauseBetweenLoads > -1) {
          try {
            Thread.sleep(pauseBetweenLoads);
          } catch (InterruptedException e) {
            throw new RuntimeException(e);
          }
        }
      }
    }
}
