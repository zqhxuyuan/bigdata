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

import com.google.common.collect.Iterables;
import org.calrissian.flowmix.example.support.ExampleRunner;
import org.calrissian.flowmix.example.support.ExampleRunner2;
import org.calrissian.flowmix.example.support.FlowProvider;
import org.calrissian.flowmix.api.Flow;
import org.calrissian.flowmix.api.Policy;
import org.calrissian.flowmix.api.builder.FlowBuilder;
import org.calrissian.flowmix.api.Function;
import org.calrissian.mango.domain.event.BaseEvent;
import org.calrissian.mango.domain.event.Event;
import org.calrissian.mango.domain.Tuple;

import java.util.List;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
/**
 * A stream join example. The events for the left hand of the join (stream1) are collected into a window and the
 * right hand side is joined against the left hand side (that is, the tuples are merged with the right hand side).
 */
public class JoinExample implements FlowProvider {

 /**
  *  INPUT: source: join:42, stream: output, id: {}, [flow,
     BaseEvent{type='', id='20280593-6cad-476a-b988-4394de0ad017', timestamp=1442479237787, tuples=[
         Tuple{key='key1', value=val1, metadata={}},
         Tuple{key='key1', value=val1, metadata={}},
         Tuple{key='key2', value=val-2, metadata={}},
         Tuple{key='key2', value=val2, metadata={}},
         Tuple{key='key5', value=val-5, metadata={}},
         Tuple{key='key5', value=val5, metadata={}},
         Tuple{key='stream', value=stream1, metadata={}},
         Tuple{key='stream', value=stream2, metadata={}},
         Tuple{key='key3', value=val-3, metadata={}},
         Tuple{key='key3', value=val3, metadata={}},
         Tuple{key='key4', value=val-4, metadata={}},
         Tuple{key='key4', value=val4, metadata={}}
     ]}, 1, stream3, stream1]

    随机事件key2的值分别是val2, val-2. 所以两条记录关联时, 包含了val2和val-2.
    等价于下面的API:
    Event event = new BaseEvent()
    event.put(new Tuple("key2","val2"));
    event.put(new Tuple("key2","val-2"));

    对于Tuple中相同的key, 事件记录并不会覆盖, 而是组成:
    "key2", [Tuple("key2","val2"), Tuple("key2","val-2")]的形式.
    参考测试用例EventTupleTest.testRepeatTupleKey

    TODO: 对于相同key,value的Tuple, 目前EVENT API存在问题.
  */
  @Override
  public List<Flow> getFlows() {
    Flow flow = new FlowBuilder()
      .id("flow")
      .flowDefs()
        .stream("stream1")
            .each().function(new Function() {
              @Override
              public List<Event> execute(Event event) {
                Event newEvent = new BaseEvent(event.getId(), event.getTimestamp());
                newEvent.putAll(Iterables.concat(event.getTuples()));
                newEvent.put(new Tuple("stream", "stream1"));
                return singletonList(newEvent);
              }
            }).end()
        .endStream(false, "stream3")   // send ALL results to stream3 and not to standard output
        .stream("stream2")      // don't read any events from standard input
          .each().function(new Function() {
              @Override
              public List<Event> execute(Event event) {
                Event newEvent = new BaseEvent(event.getId(), event.getTimestamp());
                newEvent.putAll(Iterables.concat(event.getTuples()));
                newEvent.put(new Tuple("stream", "stream2"));
                return singletonList(newEvent);
              }
            }).end()
        .endStream(false, "stream3")
        .stream("stream3", false)
            .join("stream1", "stream2").evict(Policy.TIME, 5).end()
        .endStream()
      .endDefs()
    .createFlow();

    return asList(new Flow[]{flow});
  }

  public static void main(String args[]) {
    new ExampleRunner2(new JoinExample()).run();
  }
}
