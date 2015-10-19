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
import org.calrissian.flowmix.api.aggregator.AssocCountAggregator;
import org.calrissian.flowmix.api.aggregator.DistCountAggregator;
import org.calrissian.flowmix.api.builder.FlowBuilder;
import org.calrissian.flowmix.example.support.ExampleRunner;
import org.calrissian.flowmix.example.support.ExampleRunner2;
import org.calrissian.flowmix.example.support.FlowProvider;

import java.util.List;

import static java.util.Arrays.asList;

public class AssocCountExample implements FlowProvider {
  @Override
  public List<Flow> getFlows() {
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

  public static void main(String args[]) {
    new ExampleRunner2(new AssocCountExample()).run();
  }
}
