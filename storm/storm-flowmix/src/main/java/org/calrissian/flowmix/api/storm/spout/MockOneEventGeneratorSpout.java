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

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Values;
import com.google.common.base.Preconditions;
import org.calrissian.flowmix.example.support.ExampleRunner;
import org.calrissian.flowmix.example.support.MockEvent;
import org.calrissian.mango.domain.event.Event;

import java.util.Collection;
import java.util.Iterator;
import java.util.Map;

import static java.util.Collections.singleton;

public class MockOneEventGeneratorSpout extends EventsLoaderBaseSpout {

    private int sleepBetweenEvents = 1000;
    private SpoutOutputCollector collector;

    public MockOneEventGeneratorSpout(int sleepBetweenEvents) {
        this.sleepBetweenEvents = sleepBetweenEvents;
    }

    @Override
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        this.collector = spoutOutputCollector;
    }

    @Override
    public void nextTuple() {
        Event event = MockEvent.mockOneEvent();
        collector.emit(new Values(singleton(event)));
        try {
            Thread.sleep(sleepBetweenEvents);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}
