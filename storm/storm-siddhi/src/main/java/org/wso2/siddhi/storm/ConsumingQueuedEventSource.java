/**
 * Copyright (c) 2005 - 2013, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 * <p/>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


package org.wso2.siddhi.storm;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.query.api.definition.StreamDefinition;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;

public class ConsumingQueuedEventSource implements QueuedEventSource {
    private static Log log = LogFactory.getLog(ConsumingQueuedEventSource.class);
    private final StreamDefinition streamDefinition;
    private String tracerPrefix = "";
    private ConcurrentLinkedQueue<Object[]> eventQueue;

    public ConsumingQueuedEventSource(StreamDefinition streamDefinition) {
        this.streamDefinition = streamDefinition;
        this.eventQueue = new ConcurrentLinkedQueue<Object[]>();
    }

    public StreamDefinition getStreamDefinition() {
        return streamDefinition;
    }

    public void consumeEvents(Object[][] events) {
        for (Object[] eventData : events) {
            eventQueue.offer(eventData);
        }

    }

    public void consumeEvents(Event[] events) {
        for (Event event : events) {
            eventQueue.offer(event.getData());
        }
    }

    public void consumeEvent(Object[] eventData) {
        eventQueue.offer(eventData);
    }

    public void consumeEvent(Event event) {
        eventQueue.offer(event.getData());
    }

    @Override
    public Object[] getEvent() {
        return eventQueue.poll();
    }

    @Override
    public List<Object[]> getAllEvents() {
        List<Object[]> eventList = new ArrayList<Object[]>(StormProcessorConstants.MAX_BATCH_SIZE);
        Object[] currentEvent;
        int batchCount = 0;
        while ((currentEvent = eventQueue.poll()) != null && batchCount++ < StormProcessorConstants.MAX_BATCH_SIZE) {
            eventList.add(currentEvent);
        }

        return eventList;
    }

    @Override
    public String getStreamId() {
        return this.streamDefinition.getStreamId();
    }
}
