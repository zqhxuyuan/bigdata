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
package org.calrissian.flowmix.core.support.window;

import java.util.Collection;

import org.calrissian.flowmix.api.Aggregator;
import org.calrissian.flowmix.core.model.event.AggregatedEvent;
import org.calrissian.flowmix.core.support.deque.AggregatorLimitingDeque;
import org.calrissian.mango.domain.event.Event;

/**
 * 基于窗口的聚合. 由于包含有聚合Aggregator,所以会调用实际的聚合方法.
 */
public class AggregatorWindow extends Window {

    //聚合动作,计算策略
    private Aggregator aggregator;

    public AggregatorWindow(Aggregator aggregator, String groupedIndex, long size) {
      //基于容量的双端队列
      events = new AggregatorLimitingDeque(size, aggregator);
      this.groupedIndex = groupedIndex;
      this.aggregator = aggregator;
    }

    public AggregatorWindow(Aggregator aggregator, String groupedIndex) {
        super(groupedIndex);
        this.aggregator = aggregator;
    }

    @Override
    public WindowItem add(Event event, String previousStream) {
        WindowItem item = super.add(event, previousStream);
        aggregator.added(item);
        return item;
    }

    @Override
    public WindowItem expire() {
        WindowItem item = super.expire();
        aggregator.evicted(item);
        return item;
    }

    @Override
    public void clear() {
      while(size() > 0)
        expire();
    }

    /**
     * Used for age-based expiration(time-based)
     * 基于时间的失效策略: events.peek()选择队列中的第一个元素,因为基于时间的事件是按照时间顺序加入到队列中.
     * 如果第一个事件过期了,则从队列中弹出该事件,并调用aggregator.evicted使该事件失效,并继续判断下一个事件.
     * 如果第一个事件没有过期,则后面的事件也一定不会过期.
     */
    public void timeEvict(long thresholdInSeconds) {
        while(events != null && events.peek() != null &&
                (System.currentTimeMillis() - events.peek().getTimestamp()) >= (thresholdInSeconds * 1000)) {
          WindowItem item = events.poll();
          aggregator.evicted(item);
        }
    }

    public Collection<AggregatedEvent> getAggregate() {
        return aggregator.aggregate();
    }
}
