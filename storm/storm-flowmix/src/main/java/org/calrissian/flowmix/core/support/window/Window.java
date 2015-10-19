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

import org.calrissian.flowmix.core.support.deque.LimitingDeque;
import org.calrissian.mango.domain.event.Event;

import java.util.*;
import java.util.concurrent.LinkedBlockingDeque;

import static java.lang.System.currentTimeMillis;
import static org.apache.commons.lang.StringUtils.join;

/**
 * 窗口, 一个窗口内有很多事件, 用双端队列events保存这个窗口内的所有事件
 */
public class Window {

    protected String groupedIndex;        // a unique key given to the groupBy field/value combinations in the window buffer

    protected Deque<WindowItem> events;     // using standard array list for proof of concept.
                                             // Circular buffer needs to be used after concept is proven
    protected int triggerTicks = 0;     //计数器:在基于时间的触发策略中,在tick中增加计数器,表示增加了一秒; 在基于次数的触发策略中,在非tick中增加计数器,表示增加了一条事件

    public Window() {}

    /**
     * A progressive window buffer which automatically evicts by count
     * 渐进的窗口缓冲区, 根据数量进行失效. 即窗口内的事件数量有个阈值, 当达到阈值时, 移除最早加入窗口内的事件.
     */
    public Window(String groupedIndex, long size) {
        //this调用初始化一次无容量的events,这里又要覆盖为有容量的队列
        //this(groupedIndex);
        this.groupedIndex = groupedIndex;
        events = new LimitingDeque<WindowItem>(size);
    }

    //大小无限制的队列
    public Window(String groupedIndex) {
        events = new LinkedBlockingDeque<WindowItem>();
        this.groupedIndex = groupedIndex;
    }

    //添加一个事件到窗口队列中,并返回这个事件
    public WindowItem add(Event event, String previousStream) {
        WindowItem item = new WindowItem(event, currentTimeMillis(), previousStream);
        events.add(item);
        return item;
    }

    /**
     * Used for age-based expiration 基于时间的失效策略.
     * 当前时间-事件的时间,超过阈值,说明这个事件已经时效了,则从窗口队列中移除.
     */
    public void timeEvict(long thresholdInSeconds) {
        while(events != null && events.peek() != null &&
                (System.currentTimeMillis() - events.peek().getTimestamp()) >= (thresholdInSeconds * 1000))
            events.poll();
    }

    /**
     * Returns the difference(in millis) between the HEAD & TAIL timestamps.
     */
    public long timeRange() {
        if(events.size() <= 1) return -1;
        return events.getLast().getTimestamp() - events.getFirst().getTimestamp();
    }

    /**
     * Used for count-based expiration 基于个数的失效
     */
    public WindowItem expire() {
        return events.removeLast();
    }

    public void resetTriggerTicks() {
        triggerTicks = 0;
    }

    public int getTriggerTicks() {
        return triggerTicks;
    }

    public void incrTriggerTicks() {
        triggerTicks += 1;
    }

    public String getGroupedIndex() {
        return groupedIndex;
    }
    public Iterable<WindowItem> getEvents() {
        return events;
    }

    public int size() {
        return events.size();
    }

    public void clear() {
        events.clear();
    }

    @Override
    public String toString() {
        return "Window{" +
                "groupedIndex='" + groupedIndex + '\'' +
                ", size=" + events.size() +
                ", events=" + events +
                ", triggertTicks=" + triggerTicks +
                '}';
    }
}
