/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package storm.starter.tools;

import java.io.Serializable;
import java.util.Map;

/**
 * This class counts objects in a sliding window fashion.
 * <p/>
 * It is designed 1) to give multiple "producer" threads write access to the counter, i.e. being able to increment
 * counts of objects, and 2) to give a single "consumer" thread (e.g. {@link PeriodicSlidingWindowCounter}) read access
 * to the counter. Whenever the consumer thread performs a read operation, this class will advance the head slot of the
 * sliding window counter. This means that the consumer thread indirectly controls where writes of the producer threads
 * will go to. Also, by itself this class will not advance the head slot.
 * <p/>
 * A note for analyzing data based on a sliding window count: During the initial <code>windowLengthInSlots</code>
 * iterations, this sliding window counter will always return object counts that are equal or greater than in the
 * previous iteration. This is the effect of the counter "loading up" at the very start of its existence. Conceptually,
 * this is the desired behavior.
 * <p/>
 * To give an example, using a counter with 5 slots which for the sake of this example represent 1 minute of time each:
 * 下面的例子假设每隔1分钟,统计过去5分钟的数量.
 * <p/>
 * <pre>
 * {@code
 * Sliding window counts of an object X over time
 *
 * Minute (timeline):
 * 1    2   3   4   5   6   7   8
 *
 * Observed counts per minute: 在每一分钟上发生的计数
 * 1    1   1   1   0   0   0   0
 *
 * Counts returned by counter: 在某个时间点(每隔一分钟)上观察到的过去5分钟的计数值
 * 1    2   3   4   4   3   2   1
 * }
 * </pre>
 * <p/>
 * As you can see in this example, for the first <code>windowLengthInSlots</code> (here: the first five minutes) the
 * counter will always return counts equal or greater than in the previous iteration (1, 2, 3, 4, 4). This initial load
 * effect needs to be accounted for whenever you want to perform analyses such as trending topics; otherwise your
 * analysis algorithm might falsely identify the object to be trending as the counter seems to observe continuously
 * increasing counts. Also, note that during the initial load phase <em>every object</em> will exhibit increasing
 * counts.
 * <p/>
 * On a high-level, the counter exhibits the following behavior: If you asked the example counter after two minutes,
 * "how often did you count the object during the past five minutes?", then it should reply "I have counted it 2 times
 * in the past five minutes", implying that it can only account for the last two of those five minutes because the
 * counter was not running before that time.
 *
 * SlidingWindowCounter只是对SlotBasedCounter做了进一步的封装, 通过headSlot和tailSlot提供sliding window的概念
 *
 * A. incrementCount, 只能对headSlot进行increment, 其他slot作为窗口中的历史数据
 * B. 核心的操作为getCountsThenAdvanceWindow
 *   1. 取出Map<T, Long> counts, 对象和窗口内所有slots求和值的map
 *   2. 调用wipeZeros, 删除已经不被使用的obj, 释放空间
 *   3. 最重要的一步, 清除tailSlot, 并advanceHead, 以实现滑动窗口
 * C. advanceHead的实现, 如何在数组实现循环的滑动窗口
 *
 * @param <T> The type of those objects we want to count.
 */
public final class SlidingWindowCounter<T> implements Serializable {

    private static final long serialVersionUID = -2645063988768785810L;

    private SlotBasedCounter<T> objCounter;
    private int headSlot;   // head is the current slot. why not called currentSlot?
    private int tailSlot;   // tail is the next slot. Why not called nextSlot?
    private int windowLengthInSlots;

    //假设每隔3分钟统计过去9分钟的数据,则slots=9/3=3
    //head=0, tail=1%3=1. tail是head的next slot.
    public SlidingWindowCounter(int windowLengthInSlots) {
        if (windowLengthInSlots < 2) {
            throw new IllegalArgumentException(
                    "Window length in slots must be at least two (you requested " + windowLengthInSlots + ")");
        }
        this.windowLengthInSlots = windowLengthInSlots;
        this.objCounter = new SlotBasedCounter<T>(this.windowLengthInSlots);

        //slots的总数不变,实现类似环形缓冲区的效果.
        this.headSlot = 0;
        this.tailSlot = slotAfter(headSlot);
    }

    //tuple每次总是在headSlot上进行的. 因为一旦一个slot对应的时间窗口过去之后, head会往前移动一个slot.这样head还是指向当前写入tuple所在的时间窗口.
    //注意按照例子9min的窗口大小,滑动窗口为3min. 则9min的时间片会被分为0-3,3-6,6-9.
    //当过了第一个slot=[0,3)之后, 在3-6min写入的数据只会写入到第二个slot中,不会写入到旧的时间窗口对应的slot中.
    public void incrementCount(T obj) {
        objCounter.incrementCount(obj, headSlot);
    }

    /**
     * Return the current (total) counts of all tracked objects, then advance the window.
     * <p/>
     * Whenever this method is called, we consider the counts of the current sliding window to be available to and
     * successfully processed "upstream" (i.e. by the caller). Knowing this we will start counting any subsequent
     * objects within the next "chunk" of the sliding window.
     *
     * @return The current (total) counts of all tracked objects.
     */
    public Map<T, Long> getCountsThenAdvanceWindow() {
        Map<T, Long> counts = objCounter.getCounts();
        //释放obj的totalCounts=0的空间
        objCounter.wipeZeros();
        //清空下一个slot的所有数据,因为这个slot已经到期了,接下去我们要在下一个slot上继续.
        objCounter.wipeSlot(tailSlot);
        //headSlot和tailSlot分别往前移动一个
        advanceHead();
        return counts;
    }

    /**
     * head tail
     * 0    1
     * 1    2
     * 2    0
     * 0    1
     * 1    2
     */
    private void advanceHead() {
        headSlot = tailSlot;
        tailSlot = slotAfter(tailSlot);
    }

    private int slotAfter(int slot) {
        return (slot + 1) % windowLengthInSlots;
    }

}
