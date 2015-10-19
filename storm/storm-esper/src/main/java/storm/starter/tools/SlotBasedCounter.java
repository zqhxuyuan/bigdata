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
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * This class provides per-slot counts of the occurrences of objects.
 * <p/>
 * It can be used, for instance, as a building block for implementing sliding window counting of objects.
 *
 * 基于slot的counter模板类,可以指定被计数对象的类型T. 实现计数对象和一组slot(用long数组实现)的map, 并可以对任意slot做increment或reset等操作
 *
 * @param <T> The type of those objects we want to count.
 */
public final class SlotBasedCounter<T> implements Serializable {

    private static final long serialVersionUID = 4858185737378394432L;

    //每个obj都对应于一个大小为numSlots的long数组, 所以对每个obj可以计numSlots个数
    //假设9min的数据,每隔3min计算一次. 则一共有3个slots.在一共3次的时间窗口中,每个3min的时间窗口只统计自己这个窗口内的count.
    //最后要计算9min某个obj的总和,只要把这三个slots中属于这个obj的count相加即可: objToCounts.get(obj).sum()
    private final Map<T, long[]> objToCounts = new HashMap<T, long[]>();
    private final int numSlots;

    public SlotBasedCounter(int numSlots) {
        if (numSlots <= 0) {
            throw new IllegalArgumentException("Number of slots must be greater than zero (you requested " + numSlots + ")");
        }
        this.numSlots = numSlots;
    }

    /**
     * 针对obj, 在指定的槽位上的计数+1
     * 1. 因为一个obj可以存在一共numSlots的槽位中
     * 2. 一个slot可以存放多个obj
     * @param obj
     * @param slot
     */
    public void incrementCount(T obj, int slot) {
        long[] counts = objToCounts.get(obj);
        if (counts == null) {
            counts = new long[this.numSlots];
            objToCounts.put(obj, counts);
        }
        counts[slot]++;
    }

    // 获取当前对象obj在指定slot上的计数值
    public long getCount(T obj, int slot) {
        long[] counts = objToCounts.get(obj);
        if (counts == null) {
            return 0;
        }
        else {
            return counts[slot];
        }
    }

    // 获取所有slots中obj和counter的全局计数器
    public Map<T, Long> getCounts() {
        Map<T, Long> result = new HashMap<T, Long>();
        //每个Object
        for (T obj : objToCounts.keySet()) {
            result.put(obj, computeTotalCount(obj));
        }
        return result;
    }

    // 获取一个对象在所有slots中的计数值
    private long computeTotalCount(T obj) {
        long[] curr = objToCounts.get(obj);
        long total = 0;
        for (long l : curr) {
            total += l;
        }
        return total;
    }

    /**
     * Reset the slot count of any tracked objects to zero for the given slot.
     * 针对给定的slot, 把这个slot上的所有对象的计数器重置
     *
     * @param slot
     */
    public void wipeSlot(int slot) {
        for (T obj : objToCounts.keySet()) {
            resetSlotCountToZero(obj, slot);
        }
    }

    private void resetSlotCountToZero(T obj, int slot) {
        long[] counts = objToCounts.get(obj);
        counts[slot] = 0;
    }

    private boolean shouldBeRemovedFromCounter(T obj) {
        return computeTotalCount(obj) == 0;
    }

    /**
     * Remove any object from the counter whose total count is zero (to free up memory).
     */
    public void wipeZeros() {
        Set<T> objToBeRemoved = new HashSet<T>();
        for (T obj : objToCounts.keySet()) {
            if (shouldBeRemovedFromCounter(obj)) {
                objToBeRemoved.add(obj);
            }
        }
        for (T obj : objToBeRemoved) {
            objToCounts.remove(obj);
        }
    }

}
