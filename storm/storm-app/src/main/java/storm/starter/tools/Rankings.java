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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import java.io.Serializable;
import java.util.Collections;
import java.util.List;

public class Rankings implements Serializable {

  private static final long serialVersionUID = -1549827195410578903L;
  private static final int DEFAULT_COUNT = 10;

  private final int maxSize;
  //Rankings代表topN一共N个对象. 其中每个对象都是一个Rankable对象,即待排序的项
  private final List<Rankable> rankedItems = Lists.newArrayList();

  public Rankings() {
    this(DEFAULT_COUNT);
  }

  public Rankings(int topN) {
    if (topN < 1) {
      throw new IllegalArgumentException("topN must be >= 1");
    }
    maxSize = topN;
  }

  /**
   * Copy constructor.
   * @param other
   */
  public Rankings(Rankings other) {
    this(other.maxSize());
    updateWith(other);
  }

  /**
   * @return the maximum possible number (size) of ranked objects this instance can hold
   */
  public int maxSize() {
    return maxSize;
  }

  /**
   * @return the number (size) of ranked objects this instance is currently holding
   */
  public int size() {
    return rankedItems.size();
  }

  /**
   * The returned defensive copy is only "somewhat" defensive.  We do, for instance, return a defensive copy of the
   * enclosing List instance, and we do try to defensively copy any contained Rankable objects, too.  However, the
   * contract of {@link storm.starter.tools.Rankable#copy()} does not guarantee that any Object's embedded within
   * a Rankable will be defensively copied, too.
   *
   * 获取所有项
   * @return a somewhat defensive copy of ranked items
   */
  public List<Rankable> getRankings() {
    List<Rankable> copy = Lists.newLinkedList();
    for (Rankable r: rankedItems) {
      copy.add(r.copy());
    }
    return ImmutableList.copyOf(copy);
  }

  //排序所有项
  public void updateWith(Rankings other) {
    for (Rankable r : other.getRankings()) {
      updateWith(r);
    }
  }

  /**
   * 排序某一项
   *
   * 上一级的blot会定期的发送某个时间窗口的(obj, count), 所以obj之间的排序是在不断变化的
   * 1. 替换已有的, 或新增rankable对象(包含obj, count)
   * 2. 重新排序(Collections.sort)
   * 3. 由于只需要topN, 所以大于maxsize的需要删除
   * @param r
   */
  public void updateWith(Rankable r) {
    synchronized(rankedItems) {
      addOrReplace(r);
      rerank();
      shrinkRankingsIfNeeded();
    }
  }

  //添加或替换一项
  private void addOrReplace(Rankable r) {
    //找出排名在第几个位置
    Integer rank = findRankOf(r);
    if (rank != null) {
      //设置为第几名: 替换-replace
      rankedItems.set(rank, r);
    }
    //没有在排名中, 直接添加到列表中-add
    else {
      rankedItems.add(r);
    }
  }

  //排名,在topN中是第几项
  private Integer findRankOf(Rankable r) {
    Object tag = r.getObject();
    for (int rank = 0; rank < rankedItems.size(); rank++) {
      Object cur = rankedItems.get(rank).getObject();
      if (cur.equals(tag)) {
        return rank;
      }
    }
    return null;
  }

  private void rerank() {
    Collections.sort(rankedItems);
    Collections.reverse(rankedItems);
  }

  private void shrinkRankingsIfNeeded() {
    if (rankedItems.size() > maxSize) {
      rankedItems.remove(maxSize);
    }
  }

  /**
   * Removes ranking entries that have a count of zero.
   */
  public void pruneZeroCounts() {
    int i = 0;
    while (i < rankedItems.size()) {
      if (rankedItems.get(i).getCount() == 0) {
        rankedItems.remove(i);
      }
      else {
        i++;
      }
    }
  }

  public String toString() {
    return rankedItems.toString();
  }

  /**
   * Creates a (defensive) copy of itself.
   */
  public Rankings copy() {
    return new Rankings(this);
  }
}