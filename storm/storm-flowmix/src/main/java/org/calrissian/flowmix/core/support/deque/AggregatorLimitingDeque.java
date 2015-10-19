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
package org.calrissian.flowmix.core.support.deque;

import org.calrissian.flowmix.api.Aggregator;
import org.calrissian.flowmix.core.support.window.WindowItem;

//聚合策略, 基于容量的双端队列. 比如限制窗口的大小为1000项.则窗口内的事件数量超过阈值时,移除队列头部元素
public class AggregatorLimitingDeque extends LimitingDeque<WindowItem> {

  Aggregator aggregator;

  public AggregatorLimitingDeque(long maxSize, Aggregator aggregator) {
    super(maxSize);
    this.aggregator = aggregator;
  }

  //向队列尾部添加一个窗口项
  @Override
  public boolean offerLast(WindowItem windowItem) {
    //如果队列满了,每往队列尾部添加一项,使第一项时效
    if(size() == getMaxSize())
      aggregator.evicted(getFirst());

    return super.offerLast(windowItem);
  }
}
