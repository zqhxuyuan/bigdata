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
package org.calrissian.flowmix.api.builder;

import java.util.ArrayList;
import java.util.List;

import org.calrissian.flowmix.api.Order;
import org.calrissian.flowmix.api.Policy;
import org.calrissian.flowmix.core.model.op.FlowOp;
import org.calrissian.flowmix.core.model.op.PartitionOp;
import org.calrissian.flowmix.core.model.op.SortOp;
import org.calrissian.mango.domain.Pair;

import static java.util.Collections.EMPTY_LIST;

public class SortBuilder extends AbstractOpBuilder {

    //可以指定多个排序字段
    private List<Pair<String,Order>> sortBy = new ArrayList<Pair<String, Order>>();

    private Policy evictionPolicy;
    private long evictionThreshold = -1;

    private Policy triggerPolicy;
    private long triggerThreshold = -1;

    private boolean clearOnTrigger = false;
    private boolean progressive = false;

    public SortBuilder(StreamBuilder flowOpsBuilder) {
        super(flowOpsBuilder);
    }

    //字段, 以及怎么对这个字段排序:升序还是降序
    public SortBuilder sortBy(String field, Order order) {
      sortBy.add(new Pair<String, Order>(field, order));
      return this;
    }

    public SortBuilder sortBy(String field) {
      sortBy.add(new Pair<String, Order>(field, Order.ASC));
      return this;
    }

    //渐进: 假设窗口内的事件有5条(evictionThreshold=5), 则每当新增一条记录,就失效一条最旧的记录.
    //比如1,2,3,4,5 -> 2,3,4,5,6 -> 3,4,5,6,7 -> ....
    public SortBuilder progressive(long countEvictionThreshold) {
      evictionPolicy = Policy.COUNT;    //失效策略为COUNT,添加一条新记录,就失效一条旧记录.
      evictionThreshold = countEvictionThreshold;
      triggerPolicy = Policy.COUNT;     //触发策略为COUNT
      triggerThreshold = 1;             //每增加一条记录,就触发一次动作.
      clearOnTrigger = false;           //不会把旧的状态清空.因为这个窗口内除了被弹出的最旧记录外,其他记录都要保留
      progressive = true;
      return this;
    }

    //滚动: 假设triggerPolicy=Time,triggerThreshold=5s. 则时间窗口为1-5, 6-10, 11-15... 跟AggregatorBolt有点像(每隔5秒统计一次)??
    //https://developer.ibm.com/streamsdev/2014/05/06/spl-tumbling-windows-explained/
    //http://reactivex.io/documentation/operators/window.html
    public SortBuilder tumbling(Policy policy, long threshold) {
      clearOnTrigger = true;        //清空
      triggerPolicy = policy;       //触发策略:窗口内的数量达到阈值,或者时间满足
      triggerThreshold = threshold;
      evictionPolicy = null;        //下一个事件进来时,原先窗口内的所有记录全部被清空.所以失效策略不是COUNT或TIME任何一种
      evictionThreshold = -1;
      return this;
    }

    //topN: 失效策略为COUNT,N可以看做是队列的大小,当超过队列大小时,移除最小值.
    public SortBuilder topN(int n, Policy triggerPolicy, long triggerThreshold, boolean flushOnTrigger) {
      evictionPolicy = Policy.COUNT;
      evictionThreshold = n;
      this.triggerPolicy = triggerPolicy; //触发策略可以是Time或者Count.表示时间到达时,或者数量到达时.
      this.triggerThreshold = triggerThreshold;
      this.clearOnTrigger = flushOnTrigger;
      return this;
    }

  @Override
    public StreamBuilder end() {

      if(sortBy.size() == 0)
        throw new RuntimeException("Sort operator needs at least one field name to sort by");

      if(clearOnTrigger == false && (evictionPolicy == null || evictionThreshold == -1))
        throw new RuntimeException("Sort operator needs an eviction policy and threshold");

      if(triggerPolicy == null || triggerThreshold == -1)
        throw new RuntimeException("Sort operator needs a trigger policy and threshold");

      List<FlowOp> flowOpList = getStreamBuilder().getFlowOpList();
      FlowOp op = flowOpList.size() == 0 ? null : flowOpList.get(flowOpList.size()-1);
      if(op == null || !(op instanceof PartitionOp))
        flowOpList.add(new PartitionOp(EMPTY_LIST));

      getStreamBuilder().addFlowOp(new SortOp(sortBy, clearOnTrigger, evictionPolicy, evictionThreshold,
              triggerPolicy, triggerThreshold, progressive));
      return getStreamBuilder();
    }
}
