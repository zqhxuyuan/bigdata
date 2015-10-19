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
package org.calrissian.flowmix.api;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

import org.calrissian.flowmix.core.model.event.AggregatedEvent;
import org.calrissian.flowmix.core.support.window.WindowItem;
import org.calrissian.flowmix.exceptions.FlowmixException;

/**
 * An aggregator over a progressive(渐进)/tumbling(倒塌) window allows aggregate values like
 * sums and averages to be maintained for some window at some point in time
 * without the whole window being available at any point in time.
 *
 * This is very useful for associative(传递,关联) algorithms that can be implemented
 * without the entire dataset being available. Often this is good for reduce
 * functions that can summarize(总结) a dataset without the need to see each
 * individual point(个体).
 *
 * Multiple events can be returned as the aggregate if necessary, this means
 * multiple aggregates could be maintained inside and emitted separately (i.e.
 * sum and count and sumsqaure, and average).
 */
public interface Aggregator extends Serializable {

    //聚合, 需要分组
    public static final String GROUP_BY = "groupBy";
    public static final String GROUP_BY_DELIM = "\u0000";

    //配置信息, 比如对哪些字段进行分组, 分组之后的聚合算法(求和,计数等)
    void configure(Map<String, String> configuration);

    //加入聚合统计的项, 在窗口之内的items进行聚合
    void added(WindowItem item);

    //过期项
    void evicted(WindowItem item);

    //聚合动作. 可以返回多个聚合事件.
    List<AggregatedEvent> aggregate();
}
