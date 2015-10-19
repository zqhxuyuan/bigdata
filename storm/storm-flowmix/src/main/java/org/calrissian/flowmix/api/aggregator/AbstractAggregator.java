/*
 * Copyright 2015 The Calrissian Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.calrissian.flowmix.api.aggregator;

import static java.lang.System.currentTimeMillis;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import static java.util.UUID.randomUUID;
import org.apache.commons.lang.StringUtils;
import org.calrissian.flowmix.api.Aggregator;
import static org.calrissian.flowmix.api.Aggregator.GROUP_BY;
import static org.calrissian.flowmix.api.Aggregator.GROUP_BY_DELIM;
import org.calrissian.flowmix.core.model.event.AggregatedEvent;
import org.calrissian.flowmix.core.support.window.WindowItem;
import org.calrissian.flowmix.exceptions.FlowmixException;
import org.calrissian.mango.domain.Tuple;
import org.calrissian.mango.domain.event.BaseEvent;
import org.calrissian.mango.domain.event.Event;

/**
 *
 * Abstract aggregator for simple implementations
 *
 * @param <T> Aggregation result type 聚合之后的结果类型
 * @param <F> Field type 要聚合的字段类型
 */
public abstract class AbstractAggregator<T, F> implements Aggregator {

    public static final String OPERATED_FIELD = "operatedField";    // field to operate with
    public static final String OUTPUT_FIELD = "outputField";        // output field
    public static final String ASSOC_FIELD = "assocField";

    protected Map<String, Collection<Tuple>> groupedValues;         // grouped fields description
    protected String[] groupByFields;                               // fields to group by
    protected String operatedField;                                 // operated field name
    protected String outputField = getOutputField();                // output field set by implementation

    private String[] assocField; //关联字段,有两个,比如IP在账户上的关联个数.

    @Override
    public void configure(Map<String, String> configuration) {
        if (configuration.get(GROUP_BY) != null) {
            groupByFields = StringUtils.splitPreserveAllTokens(configuration.get(GROUP_BY), GROUP_BY_DELIM);
        }
        if (configuration.get(OUTPUT_FIELD) != null) {
            outputField = configuration.get(OUTPUT_FIELD);
        }
        if (configuration.get(OPERATED_FIELD) != null) {
            operatedField = configuration.get(OPERATED_FIELD);
        } else {
            //TODO: 一定要有一个操作字段operatedField? 如果是没有分组的count(*),operatedField是什么?
            //TODO: 多个分组条件的count,operatedField有几个?  operatedField和groupByFields一样吗?
            //throw new RuntimeException("Aggregator needs a field to operate it. Property missing: " + OPERATED_FIELD);
        }
        if (configuration.get(ASSOC_FIELD) != null) {
            assocField = StringUtils.splitPreserveAllTokens(configuration.get(ASSOC_FIELD), ",");
        }
    }

    //输出字段名称
    protected abstract String getOutputField();

    /**
     * 添加一项数据到窗口内, 对结果的影响, 比如count计数时,调用一次add,则计数器+1
     * added(WindowItem)会调用该抽象方法
     * @param fieldValue field value to work with after item is added to grouped values
     */
    public abstract void add(F... fieldValue);
    //public abstract void add(F fieldValue, F assocFieldValue);

    //删除窗口中的一项, 对结果的影响. 比如count计数时计数器-1.
    public abstract void evict(F fieldValue);

    /**
     * 开始聚合操作. added,evicted是添加或删除一项窗口中的数据对结果的影响.
     * 在窗口完整后, 对所有的项进行聚合操作,利用added,evicted的结果计算.
     * @return aggregation result provided by implementation
     */
    protected abstract T aggregateResult();

    /**
     * 假设根据year,age统计count:select year,age,count(*) from table group by year,age
     * 则groupByFields=[year,age], operateField=?
     * @param item
     */
    @Override
    public void added(WindowItem item){
        //第一个窗口项(事件)进来时满足条件.后面的窗口项因为groupedValues!=null,不满足条件.
        if (groupedValues == null && groupByFields != null) {
            groupedValues = new HashMap<String, Collection<Tuple>>();
            for (String group : groupByFields) {
                groupedValues.put(group, item.getEvent().getAll(group));
            }
        }
        if (item.getEvent().get(operatedField) != null) {
            try {
                //参数是事件记录中操作字段的值. 而不是事件记录本身.比如要聚合sum(key3),则我们要把key3的值取出来,作为sum的参数
                add(((F) item.getEvent().get(operatedField).getValue()));
            } catch (ClassCastException e) {
                throw new FlowmixException("Problem converting value " + item.getEvent().get(operatedField).getValue(), e);
            }
        }
        //TODO: 关联字段
        if(assocField != null){
            try {
                //如果关联的两个字段类型不同呢, 所以最好自定义的Aggregator的F为Object类型.
                add(((F) item.getEvent().get(assocField[0]).getValue()), ((F) item.getEvent().get(assocField[1]).getValue()));
            } catch (ClassCastException e) {
                throw new FlowmixException("Problem converting value " + item.getEvent().get(assocField[0]).getValue(), e);
            }
        }
    }

    @Override
    public void evicted(WindowItem item){
        if (item.getEvent().get(operatedField) != null) {
            try {
                evict((F) item.getEvent().get(operatedField).getValue());
            } catch (ClassCastException e) {
                throw new FlowmixException("Problem converting value " + item.getEvent().get(operatedField).getValue(), e);
            }
        }
    }

    @Override
    public List<AggregatedEvent> aggregate() {
        //聚合结果也要封装为Event,这是因为聚合之后,可能还有别的动作.就像其他组件一样都是发射Event给下一个组件.
        Event event = new BaseEvent(randomUUID().toString(), currentTimeMillis());
        if (groupedValues != null && groupByFields != null) {
            for (Collection<Tuple> tuples : groupedValues.values()) {
                event.putAll(tuples);
            }
        }
        T aggregateResult = aggregateResult();
        event.put(new Tuple(outputField, aggregateResult));
        return Collections.singletonList(new AggregatedEvent(event));
    }

}
