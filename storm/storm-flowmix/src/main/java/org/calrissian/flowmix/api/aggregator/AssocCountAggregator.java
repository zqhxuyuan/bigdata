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
package org.calrissian.flowmix.api.aggregator;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * 关联计数.
 * 比如IP在账户上的关联个数,表示IP地址关联了几个账户.
 *
 * Distinct Count:
 * {ip:1.1.1.2} => 1
 * {ip:1.1.1.3} => 2
 * {ip:1.1.1.2} => 2
 *
 * Association Count:
 * {ip:1.1.1.2, account:AA} => 1
 * {ip:1.1.1.2, account:BB} => 2
 * {ip:1.1.1.2, account:AA} => 2
 *
 * 和DistinctCount一样,用另外的数据结构来保存, 实现思路:
 * {ip:1.1.1.2, account:AA} => {ip:1.1.1.2, Map(AA,1)}          => map.size=1
 * {ip:1.1.1.2, account:BB} => {ip:1.1.1.2, Map(AA->1, BB->1)}  => map.size=2
 * {ip:1.1.1.2, account:AA} => {ip:1.1.1.2, Map(AA->2, BB->1)}  => map.size=2
 * evict {ip:1.1.1.2, account:AA} => {ip:1.1.1.2, Map(AA->1, BB->1)}  => map.size=2
 */
public class AssocCountAggregator extends AbstractAggregator<Map,Object> {

    public static final String DEFAULT_OUTPUT_FIELD = "assocCount";

    private Map<Object,Map<Object,Integer>> maps = new HashMap<>();

    @Override
    public void evict(Object item) {
        //TODO 和add一样,也要有两个参数!
        Object right = null;
    }

    @Override
    protected String getOutputField() {
        return DEFAULT_OUTPUT_FIELD;
    }

    //假设两条事件进来, left相同,right不同: {val1, val3}, {val1, val_3}
    //第一条事件(val1, val3) 进来时: val1 -> (val3->1)
    //第二条事件(val1, val_3)进来时: val1 -> (val3->1, val_3->1)
    //第三条事件(val1, val3) 进来时: val1 -> (val3->2, val_3->1)
    @Override
    public void add(Object... item) {
        Object left = item[0];
        Object right = item[1];
        //System.out.println(left+"--"+right);
        Map<Object,Integer> rightMap = maps.get(left);

        if(rightMap != null){
            Integer size = rightMap.get(right);
            if(size == null){
                rightMap.put(right, 1);
            }else{
                rightMap.put(right, size+1);
            }
        }else{
            rightMap = new HashMap();
            rightMap.put(right, 1);
        }
        maps.put(left, rightMap);
    }

    //实际上要计算的是每个IP关联的账户数, 而不是计算IP数.如果是IP数,可以用DistCount计算.所以需要传入IP作为参数!
    //或者返回的是<field,size>: 即每个IP关联的账户数, 返回Map结构.
    //protected Long aggregateResult() {return Long.parseLong(maps.size()+"");}
    //protected Long aggregateResult(Object field){return Long.parseLong(maps.get(field).size()+"");}

    protected Map aggregateResult(){
        Map<Object,Integer> map = new HashMap<>();
        Set keys = maps.keySet();
        for(Object key : keys){
            map.put(key, maps.get(key).size());
        }
        return maps;
    }
}
