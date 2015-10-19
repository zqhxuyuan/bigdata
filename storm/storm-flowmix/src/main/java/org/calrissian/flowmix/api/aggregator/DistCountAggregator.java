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

import java.util.*;

public class DistCountAggregator extends AbstractAggregator<Long,Object> {

    public static final String DEFAULT_OUTPUT_FIELD = "distCount";
    private Map<Object,Integer> maps = new HashMap<>();

    /**
     * INPUT(age): 10, 11, 10, 10
     * select age,count(age) from table group by age
     * add   10,1
     * add   11,2
     * add   10,2. 因为age=10已经存在,不会加入到sets中.count不会+1
     * evict 10,2. 因为evict第一个10后,第二个10还是存在的,所以count仍然不变.
     *             所以不能按照从sets删除后,count-1, 那样count=1.是不对的. 要用另外的数据结构来保存已经进来的数据集.
     * 动作,本次事件,sets的dist count值,maps的值:目前为止保存的事件以及数量.
     * add   10,1,<10,1>
     * add   11,2,<10,1><11,1>
     * add   10,2,<10,2><11,1>
     * evict 10,2,<10,1><11,1>
     * evict 10,1,<11,1>
     * evict 11,0,<>
     */
    @Override
    public void evict(Object item) {
        Integer size = maps.get(item);
        if(size > 1){
            maps.put(item,size-1);
        }else{
            maps.remove(item);
        }
    }

    @Override
    protected String getOutputField() {
        return DEFAULT_OUTPUT_FIELD;
    }

    @Override
    public void add(Object... item) {
        Integer size = maps.get(item[0]);
        if(size == null){
            maps.put(item[0],1);
        }else{
            maps.put(item[0],maps.get(item[0])+1);
        }
    }

    @Override
    protected Long aggregateResult() {
        return Long.parseLong(maps.size()+"");
    }
}
