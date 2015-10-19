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
import java.util.Map;

@Deprecated
public class GroupCountAggregator extends AbstractAggregator<Map,Object> {

    public static final String DEFAULT_OUTPUT_FIELD = "groupCount";
    private Map<Object,Integer> maps = new HashMap<>();

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
        //System.out.println(maps);
    }

    @Override
    protected Map aggregateResult() {
        return maps;
    }
}
