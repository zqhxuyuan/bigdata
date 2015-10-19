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

public class MaxAggregator extends AbstractAggregator<Long, Long> {

    public static final String DEFAULT_OUTPUT_FIELD = "max";

    protected long max;

    @Override
    public void evict(Long value) {
        Math.max(max,value);
    }

    @Override
    protected String getOutputField() {
        return DEFAULT_OUTPUT_FIELD;
    }

    @Override
    public void add(Long... value) {
        Math.max(max,value[0]);
    }

    @Override
    protected Long aggregateResult() {
        return max;
    }

}
