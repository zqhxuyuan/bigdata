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

import java.util.concurrent.LinkedBlockingDeque;

//带大小容量的双端队列
public class LimitingDeque<E> extends LinkedBlockingDeque<E> {

    private long maxSize;

    protected long getMaxSize() {
      return maxSize;
    }

    public LimitingDeque(long maxSize) {
        this.maxSize = maxSize;
    }

    //向队列头部添加元素
    @Override
    public boolean offerFirst(E e) {
        //如果队列满了,删除最后一个
        if(size() == maxSize)
            removeLast();

        return super.offerFirst(e);
    }

    //向队列尾部添加元素
    @Override
    public boolean offerLast(E e) {
        //如果队列满了,删除第一个.
        if(size() == maxSize)
            removeFirst();

        return super.offerLast(e);
    }
}
