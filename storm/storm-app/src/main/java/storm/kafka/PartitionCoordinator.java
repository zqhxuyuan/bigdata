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
package storm.kafka;

import java.util.List;

/**
 * 分区协调器
 */
public interface PartitionCoordinator {

    //属于我管理的分区列表. 我指的是消费者,客户端
    List<PartitionManager> getMyManagedPartitions();

    //因为一个消费者可以消费多个分区,指定一个分区,返回对应的分区管理器
    PartitionManager getManager(Partition partition);

    //刷新
    void refresh();
}
