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
package storm.starter.bolt;

import backtype.storm.tuple.Tuple;
import org.apache.log4j.Logger;
import storm.starter.tools.Rankable;
import storm.starter.tools.RankableObjectWithFields;

/**
 * This bolt ranks incoming objects by their count.
 * <p/>
 * It assumes the input tuples to adhere to the following format:
 * (object, object_count, additionalField1, additionalField2, ..., additionalFieldN).
 *
 * 这个bolt作用就是对于中间结果的排序, 为什么要增加这步, 应为数据量比较大, 如果直接全放到一个节点上排序, 会负载太重
 * 所以先通过IntermediateRankingsBolt, 过滤掉一些
 * 这里仍然对obj进行fieldsGrouping, 保证对于同一个obj, 不同时间段emit的统计数据会被发送到同一个task
 */
public final class IntermediateRankingsBolt extends AbstractRankerBolt {

  private static final long serialVersionUID = -1369800530256637409L;
  private static final Logger LOG = Logger.getLogger(IntermediateRankingsBolt.class);

  public IntermediateRankingsBolt() {
    super();
  }

  public IntermediateRankingsBolt(int topN) {
    super(topN);
  }

  public IntermediateRankingsBolt(int topN, int emitFrequencyInSeconds) {
    super(topN, emitFrequencyInSeconds);
  }

  /**
   * 将Tuple转化Rankable, 并更新Rankings列表, 参考AbstractRankerBolt, 该bolt会定时将Ranking列表emit出去
   * 因为上一个Bolt是RollingCountBolt,它发射的tuple为:obj,count,actualWindowSize.就是这里接收到的tuple.
   *
   * 由于不同task发射的obj是不同的, 我们最终要求topN.即求count值最大的N个obj.
   * 所以要将obj,count转成可以排序的对象Rankale.
   */
  @Override
  void updateRankingsWithTuple(Tuple tuple) {
    Rankable rankable = RankableObjectWithFields.from(tuple);
    super.getRankings().updateWith(rankable);
  }

  @Override
  Logger getLogger() {
    return LOG;
  }
}
