/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package storm.starter.topology;

import backtype.storm.Config;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import org.apache.log4j.Logger;
import storm.starter.bolt.RollingDistinctCountBolt;
import storm.starter.test.PrintTupleBolt;
import storm.starter.test.TestTupleSpout;
import storm.starter.util.StormRunner;

/**
 * 主维度关联的从维度的去重个数
 */
public class RollingDistinctCount {

    private static final Logger LOG = Logger.getLogger(RollingDistinctCount.class);
    private static final int DEFAULT_RUNTIME_IN_SECONDS = 60;
    private static final int TOP_N = 5;

    private final TopologyBuilder builder;
    private final String topologyName;
    private final Config topologyConfig;
    private final int runtimeInSeconds;

    public RollingDistinctCount(String topologyName) throws InterruptedException {
        builder = new TopologyBuilder();
        this.topologyName = topologyName;
        topologyConfig = createTopologyConfiguration();
        runtimeInSeconds = DEFAULT_RUNTIME_IN_SECONDS;

        wireTopology();
    }

    private static Config createTopologyConfiguration() {
        Config conf = new Config();
        //conf.setDebug(true);
        return conf;
    }

    private void wireTopology() throws InterruptedException {
        String spoutId = "tupleGenerator";
        String printId = "print";

        String leftField = "accountLogin";
        String rightField = "ipAddress";
        boolean detail = false;

        //spout:输出主维度和从维度
        builder.setSpout(spoutId, new TestTupleSpout(leftField, rightField), 5);

        //count:根据主维度进行分组,即相同的主维度会被同一个bolt/task处理.
        //OK, 即使Bolt分成多个Task, 最终相同的word只会输出一次
        builder.setBolt("counter1", new RollingDistinctCountBolt(60, 20, leftField, rightField), 4)
                .fieldsGrouping(spoutId, new Fields(leftField));
        //WRONG: Bolt分成4个Task, 最终相同的word会输出4次!
        builder.setBolt("counter2", new RollingDistinctCountBolt(60, 20, leftField, rightField), 4)
                .fieldsGrouping(spoutId, new Fields(leftField, rightField));
        //WRONG: 同上!
        builder.setBolt("counter3", new RollingDistinctCountBolt(60, 20, leftField, rightField), 4)
                .shuffleGrouping(spoutId);

        //不需要排序求topN, 直接输出. 最后的打印输出使用global, shuffleGrouping都没有影响.
        //builder.setBolt(printId, new PrintTupleBolt(detail)).globalGrouping("counter1");
        builder.setBolt(printId, new PrintTupleBolt(detail)).localOrShuffleGrouping("counter1");
    }

    public void run() throws InterruptedException {
        StormRunner.runTopologyLocally(builder.createTopology(), topologyName,
                topologyConfig, runtimeInSeconds);
    }

    public static void main(String[] args) throws Exception {
        new RollingDistinctCount("RollingDistinctCount").run();
    }
}
