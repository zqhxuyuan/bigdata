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
import backtype.storm.topology.BoltDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import storm.kafka.*;
import storm.starter.bolt.RollingCountBolt;
import storm.starter.bolt.TimedCountBolt;
import storm.starter.bolt.TimedDistinctCountBolt;
import storm.starter.model.Compute;
import storm.starter.test.JSONTestBolt;
import storm.starter.test.PrintBolt;
import storm.starter.util.StormRunner;
import storm.starter.util.WindowConstant;

public class TimedTopology {
    private static final int DEFAULT_RUNTIME_IN_SECONDS = 300;

    private final TopologyBuilder builder;
    private final String topologyName;
    private final Config topologyConfig;
    private final int runtimeInSeconds;

    public TimedTopology(String topologyName) throws InterruptedException {
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
        String spoutId = WindowConstant.spoutId;
        String printId = "print";
        String jsonBoltId = "json-metric";
        boolean debug = true;

        builder.setSpout(spoutId, new KafkaSpout(WindowConstant.spoutConf), 3);
        builder.setBolt(jsonBoltId, new JSONTestBolt(debug)).shuffleGrouping(spoutId);

        //分组方式不对,会导致相同的word到不同的task中.
        //builder.setBolt("timedCountBolt", new TimedCountBolt(), WindowConstant.paralize).localOrShuffleGrouping(jsonBoltId, Compute.COUNT.name());

        //没有使用stream-id, 只能针对一种计算类型策略
        //builder.setBolt("timedCountBolt", new TimedCountBolt(), WindowConstant.paralize).fieldsGrouping(jsonBoltId, new Fields(WindowConstant.masterKey));

        //[1]在TimedCountBolt的构造函数中创建所有时间窗口的counterMap
        builder.setBolt("timedCountBolt", new TimedCountBolt(debug), WindowConstant.paralize)
                .fieldsGrouping(jsonBoltId, Compute.COUNT.name(), new Fields(WindowConstant.masterKey));
        //builder.setBolt(printId, new PrintBolt()).globalGrouping("timedCountBolt");

        builder.setBolt("timedDistinctCountBolt", new TimedDistinctCountBolt(), WindowConstant.paralize)
                .fieldsGrouping(jsonBoltId, Compute.DISTCOUNT.name(), new Fields(WindowConstant.masterKey));
        //builder.setBolt(printId, new PrintBolt()).localOrShuffleGrouping("timedDistinctCountBolt");

        //[2]所有不同时间窗口的多个Bolt最终只发送给一个PrintBolt处理.
        BoltDeclarer printBolt = builder.setBolt("prints", new PrintBolt());
        for(int ts : WindowConstant.timeUnits){
            builder.setBolt("timedCountBolt_" + ts, new TimedCountBolt(ts))
                    .fieldsGrouping(jsonBoltId, Compute.COUNT.name(), new Fields(WindowConstant.masterKey));
            printBolt.allGrouping("timedCountBolt_" + ts);
        }

        //-> 测试可以正常获取Kafka的数据,并输出原始JSON字符串
        //builder.setBolt(printId, new PrintBolt()).shuffleGrouping(spoutId);

        //-> 验证JSONBolt根据Metric的计算类型,发射到不同的stream-id中. 这里指定了COUNT这个stream-id,说明MetricBolt只会接收COUNT类型的事件.
        //   实际中, 我们需要把Count类型的给CountBolt处理, 把DistCount类型的给DistCountBolt处理.
        //builder.setBolt(printId, new PrintBolt()).localOrShuffleGrouping(jsonBoltId, Compute.COUNT.name());
    }

    public void run() throws InterruptedException {
        StormRunner.runTopologyLocally(builder.createTopology(), topologyName, topologyConfig, runtimeInSeconds);
    }

    public static void main(String[] args) throws Exception {
        new TimedTopology("RollingDistinctCount").run();
    }
}
