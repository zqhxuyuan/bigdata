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
package storm.starter;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.testing.FeederSpout;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import storm.meta.bolt.PrintBolt;
import storm.starter.bolt.PrintPairBolt;
import storm.starter.bolt.PrinterBolt;
import storm.starter.bolt.SingleJoinBolt;

public class SingleJoinExample {
    public static void main(String[] args) {
        FeederSpout genderSpout = new FeederSpout(new Fields("id", "gender"));
        FeederSpout ageSpout = new FeederSpout(new Fields("id", "age"));

        TopologyBuilder builder = new TopologyBuilder();
        // 两个数据源
        builder.setSpout("gender", genderSpout);
        builder.setSpout("age", ageSpout);

        // After join by id, output gender and age. we can consider id is the UserID
        builder.setBolt("join", new SingleJoinBolt(new Fields("gender", "age")))
                // the same id of gender go to the same task of [SingleJoinBolt]
                .fieldsGrouping("gender", new Fields("id"))
                // also the same id of age go to the same task of [SingleJoinBolt]
                .fieldsGrouping("age", new Fields("id"));

        builder.setBolt("print", new PrinterBolt())
                .globalGrouping("join");

        Config conf = new Config();
        conf.setDebug(true);

        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("join-example", conf, builder.createTopology());

        // genderSpout: id,gender
        for (int i = 0; i < 10; i++) {
            String gender;
            if (i % 2 == 0) {
                gender = "male";
            }
            else {
                gender = "female";
            }
            genderSpout.feed(new Values(i, gender));
        }

        // ageSpout: id, age
        for (int i = 9; i >= 0; i--) {
            ageSpout.feed(new Values(i, i + 20));
        }

        /**
         *   gender   age
         * 0,M      9,29            0,M,20
         * 1,F      8,28            1,F,21
         * 2,M      7,27            2,M,22
         * 3,F      6,26    join    3,F,23
         * 4,M      5,25   ----->   4,M,24
         * 5,F      4,24            5,F,25
         * 6,M      3,23            6,M,26
         * 7,F      2,22            7,F,27
         * 8,M      1,21            8,M,28
         * 9,F      0,20            9,F,29
         *
         * PRINT>>source: join:4, stream: default, id: {-5395771344683177987=1985239719045883793, -926068727552434393=-2275168471687950265}, [male, 24]
         * PRINT>>source: join:4, stream: default, id: {4185363282615722651=-6752215041585374580, -4385649469793014943=-9147246648387740367}, [female, 23]
         * PRINT>>source: join:4, stream: default, id: {-3177966844201726151=-6304522357589141598, -4819772428069897729=-1736588165726178479}, [female, 25]
         * PRINT>>source: join:4, stream: default, id: {5354400375340000358=-8899360007402401552, -6824188804807934260=-441281429025368186}, [male, 26]
         * PRINT>>source: join:4, stream: default, id: {4111385991468667659=5213414555402399924, 4628472303140212401=1760714085017850568}, [male, 22]
         * PRINT>>source: join:4, stream: default, id: {7495744833562338552=-5414170652420275751, -4877798712110083509=-7692794876694757617}, [female, 21]
         * PRINT>>source: join:4, stream: default, id: {-6446947134653922844=7494526465965866164, -3085586469560229208=9052309250705940081}, [female, 27]
         * PRINT>>source: join:4, stream: default, id: {-4573419021612817537=800847741739524529, 8364233053666902744=3195463322613201640}, [male, 28]
         * PRINT>>source: join:4, stream: default, id: {6123221101693120902=8867051905102974175, 8085427699511512640=1662357307835288488}, [female, 29]
         * PRINT>>source: join:4, stream: default, id: {-6039601465096638324=1964873207504649687, -8702667779580093132=5444042727105154208}, [male, 20]
         *
         * 以其中一个为例, 比如[4,M] join [4,24] = [M,24] 在join之前一定要先接收到两个要参与join的数据集.
         * INFO - Processing received message source: age:2, stream: default, id: {-926068727552434393=1329099412851742186}, [4, 24]
         * INFO - Processing received message source: gender:3, stream: default, id: {-5395771344683177987=-924244963824048640}, [4, male]
         * INFO - Processing received message source: join:4, stream: default, id: {-5395771344683177987=1985239719045883793, -926068727552434393=-2275168471687950265}, [male, 24]
         * PRINT>>source: join:4, stream: default, id: {-5395771344683177987=1985239719045883793, -926068727552434393=-2275168471687950265}, [male, 24]
         * 上面的age:2, gender:3, join:4中的数字只是topology的编号, 代表每个组件,类似字符串的"age","gender","join"
         */

        Utils.sleep(2000);
        cluster.shutdown();
    }
}
