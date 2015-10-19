package com.github.miguelantonio;

/*
 * Copyright 2015 Variacode
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import com.espertech.esper.client.soda.*;
import org.junit.Test;

import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;

import static org.junit.Assert.assertEquals;

public class EsperBoltTestBase {

    //Result Store
    private static Map<Integer, Double> resultEPL = new HashMap<>();
    private static Map<Integer, Double> resultSODA = new HashMap<>();

    //Fields
    private static final String LITERAL_SYMBOL = "symbol";
    private static final String LITERAL_PRICE = "price";
    private static final String LITERAL_AVG = "avg";

    //Topology Component ID & StreamId
    private static final String LITERAL_ESPER = "esper";
    private static final String LITERAL_QUOTES ="quotes";
    private static final String LITERAL_RETURN_OBJ = "Result"; //EsperBolt的输出stream-id.

    static Map<String, Object> eventTypes = new HashMap<>();
    static {
        eventTypes.put(LITERAL_SYMBOL, String.class);
        eventTypes.put(LITERAL_PRICE, Integer.class);
    }

    public void buildTestTopology(EPStatementObjectModel model, String statement) {
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout(LITERAL_QUOTES, new RandomSentenceSpout());

        EsperBolt esperBolt = new EsperBolt();
        esperBolt.addEventTypes(eventTypes)
                //Esper的输出stream-id和输出Fields. 下一个Bolt即PrintBolt的分组会使用这里的stream-id:LITERAL_RETURN_OBJ
                //Map的value为List,因为可能输出不止一个字段, 比如价格和平均价格,两个字段.
                .addOutputTypes(Collections.singletonMap(LITERAL_RETURN_OBJ, Arrays.asList(LITERAL_AVG, LITERAL_PRICE)));

        if(model!=null){
            esperBolt.addObjectStatemens(Collections.singleton(model));
        }else{
            esperBolt.addStatements(Collections.singleton(statement));
        }

        builder.setBolt(LITERAL_ESPER, esperBolt)
                .shuffleGrouping(LITERAL_QUOTES);
        builder.setBolt("print", new PrinterBolt())
                .shuffleGrouping(LITERAL_ESPER, LITERAL_RETURN_OBJ);

        Config conf = new Config();
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("test", conf, builder.createTopology());
        Utils.sleep(10000);
        cluster.shutdown();
    }


    public static class PrinterBolt extends BaseBasicBolt {
        @Override
        public void execute(Tuple tuple, BasicOutputCollector collector) {
            resultEPL.put(tuple.getIntegerByField(LITERAL_PRICE), tuple.getDoubleByField(LITERAL_AVG));
            resultSODA.put(tuple.getIntegerByField(LITERAL_PRICE), tuple.getDoubleByField(LITERAL_AVG));
        }
        @Override
        public void declareOutputFields(OutputFieldsDeclarer ofd) {
            //Not implemented
        }
    }

    public static class RandomSentenceSpout extends BaseRichSpout {
        transient Queue<HashMap.SimpleEntry<String, Integer>> data;
        transient SpoutOutputCollector collector;
        transient int i;

        @Override
        public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
            this.collector = collector;
            data = new ConcurrentLinkedQueue<>();
            data.add(new HashMap.SimpleEntry<>("A", 50));
            data.add(new HashMap.SimpleEntry<>("A", 100));
            data.add(new HashMap.SimpleEntry<>("A", 50));
            data.add(new HashMap.SimpleEntry<>("B", 50));
            data.add(new HashMap.SimpleEntry<>("A", 30));
            data.add(new HashMap.SimpleEntry<>("C", 50));
            data.add(new HashMap.SimpleEntry<>("A", 50));
        }
        @Override
        public void nextTuple() {
            Utils.sleep(500);
            HashMap.SimpleEntry<String, Integer> d = this.data.poll();
            if (d != null) {
                this.collector.emit(new Values(d.getKey(), d.getValue()));
            }
        }
        @Override
        public void ack(Object id) {
            //Not implemented
        }
        @Override
        public void fail(Object id) {
            //Not implemented
        }
        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields(LITERAL_SYMBOL, LITERAL_PRICE));
        }

    }

}
