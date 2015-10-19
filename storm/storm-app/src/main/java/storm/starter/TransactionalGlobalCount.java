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
import backtype.storm.coordination.BatchOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.testing.MemoryTransactionalSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBatchBolt;
import backtype.storm.topology.base.BaseTransactionalBolt;
import backtype.storm.transactional.ICommitter;
import backtype.storm.transactional.TransactionAttempt;
import backtype.storm.transactional.TransactionalTopologyBuilder;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import storm.starter.bolt.PrinterBolt;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * This is a basic example of a transactional topology. It keeps a count of the number of tuples seen so far in a
 * database. The source of data and the databases are mocked out as in memory maps for demonstration purposes. This
 * class is defined in depth on the wiki at https://github.com/nathanmarz/storm/wiki/Transactional-topologies
 */
public class TransactionalGlobalCount {
    public static final int PARTITION_TAKE_PER_BATCH = 3;
    public static final Map<Integer, List<List<Object>>> DATA = new HashMap<Integer, List<List<Object>>>() {{
        put(0, new ArrayList<List<Object>>() {{
            add(new Values("cat"));
            add(new Values("dog"));
            add(new Values("chicken"));
            add(new Values("cat"));
            add(new Values("dog"));
            add(new Values("apple"));
        }});
        put(1, new ArrayList<List<Object>>() {{
            add(new Values("cat"));
            add(new Values("dog"));
            add(new Values("apple"));
            add(new Values("banana"));
        }});
        put(2, new ArrayList<List<Object>>() {{
            add(new Values("cat"));
            add(new Values("cat"));
            add(new Values("cat"));
            add(new Values("cat"));
            add(new Values("cat"));
            add(new Values("dog"));
            add(new Values("dog"));
            add(new Values("dog"));
            add(new Values("dog"));
        }});
    }};

    public static class Value {
        int count = 0;
        BigInteger txid;
    }

    public static Map<String, Value> DATABASE = new HashMap<String, Value>();
    public static final String GLOBAL_COUNT_KEY = "GLOBAL-COUNT";

    // This Bolt just work for one batch. we should aggregate all batch together
    public static class BatchCount extends BaseBatchBolt {
        Object _id;
        BatchOutputCollector _collector;

        int _count = 0;

        @Override
        public void prepare(Map conf, TopologyContext context, BatchOutputCollector collector, Object id) {
            _collector = collector;
            _id = id;
        }

        //global count. no matter what word.
        @Override
        public void execute(Tuple tuple) {
            _count++;
        }

        @Override
        public void finishBatch() {
            _collector.emit(new Values(_id, _count));
        }

        // like drpc's request-id should keep in each component. transaction also keep the id which represent tx-id
        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("id", "count"));
        }
    }

    // Transaction Bolt works for all Batch Bolt
    public static class UpdateGlobalCount extends BaseTransactionalBolt implements ICommitter {
        TransactionAttempt _attempt;    // like map-reduce's TaskAttempt. which represent this time transaction
        BatchOutputCollector _collector;

        int _sum = 0;

        @Override
        public void prepare(Map conf, TopologyContext context, BatchOutputCollector collector, TransactionAttempt attempt) {
            _collector = collector;
            _attempt = attempt;
        }

        //This Bolt reveive Tuple from BatchBolt: BatchCount which the output fields=[id,count]. so we get the 2nd one:count
        @Override
        public void execute(Tuple tuple) {
            _sum += tuple.getInteger(1);
        }

        @Override
        public void finishBatch() {
            Value val = DATABASE.get(GLOBAL_COUNT_KEY);
            Value newval;
            //数据库中没有这个key, 或者说数据库中已经保存的事务ID和当前事务ID不一致
            if (val == null || !val.txid.equals(_attempt.getTransactionId())) {
                //新建一个,并且它的事务ID就等于当前的事务ID, 代表的是最新的事务!
                newval = new Value();
                newval.txid = _attempt.getTransactionId();

                //数据库中没有这个Key, 说明是第一次, 直接把global-count赋值进来
                if (val == null) {
                    newval.count = _sum;
                }
                //数据库中有这个Key, 而且已经保存的事务ID和当前最新的事务ID不一样, 则在旧的count(val.count)上加上本次的count
                else {
                    newval.count = _sum + val.count;
                }
                DATABASE.put(GLOBAL_COUNT_KEY, newval);
            }
            //数据库中已经存在这个key了, 并且事务ID是一样的. 因为事务ID一样,说明是重复的, 不需要更新!
            else {
                newval = val;
            }
            _collector.emit(new Values(_attempt, newval.count));
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("id", "sum"));
        }
    }

    public static void main(String[] args) throws Exception {
        MemoryTransactionalSpout spout = new MemoryTransactionalSpout(DATA, new Fields("word"), PARTITION_TAKE_PER_BATCH);
        TransactionalTopologyBuilder builder = new TransactionalTopologyBuilder("global-count", "spout", spout, 3);

        builder.setBolt("partial-count", new BatchCount(), 5).noneGrouping("spout");

        builder.setBolt("sum", new UpdateGlobalCount()).globalGrouping("partial-count");

        builder.setBolt("printx", new PrinterBolt()).globalGrouping("sum");

        LocalCluster cluster = new LocalCluster();

        Config config = new Config();
        config.setDebug(true);
        config.setMaxSpoutPending(3);

        cluster.submitTopology("global-count-topology", config, builder.buildTopology());

        //Thread.sleep(10000);
        //cluster.shutdown();
    }
}
