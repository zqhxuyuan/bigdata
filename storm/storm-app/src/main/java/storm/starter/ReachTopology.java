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
import backtype.storm.LocalDRPC;
import backtype.storm.StormSubmitter;
import backtype.storm.coordination.BatchOutputCollector;
import backtype.storm.coordination.CoordinatedBolt;
import backtype.storm.drpc.LinearDRPCTopologyBuilder;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.topology.base.BaseBatchBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import java.util.*;

/**
 * This is a good example of doing complex Distributed RPC on top of Storm. This program creates a topology that can
 * compute the reach for any URL on Twitter in realtime by parallelizing the whole computation.
 * <p/>
 * Reach is the number of unique people exposed to a URL on Twitter. To compute reach, you have to get all the people
 * who tweeted the URL, get all the followers of all those people, unique that set of followers, and then count the
 * unique set. It's an intense computation that can involve thousands of database calls and tens of millions of follower
 * records.
 * <p/>
 * This Storm topology does every piece of that computation in parallel, turning what would be a computation that takes
 * minutes on a single machine into one that takes just a couple seconds.
 * <p/>
 * For the purposes of demonstration, this topology replaces the use of actual DBs with in-memory hashmaps.
 * <p/>
 * See https://github.com/nathanmarz/storm/wiki/Distributed-RPC for more information on Distributed RPC.
 */
public class ReachTopology {

    //url --> tweeters
    public static Map<String, List<String>> TWEETERS_DB = new HashMap<String, List<String>>() {{
        put("foo.com/blog/1", Arrays.asList("sally", "bob", "tim", "george", "nathan"));
        put("engineering.twitter.com/blog/5", Arrays.asList("adam", "david", "sally", "nathan"));
        put("tech.backtype.com/blog/123", Arrays.asList("tim", "mike", "john"));
    }};

    //tweeter --> followers
    public static Map<String, List<String>> FOLLOWERS_DB = new HashMap<String, List<String>>() {{
        put("sally", Arrays.asList("bob", "tim", "alice", "adam", "jim", "chris", "jai"));
        put("bob", Arrays.asList("sally", "nathan", "jim", "mary", "david", "vivian"));
        put("tim", Arrays.asList("alex"));
        put("nathan", Arrays.asList("sally", "bob", "adam", "harry", "chris", "vivian", "emily", "jordan"));
        put("adam", Arrays.asList("david", "carissa"));
        put("mike", Arrays.asList("john", "bob"));
        put("john", Arrays.asList("alice", "nathan", "jim", "mike", "bob"));
    }};

    // [id, url] --> [id, tweeter]
    public static class GetTweeters extends BaseBasicBolt {
        @Override
        public void execute(Tuple tuple, BasicOutputCollector collector) {
            //id是DRPC框架自身的request-id
            Object id = tuple.getValue(0);
            //我们要使用的数据是第二个参数开始
            String url = tuple.getString(1);
            List<String> tweeters = TWEETERS_DB.get(url);
            if (tweeters != null) {
                for (String tweeter : tweeters) {
                    //还要把request-id作为第一个参数继续传递给下一个bolt
                    collector.emit(new Values(id, tweeter));
                }
            }
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("id", "tweeter"));
        }
    }

    // [id, tweeter] --> [id, follower]
    public static class GetFollowers extends BaseBasicBolt {
        @Override
        public void execute(Tuple tuple, BasicOutputCollector collector) {
            Object id = tuple.getValue(0);
            String tweeter = tuple.getString(1);
            List<String> followers = FOLLOWERS_DB.get(tweeter);
            if (followers != null) {
                for (String follower : followers) {
                    collector.emit(new Values(id, follower));
                }
            }
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("id", "follower"));
        }
    }

    // [id, follower] --> [id, count]
    public static class PartialUniquer extends BaseBatchBolt {
        BatchOutputCollector _collector;
        Object _id;
        //Set含有不允许有重复记录的语义. 即多个相同的记录最后只有一条记录
        Set<String> _followers = new HashSet<String>();

        @Override
        public void prepare(Map conf, TopologyContext context, BatchOutputCollector collector, Object id) {
            _collector = collector;
            _id = id;
        }

        //前面几个bolt的id都是从execute()的Tuple中解析出第一个参数,发射新的tuple时,又把id当做第一个参数
        //这里id在prepare()中被确定,execute()执行逻辑中没有调用一次execute()就发射一次,而是使用批处理的方式
        //在一整批数据处理完后,才发射一个tuple. 这一整批数据指的是同一个task上不同followers的数量
        //注意相同的follower的多条记录(比如不同的tweeter他们有共同的follower,这些相同的followers会被同一个task处理)只被一个task处理
        //但是一个task不只是处理这单个的follower, 它可以处理多个不同的follower
        @Override
        public void execute(Tuple tuple) {
            _followers.add(tuple.getString(1));
        }

        @Override
        public void finishBatch() {
            _collector.emit(new Values(_id, _followers.size()));
        }

        //当前task计算出来的经过去重后的followers的数量
        //因为不同的followers是被分散到不同的task上. 不会说相同的follower被多个task处理
        //因此最后只要把所有的task上的计数值相加就是reach值了
        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("id", "partial-count"));
        }
    }

    public static class CountAggregator extends BaseBatchBolt {
        BatchOutputCollector _collector;
        Object _id;
        int _count = 0;

        @Override
        public void prepare(Map conf, TopologyContext context, BatchOutputCollector collector, Object id) {
            _collector = collector;
            _id = id;
        }

        @Override
        public void execute(Tuple tuple) {
            _count += tuple.getInteger(1);
        }

        @Override
        public void finishBatch() {
            _collector.emit(new Values(_id, _count));
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("id", "reach"));
        }
    }

    public static LinearDRPCTopologyBuilder construct() {
        LinearDRPCTopologyBuilder builder = new LinearDRPCTopologyBuilder("reach");
        builder.addBolt(new GetTweeters(), 4);
        builder.addBolt(new GetFollowers(), 12).shuffleGrouping();
        builder.addBolt(new PartialUniquer(), 6).fieldsGrouping(new Fields("id", "follower"));
        builder.addBolt(new CountAggregator(), 3).fieldsGrouping(new Fields("id"));
        return builder;
    }

    public static void main(String[] args) throws Exception {
        LinearDRPCTopologyBuilder builder = construct();


        Config conf = new Config();

        if (args == null || args.length == 0) {
            conf.setMaxTaskParallelism(3);
            LocalDRPC drpc = new LocalDRPC();
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("reach-drpc", conf, builder.createLocalTopology(drpc));

            String[] urlsToTry = new String[]{"foo.com/blog/1", "engineering.twitter.com/blog/5", "notaurl.com"};
            for (String url : urlsToTry) {
                System.out.println("Reach of " + url + ": " + drpc.execute("reach", url));
            }

            cluster.shutdown();
            drpc.shutdown();
        } else {
            conf.setNumWorkers(6);
            StormSubmitter.submitTopologyWithProgressBar(args[0], conf, builder.createRemoteTopology());
        }
    }

    // old api
    public static class PartialDistinct implements IRichBolt, CoordinatedBolt.FinishedCallback {
        OutputCollector _collector;
        Map<Object, Set<String>> _sets = new HashMap<Object, Set<String>>();

        public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
            _collector = collector;
        }

        public void execute(Tuple tuple) {
            Object id = tuple.getValue(0);
            Set<String> curr = _sets.get(id);
            if (curr == null) {
                curr = new HashSet<String>();
                _sets.put(id, curr);
            }
            curr.add(tuple.getString(1));
            _collector.ack(tuple);
        }

        public void cleanup() {
        }

        public void finishedId(Object id) {
            Set<String> curr = _sets.remove(id);
            int count;
            if (curr != null) {
                count = curr.size();
            } else {
                count = 0;
            }
            _collector.emit(new Values(id, count));
        }

        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("id", "partial-count"));
        }

        @Override
        public Map<String, Object> getComponentConfiguration() {
            return null;
        }
    }
}
