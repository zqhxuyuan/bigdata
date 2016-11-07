package com.zqh.stream.demo;


import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * This topology demonstrates Storm's stream groupings and multilang capabilities.
 */
public class WordCountTopology1 {

    public static class RandomSentenceSpout implements IRichSpout {
        SpoutOutputCollector collector;
        Random rand;
        String[] sentences = null;
        AtomicInteger counter = null;

        @Override
        public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
            this.collector = collector;
            rand = new Random();
            counter = new AtomicInteger(1);
            sentences = new String[]{ "the cow jumped over the moon", "an apple a day keeps the doctor away", "four score and seven years ago", "snow white and the seven dwarfs", "i am at two with nature" };
        }

        @Override
        public void close() {}

        @Override
        public void activate() {}

        @Override
        public void deactivate() {}

        @Override
        public void nextTuple() {
            Utils.sleep(5000);
            int msgId = counter.getAndIncrement();
            String sentence = sentences[rand.nextInt(sentences.length)];
            System.out.println(">>>>>>>>>>emit sentence["+msgId+"]:" + sentence);
            this.collector.emit(new Values(sentence), msgId);
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("sentence"));
        }

        @Override
        public Map<String, Object> getComponentConfiguration() {
            return null;
        }

        public void ack(Object msgId) {
            System.out.println(">>>>>>>>>>ack:"+msgId);
        }
        public void fail(Object msgId) {
            System.out.println(">>>>>>>>>>fail:"+msgId);
        }
    }

    //tuple ack
    public static class SplitSentenceBolt extends BaseRichBolt {
        private OutputCollector collector;

        @Override
        public void prepare(Map config, TopologyContext context,
                            OutputCollector collector) {
            this.collector = collector;
        }

        @Override
        public void execute(Tuple tuple) {
            String sentence = tuple.getStringByField("sentence");
            String[] words = sentence.split(" ");
            for (String word : words) {
                this.collector.emit(tuple, new Values(word));
            }
            this.collector.ack(tuple);
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("word"));
        }
    }

    public static class WordCountBolt extends BaseBasicBolt {
        Map<String, Integer> counts = new HashMap<String, Integer>();

        @Override
        public void execute(Tuple tuple, BasicOutputCollector collector) {
            String word = tuple.getString(0);
            Integer count = counts.get(word);
            if (count == null) count = 0;
            count++;
            counts.put(word, count);
            collector.emit(new Values(word, count));
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("word", "count"));
        }
    }

    //Tuple ack
    public static class WordCountBolt2 extends BaseRichBolt {
        OutputCollector collector;
        Map<String, Integer> counts = new HashMap<String, Integer>();

        @Override
        public void prepare(Map stormConf, TopologyContext context,
                            OutputCollector collector) {
            this.collector = collector;
        }

        @Override
        public void execute(Tuple tuple) {
            String word = tuple.getString(0);
            Integer count = counts.get(word);
            if (count == null) count = 0;
            count++;
            counts.put(word, count);
            collector.emit(tuple, new Values(word, count));
            collector.ack(tuple);
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("word", "count"));
        }
    }

    public static class PrinterBolt extends BaseBasicBolt {
        @Override
        public void execute(Tuple tuple, BasicOutputCollector collector) {
            String first = tuple.getString(0);
            int second = tuple.getInteger(1);
            System.out.println(first + "," + second);
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer ofd) {}
    }

    public static void main(String[] args) throws Exception {

        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("spout", new RandomSentenceSpout(), 1);
        builder.setBolt("split", new SplitSentenceBolt(), 2).shuffleGrouping("spout");
        builder.setBolt("count", new WordCountBolt2(), 2).fieldsGrouping("split", new Fields("word"));
        builder.setBolt("print", new PrinterBolt(), 1).shuffleGrouping("count");

        Config conf = new Config();
        conf.setDebug(false);
        conf.setNumAckers(1);

        if (args != null && args.length > 0) {
            conf.setNumWorkers(3);
            StormSubmitter.submitTopologyWithProgressBar(args[0], conf, builder.createTopology());
        } else {
            conf.setMaxTaskParallelism(3);
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("word-count", conf, builder.createTopology());
            Thread.sleep(32000);
            cluster.shutdown();
        }
    }
}