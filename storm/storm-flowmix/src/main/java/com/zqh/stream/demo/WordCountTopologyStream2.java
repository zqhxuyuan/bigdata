package com.zqh.stream.demo;


import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;

/**
 * This topology demonstrates Storm's stream groupings and multilang capabilities.
 */
public class WordCountTopologyStream2 {

    public static class RandomSentenceSpout extends BaseRichSpout {
        SpoutOutputCollector collector;
        Random rand;
        String[] sentences = null;

        @Override
        public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
            this.collector = collector;
            rand = new Random();
            sentences = new String[]{ "the cow jumped over the moon", "an apple a day keeps the doctor away", "four score and seven years ago", "snow white and the seven dwarfs", "i am at two with nature" };
        }

        @Override
        public void nextTuple() {
            Utils.sleep(1000);
            String sentence = sentences[rand.nextInt(sentences.length)];
            System.out.println("\n" + sentence);
            this.collector.emit("split-stream", new Values(sentence));
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            /*
            declarer.declareStream("split-stream", new Fields("sentence"));
            declarer.declareStream("count-stream", new Fields("sentence"));
            declarer.declareStream("print-stream", new Fields("sentence"));
            */
            declareStream(declarer, new Fields("sentence"));
        }
        public void ack(Object id) {}
        public void fail(Object id) {}
    }

    public static class SplitSentenceBolt extends BaseRichBolt {
        private OutputCollector collector;

        @Override
        public void prepare(Map config, TopologyContext context, OutputCollector collector) {
            this.collector = collector;
        }

        @Override
        public void execute(Tuple tuple) {
            String sentence = tuple.getStringByField("sentence");
            String[] words = sentence.split(" ");
            for (String word : words) {
                this.collector.emit("count-stream", new Values(word));
            }
            this.collector.ack(tuple);
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            /*
            declarer.declareStream("split-stream", new Fields("word"));
            declarer.declareStream("count-stream", new Fields("word"));
            declarer.declareStream("print-stream", new Fields("word"));
            */
            declareStream(declarer, new Fields("word"));
        }
    }

    public static class WordCountBolt extends BaseRichBolt {
        Map<String, Integer> counts = new HashMap<String, Integer>();
        private OutputCollector collector;
        @Override
        public void prepare(Map config, TopologyContext context, OutputCollector collector) {
            this.collector = collector;
        }

        @Override
        public void execute(Tuple tuple) {
            String word = tuple.getString(0);
            Integer count = counts.get(word);
            if (count == null) count = 0;
            count++;
            counts.put(word, count);
            collector.emit("print-stream", new Values(word, count));
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            /*
            declarer.declareStream("split-stream", new Fields("word", "count"));
            declarer.declareStream("count-stream", new Fields("word", "count"));
            declarer.declareStream("print-stream", new Fields("word", "count"));
            */
            declareStream(declarer, new Fields("word", "count"));
        }
    }

    public static class PrinterBolt extends BaseRichBolt {
        private OutputCollector collector;
        @Override
        public void prepare(Map config, TopologyContext context, OutputCollector collector) {
            this.collector = collector;
        }
        @Override
        public void execute(Tuple tuple) {
            String first = tuple.getString(0);
            int second = tuple.getInteger(1);
            System.out.println(first + "," + second);
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
        }
    }

    public static void main(String[] args) throws Exception {
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("spout", new RandomSentenceSpout(), 1);

        //最简单的stream-id √
        /*
        builder.setBolt("split", new SplitSentenceBolt(), 2).shuffleGrouping("spout", "split-stream");
        builder.setBolt("count", new WordCountBolt(), 2).fieldsGrouping("split", "count-stream", new Fields("word"));
        builder.setBolt("print", new PrinterBolt(), 1).shuffleGrouping("count", "print-stream");
        */

        //错误的使用方式 ×
        /*
        builder.setBolt("split", new SplitSentenceBolt(), 2)
                .shuffleGrouping("spout", "split-stream")                      //⬅
                .shuffleGrouping("split", "split-stream")
                .shuffleGrouping("count", "split-stream")
        ;
        builder.setBolt("count", new WordCountBolt(), 2)
                .fieldsGrouping("spout", "count-stream", new Fields("word"))
                .fieldsGrouping("split", "count-stream", new Fields("word"))   //⬅
                .fieldsGrouping("count", "count-stream", new Fields("word"))
        ;
        builder.setBolt("print", new PrinterBolt(), 1)
                .shuffleGrouping("count", "print-stream");
        */

        //多个stream-id √
        /*
        builder.setBolt("split", new SplitSentenceBolt(), 2)
                .shuffleGrouping("spout", "split-stream")                      //⬅
                .fieldsGrouping("split", "split-stream", new Fields("word"))
                .shuffleGrouping("count", "split-stream")
        ;
        builder.setBolt("count", new WordCountBolt(), 2)
                .shuffleGrouping("spout", "count-stream")
                .fieldsGrouping("split", "count-stream", new Fields("word"))   //⬅
                .shuffleGrouping("count", "count-stream")
        ;
        builder.setBolt("print", new PrinterBolt(), 1)
                .shuffleGrouping("count", "print-stream");
        */

        //抽取多个stream-id √
        /*
        setBolt(builder, new SplitSentenceBolt(), "split");
        setBolt(builder, new WordCountBolt(), "count");
        builder.setBolt("print", new PrinterBolt(), 1).shuffleGrouping("count", "print-stream");
        */

        //InvalidTopologyException:
        //不注释:    [count] subscribes from non-existent stream: [count-stream] of component [print]
        //注释①:    [split] subscribes from non-existent stream: [split-stream] of component [print]
        //注释①②:  [print] subscribes from non-existent stream: [print-stream] of component [print]
        //注释①②③: SUCCESS!
        /*
        builder.setBolt("split", new SplitSentenceBolt(), 2)
                .shuffleGrouping("spout", "split-stream")                      //⬅
                .fieldsGrouping("split", "split-stream", new Fields("word"))
                .shuffleGrouping("count", "split-stream")
                //.shuffleGrouping("print", "split-stream")  //②
        ;
        builder.setBolt("count", new WordCountBolt(), 2)
                .shuffleGrouping("spout", "count-stream")
                .fieldsGrouping("split", "count-stream", new Fields("word"))   //⬅
                .shuffleGrouping("count", "count-stream")
                //.shuffleGrouping("print", "count-stream")  //①
        ;
        builder.setBolt("print", new PrinterBolt(), 1)
                .shuffleGrouping("spout", "print-stream")
                .fieldsGrouping("split", "print-stream", new Fields("word"))
                .shuffleGrouping("count", "print-stream")                      //⬅
                //.shuffleGrouping("print", "print-stream")  //③
        ;
        */

        setBolt(builder, new SplitSentenceBolt(), "split");
        setBolt(builder, new WordCountBolt(), "count");
        setBolt(builder, new PrinterBolt(), "print");

        Config conf = new Config();
        conf.setDebug(false);
        if (args != null && args.length > 0) {
            conf.setNumWorkers(3);
            StormSubmitter.submitTopologyWithProgressBar(args[0], conf, builder.createTopology());
        } else {
            conf.setMaxTaskParallelism(3);
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("word-count", conf, builder.createTopology());
            Thread.sleep(10000);
            cluster.shutdown();
        }
    }

    public static void declareStream(OutputFieldsDeclarer declarer, Fields fields){
        declarer.declareStream("split-stream", fields);
        declarer.declareStream("count-stream", fields);
        declarer.declareStream("print-stream", fields);
    }

    public static void setBolt(TopologyBuilder builder, IRichBolt bolt, String name){
        builder.setBolt(name, bolt, 2)
                .shuffleGrouping("spout", name + "-stream")
                .fieldsGrouping("split", name + "-stream", new Fields("word"))
                .shuffleGrouping("count", name + "-stream")
                //.shuffleGrouping("print", name + "-stream")
        ;
    }

}