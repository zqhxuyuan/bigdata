package com.zqh.storm.jd;

import backtype.storm.topology.IBasicBolt;
import backtype.storm.topology.IRichBolt;

import java.util.HashMap;
import java.util.Map;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;
/**
 *
 * 功能说明:
 *实现计数器的功能,第一次将 collector 中的元素存放在成员变量counters(Map)中.
 *如果 counters(Map)中已经存在该元素,getValule 并对 Value 进行累加操作。
 *
 * @author 毛祥溢
 * Email:frank@maoxiangyi.cn
 * 2013-8-26 下午 6:06:07
 */
public class WordCounter extends BaseBasicBolt {
    private static final long serialVersionUID = 5678586644899822142L;
    Integer id;
    String name;
    //定义 Map 封装最后的结果
    Map<String, Integer> counters;

    /**
     * 在 spout 结束时被调用,将最后的结果显示出来
     *
     * 結果:
     * -- Word Counter [word-counter-2] --
     * really: 1
     * but: 1
     * application: 1
     * is: 2
     * great: 2
     */
    @Override
    public void cleanup() {
        System.out.println("-- Word Counter ["+name+"-"+id+"] --");
        for(Map.Entry<String, Integer> entry : counters.entrySet()){
            System.out.println(entry.getKey()+": "+entry.getValue());
        }
        System.out.println("实现计数器的功能 --完畢!");
    }
    /**
     * 初始化操作
     */
    @Override
    public void prepare(Map stormConf, TopologyContext context) {
        this.counters = new HashMap<String, Integer>();
        this.name = context.getThisComponentId();
        this.id = context.getThisTaskId();
    }
    public void declareOutputFields(OutputFieldsDeclarer declarer) {}

    /**
     * 实现计数器的功能,第一次将 collector 中的元素存放在成员变量counters(Map)中.
     * 如果 counters(Map)中已经存在该元素,getValule 并对 Value 进行累加操作。
     */
    public void execute(Tuple input, BasicOutputCollector collector) {
        String str = input.getString(0);
        System.out.println("WordCounter 计数器收到单词 "+ str);
        if(!counters.containsKey(str)){
            counters.put(str, 1);
        }else{
            Integer c = counters.get(str) + 1;
            counters.put(str, c);
        }
    }
}