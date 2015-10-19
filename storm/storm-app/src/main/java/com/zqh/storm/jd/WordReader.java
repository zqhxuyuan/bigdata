package com.zqh.storm.jd;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.Map;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import  backtype.storm.tuple.Values;
/**
 *
 * 功能说明:
 *
 主要是将文件内容读出来,一行一行
 *
 *
 Spout 类里面最重要的方法是 nextTuple。
 *
 要么发射一个新的 tuple 到 topology 里面或者简单的返回如果已经没
 有新的 tuple。
 *
 要注意的是 nextTuple 方法不能阻塞,因为 storm 在同一个线程上面调
 用所有消息源 spout 的方法。
 *
 另外两个比较重要的 spout 方法是 ack 和 fail。
 *
 storm 在检测到一个 tuple 被整个 topology 成功处理的时候调用 ack,
 否则调用 fail。
 * storm 只对可靠的 spout 调用 ack 和 fail。
 *
 * @author 毛祥溢
 * Email:frank@maoxiangyi.cn
 * 2013-8-26 下午 6:05:46
 */
public class WordReader extends BaseRichSpout {
    private SpoutOutputCollector collector;
    private FileReader fileReader;
    private String filePath;
    private boolean completed = false;
    //storm 在检测到一个 tuple 被整个 topology 成功处理的时候调用 ack,否则调用 fail。
    public void ack(Object msgId) {
        System.out.println("OK:"+msgId);
    }
    public void close() {}
    //storm 在检测到一个 tuple 被整个 topology 成功处理的时候调用 ack,否则调用 fail。
    public void fail(Object msgId) {
        System.out.println("FAIL:"+msgId);
    }
    /*
    * 在 SpoutTracker 类中被调用,每调用一次就可以向 storm 集群中发射一条数据(一个 tuple 元组),该方法会被不停的调用
    */
    public void nextTuple() {
        if(completed){
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
            }
            return;
        }
        String str;
        BufferedReader reader =new BufferedReader(fileReader);
        try{
            while((str = reader.readLine()) != null){
                System.out.println("WordReader 类 读取到一行数据: "+ str);
                this.collector.emit(new Values(str),str);
                System.out.println("WordReader 类 发射了一条数据: "+ str);
            }
        }catch(Exception e){
            throw new RuntimeException("Error reading tuple",e);
        }finally{
            completed = true;
        }
    }
    public void open(Map conf, TopologyContext
            context,SpoutOutputCollector collector) {
        try {
            this.fileReader = new
                    FileReader(conf.get("wordsFile").toString());
        } catch (FileNotFoundException e) {
            throw new RuntimeException("Error reading file ["+conf.get("wordFile")+"]");
        }
        this.filePath = conf.get("wordsFile").toString();
        this.collector = collector;
    }
    /**
     * 定义字段 id,该 id 在简单模式下没有用处,但在按照字段分组的模式下有很大的用处。
     * 该 declarer 变量有很大作用,我们还可以调用declarer.declareStream();来定义 stramId,该 id 可以用来定义更加复杂的流拓扑结构
     */
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("line"));
    }
}