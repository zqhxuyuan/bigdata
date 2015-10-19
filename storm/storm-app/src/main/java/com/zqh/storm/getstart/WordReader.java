package com.zqh.storm.getstart;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.Map;

public class WordReader extends BaseRichSpout{

	private SpoutOutputCollector collector;
	private FileReader fileReader;
	private boolean completed = false;
	
	public void ack(Object msgId) {
		System.out.println("OK:"+msgId);
	}
	public void close() {
	}
	
	public void fail(Object msgId) {
		System.out.println("FAIL:"+msgId);
	}

	// The only thing that the methods will do It is emit each file line
	// 2. 通过BufferReader读取到fileReader数据之后，读取每一行数据，然后通过emit方法发送数据到订阅了数据的blot上
	public void nextTuple() {
		// The nextuple it is called forever, so if we have been readed the file we will wait and then return
		if(completed){
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				//Do nothing
			}
			return;
		}
		String str;
		// Open the reader
		BufferedReader reader = new BufferedReader(fileReader);
		try{
			// Read all lines
			while((str = reader.readLine()) != null){
				// By each line emmit a new value with the line as a their
				this.collector.emit(new Values(str),str);
			}
		}catch(Exception e){
			throw new RuntimeException("Error reading tuple",e);
		}finally{
			completed = true;
		}
	}

	// We will create the file and get the collector object
	// 1. 集群提交Topology之后，会先调用open方法，open内部使用config对象获取文件并获取fileReader对象
	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		try {
			this.fileReader = new FileReader(conf.get("wordsFile").toString());
		} catch (FileNotFoundException e) {
			throw new RuntimeException("Error reading file ["+conf.get("wordFile")+"]");
		}
		this.collector = collector;
	}

	// Declare the output field "word"
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("line"));
	}

}
