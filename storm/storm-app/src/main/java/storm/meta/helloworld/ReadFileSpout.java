package storm.meta.helloworld;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.Map;

import storm.meta.base.MacroDef;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

/** 
 * @author blogchong
 * @Blog   www.blogchong.com
 * @email  blogchong@gmail.com
 * @QQ_G   191321336
 * @version 2014年11月9日 上午11:26:29
 */

@SuppressWarnings("serial")
public class ReadFileSpout implements IRichSpout {
	
	/**
	 * 我们直接把spout下的spout拷贝过来了，作为storm的helloworld的spout
	 * 我们只统计domain文件中的两个值，第一列和最后一列即除了数字部分
	 */

	private SpoutOutputCollector collector;
	FileInputStream fis;
	InputStreamReader isr;
	BufferedReader br;

	@SuppressWarnings("rawtypes")
	@Override
	public void open(Map conf, TopologyContext context,
			SpoutOutputCollector collector) {

		this.collector = collector;
		
		String file = "domain.log";

		try {

			this.fis = new FileInputStream(file);
			this.isr = new InputStreamReader(fis, MacroDef.ENCODING);
			this.br = new BufferedReader(isr);

		} catch (Exception e) {
			e.printStackTrace();
		}

	}

	@Override
	public void close() {

	}

	@Override
	public void activate() {

	}

	@Override
	public void deactivate() {

	}

	@Override
	public void nextTuple() {
		String str = "";
		try {
			while ((str = this.br.readLine()) != null) {
				this.collector.emit(new Values(str));
				Thread.sleep(1000);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

	@Override
	public void ack(Object msgId) {

	}

	@Override
	public void fail(Object msgId) {

	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("str"));

	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		return null;
	}

}
