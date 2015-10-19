package com.github.shuliga.spout;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import com.github.shuliga.http.HttpServerQueue;
import org.apache.commons.lang.time.StopWatch;

import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * User: yshuliga
 * Date: 21.11.13 17:16
 */
public class HttpServerQueueSpout extends BaseRichSpout {

	public static final int TIMEOUT = 5;
	SpoutOutputCollector _collector;
	TopologyContext _context;

	public HttpServerQueueSpout(){

	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("word"));
	}

	@Override
	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		_collector = collector;
		_context = context;
		System.out.println("New HttpServerQueueSpout created");
	}

	@Override
	public void nextTuple() {
		try {
			HttpServerQueue.SpoutRequest request = HttpServerQueue.getQueue().poll(TIMEOUT, TimeUnit.SECONDS);
			if (request != null) {
				_collector.emit(new Values(request.comment));
			} else {
				System.out.println(" - empty queue detected");
			}
		} catch (InterruptedException e) {
			System.out.println("- polling the interrupted queue, ignored");
		}

	}
}
