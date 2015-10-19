package com.github.shuliga.bolt.stream;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import com.github.shuliga.utils.FileUtil;

import java.io.FileWriter;
import java.io.IOException;
import java.util.Map;

/**
 * User: yshuliga
 * Date: 14.01.14 14:27
 */
public class JsonWriterBolt  extends BaseRichBolt {

	public final String rootPath;
	private OutputCollector _collector;

	public JsonWriterBolt(String rootPath){
		this.rootPath = rootPath;
	}

	@Override
	public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
		this._collector = outputCollector;
	}

	//"sourceType", "eventId", "json"
	@Override
	public void execute(Tuple tuple) {
		String sourceType = tuple.getString(0);
		String eventId = tuple.getString(1);
		String json = tuple.getString(2);
		if (eventId != null && !eventId.isEmpty()){
			writeToFile(FileUtil.createJsonPath(rootPath, eventId, sourceType), json);
		}
		_collector.ack(tuple);
	}

	private void writeToFile(String path, String json) {
		try {
			FileWriter fileWriter = new FileWriter(path);
			fileWriter.write(json);
			fileWriter.close();
		} catch (IOException e) {
			e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields());
	}


}
