package org.wso2.siddhi.storm.components;

import java.io.*;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

public class FootballDataSpout extends BaseRichSpout {
	private BufferedReader reader;
	SpoutOutputCollector _collector;
	AtomicInteger counter = new AtomicInteger();
	private boolean useDefaultAsStreamName = true;

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		if(useDefaultAsStreamName){
			declarer.declare(new Fields("sid", "ts", "x", "y", "z", "v", "a"));	
		}else{
			declarer.declareStream("PlayStream1", new Fields("sid", "ts", "x", "y", "z"));
		}
	}

	@Override
	public void open(Map conf, TopologyContext context,
			SpoutOutputCollector collector) {
		this._collector = collector;
		try {
			String fileName = "resources/small-game4";
            System.out.println("File path: " + new File(fileName).getAbsolutePath());
			reader = new BufferedReader(new FileReader(fileName),
					10 * 1024 * 1024);
		} catch (FileNotFoundException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public void nextTuple() {
		try {
			if (counter.get() < 1000000) {
				String line = reader.readLine();
				while (line != null) {
					String[] dataStr = line.split(",");
					// System.out.println(tempCout + " "+ count); tempCout++;
					// sid, ts (pico second 10^-12), x (mm), y(mm), z(mm), v(um/s 10^(-6)), a (us^-2), vx, vy, vz, ax, ay, az

					double v_kmh = Double.valueOf(dataStr[5]) * 60 * 60 / 1000000000;
					double a_ms = Double.valueOf(dataStr[6]) / 1000000;

					long time = Long.valueOf(dataStr[1]);
					if ((time >= 10753295594424116l && time <= 12557295594424116l)
							|| (time >= 13086639146403495l && time <= 14879639146403495l)) {
						Object[] data = new Object[] { dataStr[0], time,
								Double.valueOf(dataStr[2]),
								Double.valueOf(dataStr[3]),
								Double.valueOf(dataStr[4]), v_kmh, a_ms };
						if(useDefaultAsStreamName){
							_collector.emit(new Values(data));	
						}else{
							_collector.emit("PlayStream1", new Values(data));
						}
						
						if(counter.incrementAndGet()%10000 == 0){
							System.out.println(">"+ counter.incrementAndGet());	
						}
						
						break;
					}
					line = reader.readLine();
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

}