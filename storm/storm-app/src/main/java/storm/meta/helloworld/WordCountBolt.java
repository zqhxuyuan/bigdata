package storm.meta.helloworld;

import java.util.HashMap;
import java.util.Map;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

/** 
 * @author blogchong
 * @Blog   www.blogchong.com
 * @email  blogchong@gmail.com
 * @QQ_G   191321336
 * @version 2014锟斤拷11锟斤拷9锟斤拷 锟斤拷锟斤拷10:09:43
 */

@SuppressWarnings("serial")
public class WordCountBolt implements IRichBolt  {
	
	/**
	 * 锟斤拷锟絙olt锟斤拷锟秸达拷normalizer锟斤拷锟斤拷牡锟斤拷锟斤拷锟斤拷锟�
	 * 然锟斤拷锟斤拷荼锟斤拷锟斤拷氐锟揭伙拷锟絤ap锟叫ｏ拷实时锟侥斤拷统锟狡斤拷锟斤拷锟饺�
	 */
		
	Integer id;
	String name;
	Map<String, Integer> counters;

	private OutputCollector collector;
	
	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		
		this.collector = collector;
		
		//new一锟斤拷hashmap实锟斤拷
		this.counters = new HashMap<String, Integer>();
		
		this.name = context.getThisComponentId();
		
		this.id = context.getThisTaskId();
		
	}

	@Override
	public void execute(Tuple input) {
		String str = input.getString(0);
		
		if (!counters.containsKey(str)) {
			counters.put(str, 1);
		} else {
			//锟斤拷锟斤拷map锟斤拷锟叫硷拷录锟斤拷锟斤拷锟斤拷锟�+1
			Integer c = counters.get(str) + 1;
			counters.put(str, c);
		}
		
		String send_str = null;
		
		int count = 0;
		
		for (String key : counters.keySet()) {
			if (count == 0) {
				send_str = "[" + key + " : " + counters.get(key) + "]";
			} else {
				send_str = send_str + ", [" + key + " : " + counters.get(key) + "]";
			}
			
			count++;
			
		}
		
		send_str = "The count:" + count + " #### " + send_str; 
		
		this.collector.emit(new Values(send_str));
		
		
	}

	@Override
	public void cleanup() {
		
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("send_str"));
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		return null;
	}

}
