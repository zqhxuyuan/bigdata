package storm.meta.helloworld;

import java.util.Map;
import storm.meta.base.MacroDef;
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
public class WordNormalizerBolt implements IRichBolt  {
	
	/**
	 * 锟矫诧拷锟街斤拷spout锟斤拷锟酵癸拷锟斤拷锟斤拷一锟叫硷拷录锟斤拷锟叫憋拷准锟斤拷锟斤拷锟斤拷
	 * 锟斤拷锟斤拷锟斤拷录锟斤拷殖傻锟斤拷剩锟斤拷锟斤拷锟街蝗omain锟侥硷拷锟叫的碉拷一锟叫猴拷锟斤拷锟揭伙拷锟�(锟斤拷式锟角憋拷准锟斤拷)
	 */

	private OutputCollector collector;
	
	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		this.collector = collector;
		
	}

	@Override
	public void execute(Tuple input) {
		String sentence = input.getString(0);
		//锟斤拷'/t'锟斤拷锟�
		String[] words = sentence.split(MacroDef.FLAG_TABS);
		//只锟窖碉拷一锟叫猴拷锟斤拷锟揭伙拷蟹锟斤拷统锟饺�

		if (words.length != 0) {
			
			String domain = words[0];
			String[] do_word = domain.split("\\.");
			
			for (int i = 0; i < do_word.length; i++) {
				String word = do_word[i].trim();
				//锟斤拷锟斤拷锟斤拷也锟斤拷殖傻锟斤拷锟�
				this.collector.emit(new Values(word));
			}
			
			//锟斤拷锟斤拷锟街憋拷臃锟斤拷统锟饺�
			String word = words[4].trim();
			this.collector.emit(new Values(word));
		}
		
	}

	@Override
	public void cleanup() {
		
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("word"));
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		return null;
	}

}
