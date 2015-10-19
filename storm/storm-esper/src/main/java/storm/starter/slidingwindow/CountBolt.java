package storm.starter.slidingwindow;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import org.apache.log4j.Logger;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CountBolt extends WindowedBolt {
	private static final long serialVersionUID = 8849434942882466073L;
	private static final Logger LOG = Logger.getLogger(CountBolt.class);

	private OutputCollector collector;

	public CountBolt(int windowLenInSecs, int emitFrequency){
		super(windowLenInSecs,emitFrequency);
	}
	
	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector; 
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("word","count"));
	}

	@Override
	public void emitCurrentWindowCounts(){
        //获取窗口内的所有tuples. 比如[nathon,mike,nathon,jackson]. 由于WindowBolt添加时直接把Tuple对象添加到Cache中. 这里接收到的也是Tuple
		List<Tuple> windowedTuples = cache.getAndAdvanceWindow();
        System.out.println("totalEvents:"+windowedTuples.size());
        Map<String,Integer> wordcount = new HashMap<>();

		if(windowedTuples!=null && windowedTuples.size()!=0){
			for(Tuple t : windowedTuples){
				//List<Object> objs = t.getValues();
                String word = t.getString(0);
                Integer count = wordcount.get(word);
                if(count != null){
                    count += 1;
                }else{
                    count = 1;
                }
                wordcount.put(word,count);
			}
		}
        for(Map.Entry<String,Integer> item : wordcount.entrySet()){
            collector.emit(new Values(item.getKey(), item.getValue()));
        }
	}
}
