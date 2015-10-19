package storm.starter.slidingwindow;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import org.apache.log4j.Logger;
import storm.starter.model.Pair;

import java.util.*;

public class DistinctCountBolt extends WindowedBolt {
	private static final long serialVersionUID = 8849434942882466073L;
	private static final Logger LOG = Logger.getLogger(DistinctCountBolt.class);

	private OutputCollector collector;

	public DistinctCountBolt(int windowLenInSecs, int emitFrequency){
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
        //[Tuple(nathon,mike), Tuple(nathon,jackson), Tuple(nathon,mike), Tuple(mike,jackson), ...]
        //需要对相同的去重, 比如两个Tuple(nathon,mike)只能有一个. 最后为Tuple(nathon,mike), Tuple(nathon,jackson), 最后nathon的distinct count=2
        //怎么去重: 直接将List转为Set. 问题是两个相同的left,right组成的Tuple会被认为是不同的Tuple吗??
		List<Tuple> windowedTuples = cache.getAndAdvanceWindow();
        Set<Pair<String,String>> windowedTupleSets = new HashSet<>();
        for(Tuple tuple : windowedTuples){
            windowedTupleSets.add(new Pair<String,String>(tuple.getString(0), tuple.getString(1)));
        }
        //打印出来两者是一样的,说明即使left,right相同(没有重写equals,hashCode),两个Tuple则被认为是不同的对象!  TOTAL EVETNS: 184--(set):184
        //将Tuple转换为Pair之后, 现在TOTAL EVETNS: 184--(set):25 说明已经去重了!
        System.out.println("TOTAL EVETNS: " + windowedTuples.size() + "--(set):" + windowedTupleSets.size());
        Map<String,Integer> wordcount = new HashMap<>();

		if(windowedTupleSets!=null && windowedTupleSets.size()!=0){
			for(Pair<String,String> t : windowedTupleSets){
                String word = t.getLeft();
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
