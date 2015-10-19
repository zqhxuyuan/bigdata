package storm.starter.slidingwindow;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import org.apache.log4j.Logger;

import java.util.List;
import java.util.Map;

public class SumupBolt extends WindowedBolt {
	private static final long serialVersionUID = 8849434942882466073L;
	private static final Logger LOG = Logger.getLogger(SumupBolt.class);

	private OutputCollector collector;
	
	public SumupBolt(int windowLenInSecs, int emitFrequency){
		super(windowLenInSecs,emitFrequency);
	}
	
	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector; 
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("sum"));
	}

	@Override
	public void emitCurrentWindowCounts(){
		int sum = 0;
        //获取窗口内的所有tuples.
		List<Tuple> windowedTuples = cache.getAndAdvanceWindow();
		Values val = new Values();
		if(windowedTuples!=null && windowedTuples.size()!=0){
			for(Tuple t : windowedTuples){
				List<Object> objs = t.getValues();
				val.addAll(t.getValues());
				if(objs!=null && objs.size() > 0){
					for(Object obj : objs){
						int tmp = Integer.parseInt(obj.toString());
						sum += tmp;
					}
				}
			}
			LOG.info("array to sum up:  " + val.toString());
			collector.emit(new Values(sum+""));
		}
	}
}
