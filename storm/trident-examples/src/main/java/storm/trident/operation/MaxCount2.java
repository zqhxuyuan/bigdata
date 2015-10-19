package storm.trident.operation;

import backtype.storm.tuple.Values;
import storm.trident.operation.MaxCount2.Topic;
import storm.trident.tuple.TridentTuple;

/**
 *
 * @author mayconbordin
 */
public class MaxCount2 extends BaseAggregator<Topic> {
    static class Topic {
        String name = null;
        long count = 0;
        
        void set(String name, long count) {
            this.name = name;
            this.count = count;
        }
    }
    
    @Override
    public Topic init(Object batchId, TridentCollector collector) {
        return new Topic();
    }

    @Override
    public void aggregate(Topic state, TridentTuple tuple, TridentCollector collector) {
        if (state.name == null || tuple.getLong(1) > state.count) {
            state.set(tuple.getString(0), tuple.getLong(1));
        }
    }

    @Override
    public void complete(Topic state, TridentCollector collector) {
        collector.emit(new Values(state.name, state.count));
    }
    
}