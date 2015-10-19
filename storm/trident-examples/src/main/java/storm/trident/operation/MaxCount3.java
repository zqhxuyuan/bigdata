package storm.trident.operation;

import backtype.storm.tuple.Values;
import storm.trident.operation.MaxCount3.Topic;
import storm.trident.tuple.TridentTuple;

/**
 *
 * @author mayconbordin
 */
public class MaxCount3 implements ReducerAggregator<Topic> {

    @Override
    public Topic init() {
        return null;
    }

    @Override
    public Topic reduce(Topic state, TridentTuple tuple) {
        if (state == null || tuple.getLong(1) > state.count) {
            return new Topic(tuple.getString(0), tuple.getLong(1));
        }
        
        return state;
    }
    
    static class Topic {
        String name = null;
        long count = 0;
        
        Topic(String name, long count) {
            this.name = name;
            this.count = count;
        }
    }
}