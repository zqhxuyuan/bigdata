package storm.trident.operation;

import backtype.storm.tuple.Values;
import storm.trident.tuple.TridentTuple;

/**
 *
 * @author mayconbordin
 */
public class SentimentScorer extends BaseFunction {

    @Override
    public void execute(TridentTuple tt, TridentCollector tc) {
        Float pos = tt.getFloat(0);
        Float neg = tt.getFloat(1);
        
        String score = pos > neg ? "positive" : "negative";
        tc.emit(new Values(score));
    }
    
}
