package storm.trident.operation;

import backtype.storm.tuple.Values;
import java.util.Set;
import storm.trident.model.NLPResource;
import storm.trident.tuple.TridentTuple;

/**
 *
 * @author mayconbordin
 */
public class SentimentTypeScorer extends BaseFunction {
    public static enum Type {Positive, Negative}
    
    private final Set<String> sentimentWords;

    public SentimentTypeScorer(Type type) {
        if (type == Type.Positive) {
            sentimentWords = NLPResource.getPosWords();
        } else {
            sentimentWords = NLPResource.getNegWords();
        }
    }
    
    @Override
    public void execute(TridentTuple tt, TridentCollector tc) {
        String text = tt.getString(0);
        String[] words = text.split(" ");
        
        int numWords = words.length;
        int numSentimentWords = 0;
        
        for (String word : words) {
            if (sentimentWords.contains(word))
                numSentimentWords++;
        }
        
        tc.emit(new Values((float)numSentimentWords / numWords));
    }
    
}
