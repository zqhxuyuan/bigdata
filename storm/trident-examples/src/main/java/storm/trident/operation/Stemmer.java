package storm.trident.operation;

import backtype.storm.tuple.Values;
import java.util.List;
import java.util.Map;
import storm.trident.model.NLPResource;
import storm.trident.tuple.TridentTuple;

/**
 *
 * @author mayconbordin
 */
public class Stemmer extends BaseFunction {
    private List<String> stopWords;

    @Override
    public void prepare(Map conf, TridentOperationContext context) {
        super.prepare(conf, context);
        stopWords = NLPResource.getStopWords();
    }

    @Override
    public void execute(TridentTuple tt, TridentCollector tc) {
        String text = tt.getString(0);

        for (String word : stopWords) {
            text = text.replaceAll("\\b" + word + "\\b", "");
        }
        
        tc.emit(new Values(text));
    }
    
}
