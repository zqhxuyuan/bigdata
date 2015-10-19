package storm.trident.operation;

import backtype.storm.tuple.Values;
import java.util.Map;
import storm.trident.tuple.TridentTuple;

/**
 *
 * @author mayconbordin
 */
public class TweetFilter extends BaseFunction {

    @Override
    public void execute(TridentTuple tt, TridentCollector tc) {
        Map tweet = (Map) tt.getValue(0);
        
        String lang = (String) tweet.get("lang");
        
        if (lang != null && "en".equals(lang)) {
            Long id = (Long) tweet.get("id");
            String text = (String) tweet.get("text");
            
            if (id != null && text != null) {
                tc.emit(new Values(id, text));
            }
        }
    }
    
}
