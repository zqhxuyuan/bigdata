package storm.trident.operation;

import backtype.storm.tuple.Values;
import storm.trident.tuple.TridentTuple;

/**
 *
 * @author mayconbordin
 */
public class TextFilter extends BaseFunction {

    @Override
    public void execute(TridentTuple tt, TridentCollector tc) {
        String text = tt.getString(0);
        
        text = text.replaceAll("[^a-zA-Z\\s]", "").trim().toLowerCase();
        tc.emit(new Values(text));
    }
    
}
