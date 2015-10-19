package storm.trident.operation;

import backtype.storm.tuple.Values;
import java.util.Map;
import java.util.StringTokenizer;
import org.apache.commons.lang.StringUtils;
import storm.trident.tuple.TridentTuple;

/**
 *
 * @author mayconbordin
 */
public class TopicExtractor extends BaseFunction {

    @Override
    public void execute(TridentTuple tt, TridentCollector tc) {
        Map tweet = (Map) tt.getValue(0);
        String text = (String) tweet.get("text");
        
        if (text != null) {
            StringTokenizer st = new StringTokenizer(text);

            while (st.hasMoreElements()) {
                String term = (String) st.nextElement();
                if (StringUtils.startsWith(term, "#")) {
                    tc.emit(new Values(term));
                }
            }
        }
    }
    
}
