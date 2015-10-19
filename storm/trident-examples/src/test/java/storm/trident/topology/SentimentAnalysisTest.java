package storm.trident.topology;

import backtype.storm.Config;
import org.junit.Test;
import static org.junit.Assert.*;
import storm.trident.StormRunner;
import storm.trident.util.Configuration;

/**
 *
 * @author mayconbordin
 */
public class SentimentAnalysisTest {
    
    public SentimentAnalysisTest() {
    }

    @Test
    public void testRunTopology() throws InterruptedException {
        Configuration conf = new Configuration();
        SentimentAnalysis topology = new SentimentAnalysis("sa", conf);
        
        StormRunner.runTopologyLocally(topology.buildTopology(), "sa", conf, 100);
    }
    
}
