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
public class WordCountTest {
    
    public WordCountTest() {
    }


    @Test
    public void testRunTopology() throws InterruptedException {
        Configuration conf = new Configuration();
        WordCount topology = new WordCount("wordcount", conf);
        
        StormRunner.runTopologyLocally(topology.buildTopology(), "wordcount", conf, 100);
    }

    
}
