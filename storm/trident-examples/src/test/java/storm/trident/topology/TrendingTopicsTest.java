/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
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
public class TrendingTopicsTest {
    
    public TrendingTopicsTest() {
    }

    @Test
    public void testRunTopology() throws InterruptedException {
        Configuration conf = new Configuration();
        TrendingTopics topology = new TrendingTopics("tt", conf);
        
        StormRunner.runTopologyLocally(topology.buildTopology(), "tt", conf, 100);
    }
    
}
