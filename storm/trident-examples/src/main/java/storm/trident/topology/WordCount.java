package storm.trident.topology;

import backtype.storm.generated.StormTopology;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import storm.trident.Stream;
import storm.trident.TridentTopology;
import storm.trident.constants.BaseConstants;
import static storm.trident.constants.WordCountConstants.*;
import storm.trident.fluent.GroupedStream;
import storm.trident.operation.builtin.Count;
import storm.trident.operation.builtin.Debug;
import storm.trident.operation.Split;
import storm.trident.spout.FileSpout;
import storm.trident.testing.FixedBatchSpout;
import storm.trident.testing.MemoryMapState;
import storm.trident.util.Configuration;

/**
 *
 * @author mayconbordin
 */
public class WordCount extends AbstractTopology {
    
    private String spoutType;
    private String sinkType;
    
    private int spoutThreads;
    private int splitterThreads;
    private int counterThreads;
    private int sinkThreads;

    public WordCount(String topologyName, Configuration config) {
        super(topologyName, config);
    }

    @Override
    public void initialize() {
        spoutType = config.getString(Config.SPOUT_TYPE, SourceType.FILE);
        sinkType  = config.getString(Config.SINK_TYPE, SinkType.CONSOLE);
        
        spoutThreads    = config.getInt(Config.SPOUT_THREADS, 1);
        splitterThreads = config.getInt(Config.SPLITTER_THREADS, 1);
        counterThreads  = config.getInt(Config.COUNTER_THREADS, 1);
        sinkThreads     = config.getInt(Config.SINK_THREADS, 1);
    }

    @Override
    public StormTopology buildTopology() {
        TridentTopology topology = new TridentTopology();
        
        Stream sentences = null;
        
        switch (spoutType) {
            case BaseConstants.SourceType.FILE:
                int maxBatchSize = config.getInt(Config.SPOUT_MAX_BATCH_SIZE, 100);
                String spoutPath = config.getString(Config.SPOUT_PATH);
                sentences = topology.newStream("spout", new FileSpout(maxBatchSize, spoutPath));
                break;
            case BaseConstants.SourceType.KAFKA:
                createKafkaSpout(spoutType, spoutType);
                break;
            default:
                throw new IllegalArgumentException("Must inform a spout type");
        }
        
        sentences = sentences.parallelismHint(spoutThreads);
                
        GroupedStream words = sentences.each(new Fields("sentence"), new Split(), new Fields("word"))
                .parallelismHint(splitterThreads)
                .groupBy(new Fields("word"));
                
        Stream counts = words.persistentAggregate(new MemoryMapState.Factory(), new Count(), new Fields("count"))
                .parallelismHint(counterThreads)
                .newValuesStream();
        
        if (sinkType.equals(SinkType.CONSOLE)) {
            counts.each(new Fields("word", "count"), new Debug())
                    .parallelismHint(sinkThreads);
        }

        return topology.build();
    }
    
}
