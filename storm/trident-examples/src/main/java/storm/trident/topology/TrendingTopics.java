package storm.trident.topology;

import backtype.storm.generated.StormTopology;
import backtype.storm.tuple.Fields;
import storm.trident.Stream;
import storm.trident.TridentTopology;
import storm.trident.constants.BaseConstants;
import static storm.trident.constants.TrendingTopicsConstants.*;
import storm.trident.fluent.GroupedStream;
import storm.trident.operation.JsonParser;
import storm.trident.operation.MaxCount;
import storm.trident.operation.builtin.Count;
import storm.trident.operation.builtin.Debug;
import storm.trident.operation.TopicExtractor;
import storm.trident.spout.FileSpout;
import storm.trident.testing.MemoryMapState;
import storm.trident.util.Configuration;

public class TrendingTopics extends AbstractTopology {
    
    private String spoutType;
    private String sinkType;
    
    private int spoutThreads;
    private int parserThreads;
    private int topicExtractorThreads;
    private int counterThreads;
    private int trendingThreads;
    private int sinkThreads;

    public TrendingTopics(String topologyName, Configuration config) {
        super(topologyName, config);
    }

    @Override
    public void initialize() {
        spoutType = config.getString(Config.SPOUT_TYPE, SourceType.FILE);
        sinkType  = config.getString(Config.SINK_TYPE, SinkType.CONSOLE);
        
        spoutThreads          = config.getInt(Config.SPOUT_THREADS, 1);
        parserThreads         = config.getInt(Config.PARSER_THREADS, 1);
        topicExtractorThreads = config.getInt(Config.TOPIC_EXTRACTOR_THREADS, 1);
        counterThreads        = config.getInt(Config.COUNTER_THREADS, 1);
        trendingThreads       = config.getInt(Config.TRENDING_THREADS, 1);
        sinkThreads           = config.getInt(Config.SINK_THREADS, 1);
    }

    @Override
    public StormTopology buildTopology() {
        TridentTopology topology = new TridentTopology();
        Stream rawTweets = null;
        
        switch (spoutType) {
            case BaseConstants.SourceType.FILE:
                int maxBatchSize = config.getInt(Config.SPOUT_MAX_BATCH_SIZE, 100);
                String spoutPath = config.getString(Config.SPOUT_PATH);
                rawTweets = topology.newStream("spout", new FileSpout(maxBatchSize, spoutPath));
                break;
            case BaseConstants.SourceType.KAFKA:
                createKafkaSpout(spoutType, spoutType);
                break;
            default:
                throw new IllegalArgumentException("Must inform a spout type");
        }
        
        rawTweets = rawTweets.parallelismHint(spoutThreads)
                .shuffle();
        
        Stream tweets = rawTweets.each(new Fields("tweet_str"), new JsonParser(), new Fields("tweet"))
                .parallelismHint(parserThreads)
                .shuffle()
                .project(new Fields("tweet"));
        
        GroupedStream topics = tweets.each(new Fields("tweet"), new TopicExtractor(), new Fields("topic"))
                .parallelismHint(topicExtractorThreads)
                .groupBy(new Fields("topic"));
        
        Stream counts = topics.persistentAggregate(new MemoryMapState.Factory(), new Count(), new Fields("count"))
                .parallelismHint(counterThreads)
                .newValuesStream();
        
        Stream trending = counts.persistentAggregate(new MemoryMapState.Factory(), new Fields("topic", "count"), new MaxCount(), new Fields("top"))
                .newValuesStream()
                .parallelismHint(trendingThreads);
        
        //Stream trending = counts.batchGlobal().partitionAggregate(new Fields("topic", "count"), new MaxCount2(), new Fields("topic", "count"));
        
        if (sinkType.equals(SinkType.CONSOLE)) {
            trending.each(new Fields("top"), new Debug())
                    .parallelismHint(sinkThreads);
        }
        
        return topology.build();
    }
    
}
