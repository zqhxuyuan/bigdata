package storm.trident.topology;

import backtype.storm.generated.StormTopology;
import backtype.storm.tuple.Fields;
import storm.trident.Stream;
import storm.trident.TridentTopology;
import static storm.trident.constants.SentimentAnalysisConstants.*;
import storm.trident.operation.JsonParser;
import storm.trident.operation.SentimentScorer;
import storm.trident.operation.SentimentTypeScorer;
import storm.trident.operation.SentimentTypeScorer.Type;
import storm.trident.operation.Stemmer;
import storm.trident.operation.TextFilter;
import storm.trident.operation.TweetFilter;
import storm.trident.operation.builtin.Debug;
import storm.trident.spout.FileSpout;
import storm.trident.util.Configuration;

/**
 *
 * @author mayconbordin
 */
public class SentimentAnalysis extends AbstractTopology {
    
    private String spoutType;
    private String sinkType;
    
    private int spoutThreads;
    private int parserThreads;
    private int tweetFilterThreads;
    private int textFilterThreads;
    private int stemmerThreads;
    private int posScorerThreads;
    private int negScorerThreads;
    private int scorerThreads;
    private int sinkThreads;
    

    public SentimentAnalysis(String topologyName, Configuration config) {
        super(topologyName, config);
    }

    @Override
    public void initialize() {
        spoutType = config.getString(Config.SPOUT_TYPE, SourceType.FILE);
        sinkType  = config.getString(Config.SINK_TYPE, SinkType.CONSOLE);
        
        spoutThreads       = config.getInt(Config.SPOUT_THREADS, 1);
        parserThreads      = config.getInt(Config.PARSER_THREADS, 1);
        tweetFilterThreads = config.getInt(Config.TWEET_FILTER_THREADS, 1);
        textFilterThreads  = config.getInt(Config.TEXT_FILTER_THREADS, 1);
        stemmerThreads     = config.getInt(Config.STEMMER_THREADS, 1);
        posScorerThreads   = config.getInt(Config.POS_SCORER_THREADS, 1);
        negScorerThreads   = config.getInt(Config.NEG_SCORER_THREADS, 1);
        scorerThreads      = config.getInt(Config.SCORER_THREADS, 1);
        sinkThreads        = config.getInt(Config.SINK_THREADS, 1);
    }

    @Override
    public StormTopology buildTopology() {
        TridentTopology topology = new TridentTopology();
        
        Stream rawTweets = null;

        switch (spoutType) {
            case SourceType.FILE:
                int maxBatchSize = config.getInt(Config.SPOUT_MAX_BATCH_SIZE, 100);
                String spoutPath = config.getString(Config.SPOUT_PATH);
                rawTweets = topology.newStream("spout", new FileSpout(maxBatchSize, spoutPath, "tweet_str"));
                break;
            case SourceType.KAFKA:
                createKafkaSpout(spoutType, spoutType);
                break;
            default:
                throw new IllegalArgumentException("Must inform a spout type");
        }

        rawTweets.parallelismHint(spoutThreads)
                .shuffle();
                
        Stream tweets = rawTweets.each(new Fields("tweet_str"), new JsonParser(), new Fields("tweet"))
                .parallelismHint(parserThreads)
                .shuffle()
                .project(new Fields("tweet"));
                
        Stream parsedTweets = tweets.each(new Fields("tweet"), new TweetFilter(), new Fields("id", "text"))
                .parallelismHint(tweetFilterThreads)
                .shuffle()
                .project(new Fields("id", "text"));
                
        Stream cleanTweets = parsedTweets.each(new Fields("text"), new TextFilter(), new Fields("text_clean"))
                .parallelismHint(textFilterThreads)
                .shuffle()
                .project(new Fields("id", "text_clean"));
                
        Stream stemmedTweets = cleanTweets.each(new Fields("text_clean"), new Stemmer(), new Fields("text_stemmed"))
                .parallelismHint(stemmerThreads)
                .shuffle()
                .project(new Fields("id", "text_stemmed"));
                
        Stream posScores = stemmedTweets.each(new Fields("text_stemmed"), new SentimentTypeScorer(Type.Positive), new Fields("pos_score"))
                .parallelismHint(posScorerThreads)
                .shuffle()
                .project(new Fields("id", "text_stemmed", "pos_score"));
                
        Stream negScores = stemmedTweets.each(new Fields("text_stemmed"), new SentimentTypeScorer(Type.Negative), new Fields("neg_score"))
                .parallelismHint(negScorerThreads)
                .shuffle()
                .project(new Fields("id", "neg_score"));
        
        Stream joinedScores = topology.join(posScores, new Fields("id"), negScores, new Fields("id"), new Fields("id", "text", "pos_score", "neg_score"))
                .parallelismHint(sinkThreads);
                
        Stream scores = joinedScores.each(new Fields("pos_score", "neg_score"), new SentimentScorer(), new Fields("score"))
                .parallelismHint(scorerThreads)
                .shuffle();
                
        if (sinkType.equals(SinkType.CONSOLE)) {
            scores.each(scores.getOutputFields(), new Debug())
                    .parallelismHint(sinkThreads);
        }
        
        return topology.build();
    }
    
}
