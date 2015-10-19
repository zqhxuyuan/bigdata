package storm.trident.constants;

/**
 *
 * @author mayconbordin
 */
public interface TrendingTopicsConstants extends BaseConstants {
    interface Config {
        String SPOUT_TYPE = "tt.spout.type";
        String SINK_TYPE  = "tt.sink.type";
        
        String SPOUT_PATH = "tt.spout.path";
        String SPOUT_MAX_BATCH_SIZE = "tt.spout.max_batch_size";
        
        String SPOUT_THREADS = "tt.spout.threads";
        String PARSER_THREADS = "tt.parser.threads";
        String TOPIC_EXTRACTOR_THREADS = "tt.topic_extractor.threads";
        String COUNTER_THREADS = "tt.counter.threads";
        String TRENDING_THREADS = "tt.trending.threads";
        String SINK_THREADS = "t.sink.threads";
    }
}
