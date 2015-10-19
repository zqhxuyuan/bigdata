package storm.trident.constants;

/**
 *
 * @author mayconbordin
 */
public interface SentimentAnalysisConstants extends BaseConstants {
    interface Config {
        String SPOUT_TYPE = "sa.spout.type";
        String SINK_TYPE  = "sa.sink.type";
        
        String SPOUT_PATH = "sa.spout.path";
        String SPOUT_MAX_BATCH_SIZE = "sa.spout.max_batch_size";
        
        String SPOUT_THREADS = "sa.spout.threads";
        String PARSER_THREADS = "sa.parser.threads";
        String TWEET_FILTER_THREADS = "sa.tweet_filter.threads";
        String TEXT_FILTER_THREADS = "sa.text_filter.threads";
        String STEMMER_THREADS = "sa.stemmer.threads";
        String POS_SCORER_THREADS = "sa.pos_scorer.threads";
        String NEG_SCORER_THREADS = "sa.neg_scorer.threads";
        String SCORER_THREADS = "sa.scorer.threads";
        String SINK_THREADS = "sa.sink.threads";
    }
}
