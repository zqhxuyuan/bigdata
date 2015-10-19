package storm.trident.constants;

/**
 *
 * @author mayconbordin
 */
public interface WordCountConstants extends BaseConstants {
    interface Config {
        String SPOUT_TYPE = "wc.spout.type";
        String SINK_TYPE  = "wc.sink.type";
        
        String SPOUT_PATH = "wc.spout.path";
        String SPOUT_MAX_BATCH_SIZE = "wc.spout.max_batch_size";
        
        String SPOUT_THREADS = "wc.spout.threads";
        String SPLITTER_THREADS = "wc.splitter.threads";
        String COUNTER_THREADS = "wc.counter.threads";
        String SINK_THREADS = "t.sink.threads";
    }
}
