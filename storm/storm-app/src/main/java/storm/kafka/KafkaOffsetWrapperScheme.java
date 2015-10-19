package storm.kafka;

import backtype.storm.spout.Scheme;
import backtype.storm.tuple.Fields;

import java.util.List;

/**
 * Created by zqhxuyuan on 15-4-24.
 *
 * https://blog.deck36.de/no-more-over-counting-making-counters-in-apache-storm-idempotent-using-redis-hyperloglog/
 * https://github.com/DECK36/deck36-storm-idempotent-counter
 */
public class KafkaOffsetWrapperScheme implements Scheme {

    public static final String SCHEME_OFFSET_KEY = "offset";

    private String _offsetTupleKeyName;
    private Scheme _localScheme;

    public KafkaOffsetWrapperScheme() {
        _localScheme = new StringScheme();
        _offsetTupleKeyName = SCHEME_OFFSET_KEY;
    }


    public KafkaOffsetWrapperScheme(Scheme localScheme,
                                    String offsetTupleKeyName) {
        _localScheme = localScheme;
        _offsetTupleKeyName = offsetTupleKeyName;
    }

    public KafkaOffsetWrapperScheme(Scheme localScheme) {
        this(localScheme, SCHEME_OFFSET_KEY);
    }

    public List<Object> deserialize(byte[] bytes) {
        return _localScheme.deserialize(bytes);
    }

    public Fields getOutputFields() {
        List<String> outputFields = _localScheme.getOutputFields().toList();
        outputFields.add(_offsetTupleKeyName);
        return new Fields(outputFields);
    }
}
