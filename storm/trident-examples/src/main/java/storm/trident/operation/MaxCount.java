package storm.trident.operation;

import storm.trident.tuple.TridentTuple;

/**
 *
 * @author mayconbordin
 */
public class MaxCount implements ReducerAggregator<TridentTuple> {

    @Override
    public TridentTuple init() {
        return null;
    }

    @Override
    public TridentTuple reduce(TridentTuple t, TridentTuple tt) {
        if (t == null) return tt;
        return (tt.getLong(1) > t.getLong(1)) ? tt : t;
    }
}