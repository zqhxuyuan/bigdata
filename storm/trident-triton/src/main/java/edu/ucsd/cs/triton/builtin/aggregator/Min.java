package edu.ucsd.cs.triton.builtin.aggregator;

import clojure.lang.Numbers;
import storm.trident.operation.CombinerAggregator;
import storm.trident.tuple.TridentTuple;

public class Min implements CombinerAggregator<Number> {
  @Override
  public Number init(TridentTuple tuple) {
      return (Number) tuple.getValue(0);
  }

  @Override
  public Number combine(Number val1, Number val2) {
      return (Number) Numbers.min(val1, val2);
  }

  @Override
  public Number zero() {
      return 0;
  }
  
}
