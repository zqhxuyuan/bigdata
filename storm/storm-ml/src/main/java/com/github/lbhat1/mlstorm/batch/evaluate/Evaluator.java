package com.github.lbhat1.mlstorm.batch.evaluate;

import com.github.lbhat1.mlstorm.batch.classifier.Predictor;
import com.github.lbhat1.mlstorm.batch.dataobject.Instance;

import java.util.List;

public abstract class Evaluator {

    public abstract double evaluate(List<Instance> instances, Predictor predictor);
}
