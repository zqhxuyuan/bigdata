package com.github.lbhat1.mlstorm.batch.evaluate;

import com.github.lbhat1.mlstorm.batch.classifier.Predictor;
import com.github.lbhat1.mlstorm.batch.dataobject.Instance;
import com.github.lbhat1.mlstorm.batch.dataobject.label.Label;

import java.util.List;

public class AccuracyEvaluator extends Evaluator {
    @Override
    public double evaluate(List<Instance> instances, Predictor predictor) {
        double match = 0;
        for (Instance instance : instances) {
            Label label = instance.getLabel();
            if (label != null && predictor.predict(instance).getLabelValue() == label.getLabelValue()) {
                ++match;
            }
        }
        return match;
    }

    public double evaluateR(List<Instance> instances, Predictor predictor) {
        double absError = 0;
        int size = 0;
        for (Instance instance : instances) {
            Label label = instance.getLabel();
            if (label == null) {
                continue;
            }

            Label prediction = predictor.predict(instance);
            /*System.out.println("prediction = " + prediction.getLabelValue());
            System.out.println("given label = " + label.getLabelValue());*/
            absError += Math.abs(prediction.getLabelValue() - label.getLabelValue());
            size++;
        }
        return absError / size;
    }

}
