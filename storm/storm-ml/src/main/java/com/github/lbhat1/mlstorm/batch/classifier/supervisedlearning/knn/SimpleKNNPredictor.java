package com.github.lbhat1.mlstorm.batch.classifier.supervisedlearning.knn;

import com.github.lbhat1.mlstorm.batch.dataobject.Instance;
import com.github.lbhat1.mlstorm.batch.dataobject.label.Label;
import com.github.lbhat1.mlstorm.batch.dataobject.label.RegressionLabel;

import java.util.SortedMap;
import java.util.TreeMap;


public class SimpleKNNPredictor extends KNNPredictor {
    @Override
    public Label predict(Instance instance) {
        SortedMap<Double, Label> neighborDistanceWithPrediction = new TreeMap<Double, Label>();
        for (Instance trainingInstance : dataset) {
            neighborDistanceWithPrediction.put(computeDifferenceNorm(instance.getFeatureVector(), trainingInstance.getFeatureVector()), trainingInstance.getLabel());
        }
        return predictLabel(kNearestNeighbors, neighborDistanceWithPrediction);
    }

    protected Label predictLabel(int k, SortedMap<Double, Label> neighborDistanceWithPrediction) {
        double prediction = 0;
        for (int i = 0; i < k; i++) {
            Label nearestNeighbor = neighborDistanceWithPrediction.get(neighborDistanceWithPrediction.firstKey());
            neighborDistanceWithPrediction.remove(neighborDistanceWithPrediction.firstKey());
            prediction += nearestNeighbor.getLabelValue();
        }
        return new RegressionLabel(prediction / k);
    }
}

