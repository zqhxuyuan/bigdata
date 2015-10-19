package com.github.lbhat1.mlstorm.batch.classifier.supervisedlearning.decisiontreetrainer;

import com.github.lbhat1.mlstorm.batch.dataobject.Instance;

import java.util.HashMap;
import java.util.List;

public interface IDecisionTree {
    void construct(List<Instance> instances);

    void addNode(HashMap<Long, Double> node);

    void removeNode(HashMap<Long, Double> node);
}
