package com.github.lbhat1.mlstorm.batch.classifier.supervisedlearning.svm.kernel;

import com.github.lbhat1.mlstorm.batch.dataobject.FeatureVector;

public class LinearKernelLogisticRegression extends KernelLogisticRegression {
    protected double kernelFunction(FeatureVector fv1, FeatureVector fv2) {
        return computeLinearCombination(fv1, fv2);
    }
}
