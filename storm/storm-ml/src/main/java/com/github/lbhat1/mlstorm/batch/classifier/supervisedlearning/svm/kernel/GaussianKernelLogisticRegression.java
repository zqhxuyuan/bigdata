package com.github.lbhat1.mlstorm.batch.classifier.supervisedlearning.svm.kernel;

import com.github.lbhat1.mlstorm.batch.dataobject.FeatureVector;
import com.github.lbhat1.mlstorm.batch.utils.CommandLineUtilities;
import com.github.lbhat1.mlstorm.batch.utils.UtilityFunctions;


public class GaussianKernelLogisticRegression extends KernelLogisticRegression {

    private double gaussianKernelSigma;

    public GaussianKernelLogisticRegression() {
        gaussianKernelSigma = 1;
        if (CommandLineUtilities.hasArg("gaussian_kernel_sigma")) {
            gaussianKernelSigma = CommandLineUtilities.getOptionValueAsFloat("gaussian_kernel_sigma");
        }
    }

    protected double kernelFunction(FeatureVector fv1, FeatureVector fv2) {
        return Math.exp(-1 * UtilityFunctions.computeL2Norm(fv1, fv2) / 2 * gaussianKernelSigma);
    }
}
