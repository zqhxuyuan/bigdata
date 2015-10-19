 /*
 * Copyright 2013-2015 Lakshmisha Bhat
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * 		http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.github.lbhat1.mlstorm.streaming.topology.weka.linear;

import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.StormTopology;
import com.github.lbhat1.mlstorm.streaming.bolt.ml.state.weka.MlStormWekaState;
import com.github.lbhat1.mlstorm.streaming.bolt.ml.state.weka.classifier.create.BinaryClassifierFactory;
import com.github.lbhat1.mlstorm.streaming.bolt.ml.state.weka.classifier.query.BinaryClassifierQuery;
import com.github.lbhat1.mlstorm.streaming.bolt.ml.state.weka.classifier.update.BinaryClassifierStateUpdater;
import com.github.lbhat1.mlstorm.streaming.bolt.ml.state.weka.cluster.KmeansClustererState;
import com.github.lbhat1.mlstorm.streaming.bolt.ml.state.weka.utils.WekaClassificationAlgorithms;
import com.github.lbhat1.mlstorm.streaming.spout.ml.MlStormSpout;
import com.github.lbhat1.mlstorm.streaming.spout.ml.weka.AustralianElectricityPricingSpout;
import com.github.lbhat1.mlstorm.streaming.topology.weka.WekaBaseLearningTopology;
import com.github.lbhat1.mlstorm.streaming.utils.MlStormConfig;
import com.github.lbhat1.mlstorm.streaming.utils.fields.MlStormFieldTemplate;
import storm.trident.state.QueryFunction;
import storm.trident.state.StateFactory;
import storm.trident.state.StateUpdater;
import com.github.lbhat1.mlstorm.streaming.utils.fields.FieldTemplate;

import java.util.logging.Level;
import java.util.logging.Logger;

public class SvmTopology extends WekaBaseLearningTopology {
    public static void main(String[] args) throws Exception {
        if (args.length < 4) {
            Logger.getAnonymousLogger().log(Level.SEVERE, " Where are all the arguments? -- use args -- file numWorkers windowSize parallelism");
            System.exit(-1);
        }

        final FieldTemplate template = new MlStormFieldTemplate();
        final int numWorkers = Integer.valueOf(args[1]);
        final int windowSize = Integer.valueOf(args[2]);
        final int parallelism = Integer.valueOf(args[3]);
        final StateUpdater stateUpdater = new BinaryClassifierStateUpdater(template);
        final StateFactory stateFactory = new BinaryClassifierFactory(WekaClassificationAlgorithms.svm.name(), windowSize, template, null /* weka.core.Utils.splitOptions("-C 1.0 -L 0.0010 -P 1.0E-12 -N 0 -V -1 -W 1 -K \"weka.classifiers.functions.supportVector.PolyKernel -C 250007 -E 1.0\"")*/);
        final QueryFunction<MlStormWekaState, Integer> queryFunction = new BinaryClassifierQuery.SvmQuery();
        final QueryFunction<KmeansClustererState, String> parameterUpdateFunction = null;
        final MlStormSpout features = new AustralianElectricityPricingSpout(args[0], template);
        final StormTopology stormTopology = WekaBaseLearningTopology.buildTopology(features, template, parallelism, stateUpdater, stateFactory, queryFunction, parameterUpdateFunction, "svm", "svmUpdate");

        if (numWorkers == 1) {
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("SVM", MlStormConfig.getDefaultMlStormConfig(numWorkers), stormTopology);
        } else {
            StormSubmitter.submitTopology("SVM", MlStormConfig.getDefaultMlStormConfig(numWorkers), stormTopology);
        }
    }
}
