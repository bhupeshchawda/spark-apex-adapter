package org.apache.apex.adapters.spark.algorithmapex.decisiontree;

import org.apache.apex.adapters.spark.ApexConf;
import org.apache.apex.adapters.spark.ApexContext;
import org.apache.apex.adapters.spark.ApexRDD;
import org.apache.apex.adapters.spark.apexscala.DecisionTreeApex;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.mllib.tree.model.DecisionTreeModel;
import org.apache.spark.mllib.util.MLUtils;
import scala.reflect.ClassTag;

import java.util.HashMap;

/**
 * Created by anurag on 27/12/16.
 */
public class DecisionTree {

    public static void main(String args[]) {
        ApexConf apexConf = new ApexConf().setMaster("local[2]").setAppName("JavaDecisionTreeClassificationExample");
        ApexContext ac = new ApexContext(apexConf);
        // Load and parse the data file.
        String datapath =  "/user/anurag/sample_libsvm_data.txt";;
        ClassTag<LabeledPoint> tag = scala.reflect.ClassTag$.MODULE$.apply(LabeledPoint.class);
        ApexRDD<LabeledPoint> data = new ApexRDD<>(MLUtils.loadLibSVMFile(ac, datapath),tag);
// Split the data into training and test sets (30% held out for testing)
//        ApexRDD<LabeledPoint>[] splits = data.randomSplit(new double[]{0.7, 0.3},11L);
//        ApexRDD<LabeledPoint> trainingData = splits[0];
//        ApexRDD<LabeledPoint> testData = splits[1];
// Set parameters.
//  Empty categoricalFeaturesInfo indicates all features are continuous.
        Integer numClasses = 2;
        HashMap<Integer, Integer> categoricalFeaturesInfo = new HashMap<>();
        String impurity = "gini";
        Integer maxDepth = 5;
        Integer maxBins = 32;

// Train a DecisionTree model for classification.
        final DecisionTreeModel model = DecisionTreeApex.trainClassifier(data, numClasses,
                categoricalFeaturesInfo, impurity, maxDepth, maxBins);

// Save and load model
        model.save(ac, "target/tmp/myDecisionTreeClassificationModel");


    }
}
