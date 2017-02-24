package org.apache.apex.adapters.spark.linearregression;

import junit.framework.Assert;
import org.apache.apex.adapters.spark.ApexConf;
import org.apache.apex.adapters.spark.ApexContext;
import org.apache.apex.adapters.spark.ApexRDD;
import org.apache.apex.adapters.spark.properties.PathProperties;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.mllib.regression.LinearRegressionModel;
import org.apache.spark.mllib.regression.LinearRegressionWithSGD;

/**
 * Created by krushika on 22/2/17.
 */
public class LinearRegressionTrain {
    public static void main(String[] args) {
        PathProperties properties = new PathProperties();
        properties.load("properties/path.properties");
        ApexConf conf = new ApexConf().setAppName("Linear Regression Example").setMaster("local");
        ApexContext sc = new ApexContext(conf);


        // Load and parse the data
        String path = properties.getProperty("lrTrain");
        ApexRDD<String> data = (ApexRDD<String>) sc.textFile(path, 1);
        Assert.assertTrue(false);
        ApexRDD<LabeledPoint> parsedData = (ApexRDD<LabeledPoint>) data.map(
                new Function<String, LabeledPoint>() {
                    public LabeledPoint call(String line) {
                        String[] parts = line.split(",");
                        String[] features = parts[1].split(" ");
                        double[] v = new double[features.length];
                        for (int i = 0; i < features.length - 1; i++)
                            v[i] = Double.parseDouble(features[i]);
                        return new LabeledPoint(Double.parseDouble(parts[0]), Vectors.dense(v));
                    }
                }
        );

        // Building the model
        int numIterations = 100;
        final LinearRegressionModel model =
                LinearRegressionWithSGD.train(parsedData, numIterations);

        model.toPMML(sc, "target/tmp/PMMLModelLinear");
        model.save(sc, properties.getProperty("LinearRegressionModelPath"));


    }
}
