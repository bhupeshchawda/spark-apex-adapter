package org.apache.apex.adapters.spark.logisticregression;

import org.apache.apex.adapters.spark.properties.PathProperties;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.classification.LogisticRegressionModel;
import org.apache.spark.mllib.evaluation.MulticlassMetrics;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.mllib.util.MLUtils;
import scala.Tuple2;
import scala.reflect.ClassTag;

/**
 * Created by krushika on 5/1/17.
 */
public class LogisticRegressionTest {
    public static void main(String[] args){
        PathProperties properties = new PathProperties();
        properties.load("properties/path.properties");

        SparkContext sc = new SparkContext(new SparkConf().setAppName("Logistic Regression Test Module").setMaster("local"));
        ClassTag<LabeledPoint> tag = scala.reflect.ClassTag$.MODULE$.apply(LabeledPoint.class);
        JavaRDD<LabeledPoint> data = new JavaRDD<>( MLUtils.loadLibSVMFile(sc,properties.getProperty("testData")),tag);

        final LogisticRegressionModel model = LogisticRegressionModel.load(sc,properties.getProperty("LogisticRegressionModelPath"));

        JavaRDD<Tuple2<Object, Object>> predictionAndLabels = data.map(
                new Function<LabeledPoint, Tuple2<Object, Object>>() {
                    public Tuple2<Object, Object> call(LabeledPoint p) {
                        Double prediction = model.predict(p.features());
                        return new Tuple2<Object, Object>(prediction, p.label());
                    }
                }
        );

        MulticlassMetrics metrics = new MulticlassMetrics(predictionAndLabels.rdd());
        double accuracy = metrics.accuracy();
        System.out.println("Accuracy = " + accuracy);
    }
}
