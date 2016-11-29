package org.apache.apex.adapters.spark;


import java.io.Serializable;

import org.junit.Before;
import org.junit.Test;

import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.mllib.util.MLUtils;

import scala.reflect.ClassTag;

public class TestNaiveBayes implements Serializable
{

  public static ApexContext sc;

//  @Test
//  public TestNaiveBayes(ApexContext sc)
//  {
//    String path = "src/main/resources/data/sample_libsvm_data.txt";
//    ClassTag<LabeledPoint> tag = scala.reflect.ClassTag$.MODULE$.apply(LabeledPoint.class);
//    ApexRDD<LabeledPoint> inputData = new ApexRDD<LabeledPoint> (MLUtils.loadLibSVMFile(sc, path), tag);
//    System.out.println("Count: " + inputData.count());
//    JavaRDD<LabeledPoint>[] tmp = inputData.randomSplit(new double[]{0.6, 0.4});
//    ApexRDD<LabeledPoint> training = new ApexRDD<LabeledPoint>(tmp[0].rdd(), tag); // training set
//    ApexRDD<LabeledPoint> test = new ApexRDD<LabeledPoint>(tmp[1].rdd(), tag); // test set
//    final NaiveBayesModel model = NaiveBayes.train(training.rdd(), 1.0);
//    JavaPairRDD<Double, Double> predictionAndLabel =
//        test.mapToPair(new PairFunction<LabeledPoint, Double, Double>() {
//          @Override
//          public Tuple2<Double, Double> call(LabeledPoint p) {
//            return new Tuple2<>(model.predict(p.features()), p.label());
//          }
//        });
//      double accuracy = predictionAndLabel.filter(new Function<Tuple2<Double, Double>, Boolean>() {
//        @Override
//        public Boolean call(Tuple2<Double, Double> pl) {
//          return pl._1().equals(pl._2());
//        }
//      }).count() / (double) test.count();
//
//      // Save and load model
//      model.save(sc.sc(), "target/tmp/myNaiveBayesModel");
//      NaiveBayesModel sameModel = NaiveBayesModel.load(sc.sc(), "target/tmp/myNaiveBayesModel");
//      System.out.println("Accuracy: " + accuracy);
//  }

  @Test
  public void testApexContext()
  {
    String path = "src/test/resources/data/sample_libsvm_data.txt";
    ClassTag<LabeledPoint> tag = scala.reflect.ClassTag$.MODULE$.apply(LabeledPoint.class);
    ApexRDD<LabeledPoint> inputData = new ApexRDD<LabeledPoint> (MLUtils.loadLibSVMFile(sc, path), tag);
    System.out.println("Count: " + inputData.count());
  }

  @Before
  public void setup()
  {
    // JavaSparkContext sc  = new JavaSparkContext(new SparkConf().setMaster("local[2]").setAppName("TestNaiveBayes"));
    sc  = new ApexContext(new ApexConf().setMaster("local").setAppName("ApexApp"));
  }
}
