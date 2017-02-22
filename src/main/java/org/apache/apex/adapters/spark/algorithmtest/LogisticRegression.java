package org.apache.apex.adapters.spark.algorithmtest;


import org.apache.apex.adapters.spark.ApexConf;
import org.apache.apex.adapters.spark.ApexContext;
import org.apache.apex.adapters.spark.ApexRDD;
import org.apache.spark.mllib.classification.LogisticRegressionModel;
import org.apache.spark.mllib.classification.LogisticRegressionWithLBFGS;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.mllib.util.MLUtils;
import scala.reflect.ClassTag;

/**
 * Created by anurag on 19/12/16.
 */
public class LogisticRegression {
    public static  void main(String [] args){
        ApexContext sc= new ApexContext(new ApexConf().setMaster("local[2]").setAppName("Kmeans"));
        String path = "/user/anurag/sample_libsvm_data.txt";
        ClassTag<LabeledPoint> tag = scala.reflect.ClassTag$.MODULE$.apply(LabeledPoint.class);
        ApexRDD<LabeledPoint> data = new ApexRDD<>( MLUtils.loadLibSVMFile(sc,path),tag);
        final LogisticRegressionModel model = new LogisticRegressionWithLBFGS()
                .setNumClasses(10)
                .run(data);
        model.save(sc, "target/tmp/apexLogisticRegressionWithLBFGSModel");
    }

}
