package org.apache.apex.adapters.spark.logisticregression;

import org.apache.apex.adapters.spark.ApexConf;
import org.apache.apex.adapters.spark.ApexContext;
import org.apache.apex.adapters.spark.ApexRDD;
import org.apache.apex.adapters.spark.properties.PathProperties;
import org.apache.spark.mllib.classification.LogisticRegressionModel;
import org.apache.spark.mllib.classification.LogisticRegressionWithLBFGS;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.mllib.util.MLUtils;
import scala.reflect.ClassTag;

/**
 * Created by krushika on 5/1/17.
 */
public class LogisticRegressionTrain {
    public static void main(String[] args){
        PathProperties properties = new PathProperties();
        ApexContext sc= new ApexContext(new ApexConf().setMaster("local[2]").setAppName("Logistic Regression Train Module"));
        ClassTag<LabeledPoint> tag = scala.reflect.ClassTag$.MODULE$.apply(LabeledPoint.class);
        ApexRDD<LabeledPoint> data = new ApexRDD<>( MLUtils.loadLibSVMFile(sc,properties.getProperty("sample_libsvm_HDFS"),-1,1),tag);

        final LogisticRegressionModel model = new LogisticRegressionWithLBFGS()
                .setNumClasses(10)
                .run(data);
        model.save(sc, properties.getProperty("LogisticRegressionModelPath"));


    }

}
