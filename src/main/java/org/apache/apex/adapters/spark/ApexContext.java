package org.apache.apex.adapters.spark;

import org.apache.apex.adapters.spark.operators.BaseInputOperatorSerializable;
import org.apache.apex.adapters.spark.operators.InputSplitOperator;
import org.apache.apex.adapters.spark.properties.PathProperties;
import org.apache.spark.SparkContext;
import org.apache.spark.rdd.RDD;

import java.io.Serializable;

//@DefaultSerializer(JavaSerializer.class)
public class ApexContext extends SparkContext implements Serializable
{
  public ApexContext()
  {
    super(new ApexConf());
  }

  public ApexContext(ApexConf config)
  {
    super(config);
  }
  public RDD<String> createRDD(String path, int minPartitions){
      PathProperties properties = new PathProperties();
      String fs = properties.getProperty("fs");
      if (fs.equals("hdfs")){
          ApexRDD rdd = new ApexRDD<>(this);
          InputSplitOperator fileInput = rdd.getDag().addOperator(System.currentTimeMillis()+ " Input ", InputSplitOperator.class);
          rdd.currentOperator = fileInput;
          rdd.currentOperatorType = ApexRDD.OperatorType.INPUT;
          rdd.currentOutputPort = fileInput.output;
          rdd.controlOutput = fileInput.controlOut;
          fileInput.path = path;
          fileInput.minPartitions = minPartitions;
          return rdd;
      }
      else {
          ApexRDD rdd = new ApexRDD<>(this);
          BaseInputOperatorSerializable fileInput = rdd.getDag().addOperator(System.currentTimeMillis() + " Input ", BaseInputOperatorSerializable.class);
          rdd.currentOperator = fileInput;
          rdd.currentOperatorType = ApexRDD.OperatorType.INPUT;
          rdd.currentOutputPort = fileInput.output;
          rdd.controlOutput = fileInput.controlOut;
          fileInput.path = path;
          fileInput.minPartitions = minPartitions;
          return rdd;
      }

  }

  @Override
  public RDD<String> textFile(String path, int minPartitions)
  {
    return createRDD(path, minPartitions);
  }
}
