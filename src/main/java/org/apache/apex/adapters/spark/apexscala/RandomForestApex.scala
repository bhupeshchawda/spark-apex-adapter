package org.apache.apex.adapters.spark.apexscala

import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.RandomForest
import org.apache.spark.mllib.tree.model.RandomForestModel
import org.apache.spark.rdd.RDD

import scala.collection.JavaConverters._
/**
  * Created by anurag on 31/12/16.
  */
class RandomForestApex{

}
object RandomForestApex{
  def trainRegressor(
                      input: RDD[LabeledPoint],
                      categoricalFeaturesInfo: java.util.Map[java.lang.Integer, java.lang.Integer],
                      numTrees: Int,
                      featureSubsetStrategy: String,
                      impurity: String,
                      maxDepth: Int,
                      maxBins: Int,
                      seed: Int): RandomForestModel = {
    RandomForest.trainRegressor(input,
      categoricalFeaturesInfo.asInstanceOf[java.util.Map[Int, Int]].asScala.toMap,
      numTrees, featureSubsetStrategy, impurity, maxDepth, maxBins, seed)
  }
  def trainClassifier(
                       input: RDD[LabeledPoint],
                       numClasses: Int,
                       categoricalFeaturesInfo: java.util.Map[java.lang.Integer, java.lang.Integer],
                       numTrees: Int,
                       featureSubsetStrategy: String,
                       impurity: String,
                       maxDepth: Int,
                       maxBins: Int,
                       seed: Int): RandomForestModel = {
    RandomForest.trainClassifier(input, numClasses,
      categoricalFeaturesInfo.asInstanceOf[java.util.Map[Int, Int]].asScala.toMap,
      numTrees, featureSubsetStrategy, impurity, maxDepth, maxBins, seed)
  }
}