package org.apache.apex.adapters.spark.apexscala

import org.apache.apex.adapters.spark.ApexRDD
import org.apache.spark.rdd.RDD

import scala.collection.{Map, mutable}
import scala.reflect.ClassTag

/**
  * Created by anurag on 16/12/16.
  */
 class PairApexRDDFunction[K, V](self: RDD[(K, V)])
                            ( kt: ClassTag[K], vt: ClassTag[V], ord: Ordering[K] = null){
  def test(): Unit = {

  }
  def collectAsApexMap(): Map[K, V] = {
    val data = self.collect()
    val map = new mutable.HashMap[K, V]
    map.sizeHint(data.length)
    data.foreach { pair => map.put(pair._1, pair._2) }
    map
  }
}
