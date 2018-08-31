package com.aw.rdd

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object TopN {

  def main(args: Array[String]): Unit = {
    var conf: SparkConf = new SparkConf().setAppName("uv").setMaster("local[2]")
    val context = new SparkContext(conf)

    var data: RDD[String] = context.textFile("")

    var tuples: Array[(String, Int)] = data.map(_.split(" ")).filter(_.length > 10).map(x => (x(10), 1)).reduceByKey(_ + _).sortBy(_._2, false).take(5)

    println("排名前五："+tuples)
  }

}
