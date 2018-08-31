package com.aw.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object PV {

  def main(args: Array[String]): Unit = {

    var conf: SparkConf = new SparkConf().setAppName("pv").setMaster("local[2]")
    var context: SparkContext = new SparkContext(conf)

    var data: RDD[String] = context.textFile("")

    val pv = data.count()

    println("pv总数："+pv)
  }


}
