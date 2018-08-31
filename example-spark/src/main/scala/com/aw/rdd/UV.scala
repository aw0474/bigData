package com.aw.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object UV {

  def main(args: Array[String]): Unit = {
    var conf: SparkConf = new SparkConf().setAppName("uv").setMaster("local[2]")
    val context = new SparkContext(conf)

    var data: RDD[String] = context.textFile("")

    var unit: RDD[String] = data.map(_.split(" ")(0))

    var uv: RDD[String] = uv.distinct()

    println("uv的数量："+uv)
  }

}
