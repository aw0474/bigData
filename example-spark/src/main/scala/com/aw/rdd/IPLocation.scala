package com.aw.rdd

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
  * 通过spatrk实现IP地址的查询
  */
object IPLocation {

  def main(args: Array[String]): Unit = {
    var conf: SparkConf = new SparkConf().setAppName("IPLocation").setMaster("local[2]")
    val context = new SparkContext(conf)

    //读取城市IP  获取ip开始、结束、经度、纬度
    var ipdata: RDD[(String, String, String, String)] = context.textFile("").map(_.split("\\|")).map(x => (x(2), x(3), x(x.length - 2), x(x.length - 3)))

    //读取IP地址
    var ips: RDD[Array[String]] = context.textFile("").map(_.split("\\|"))(1)






  }

}
