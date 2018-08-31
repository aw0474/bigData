package com.aw.demo

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 利用scala开发一个wordcount程序
  */
object WordCount {
  def main(args: Array[String]): Unit = {
    //创建SaprkContext对象
    var conf: SparkConf = new SparkConf().setAppName("WordCount").setMaster("local[2]")
    var sc: SparkContext = new SparkContext(conf)

    //设置日志输出级别
    sc.setLogLevel("WARN")
    //读取数据文件
    var data: RDD[String] = sc.textFile("D:\\aa.txt")
    //切分每一行来获取所有的单词
    var words: RDD[String] = data.flatMap(_.split(" "))
    //每个单词标记为以返回元组
    val wordANndOne: RDD[(String, Int)] = words.map((_,1))
    //将所有单词的相同key的val累加
    val result: RDD[(String, Int)] = wordANndOne.reduceByKey(_+_)
    //按照单词出现次数降序排列
    val sortRdd: RDD[(String, Int)] = result.sortBy(x=>x._2,false)
    //打印输出
    val finalResult: Array[(String, Int)] = sortRdd.collect()


//    finalResult.foreach(x=>println(x))
//    finalResult.foreach(println(_))
    finalResult.foreach(println)

    //关闭sc
    sc.stop()
  }
}
