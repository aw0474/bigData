package com.aw.demo

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD


//todo:利用scala语言开发一个spark的wordcount程序（集群运行）
object WordCount_Online {

  def main(args: Array[String]): Unit = {
    //1、创建sparkConf对象,设置appName
    val sparkConf: SparkConf = new SparkConf().setAppName("WordCount_Online")

    //2、创建SparkContext,它是所有spark程序的入口，它会创建DAGScheduler和TaskScheduler对象
    val sc = new SparkContext(sparkConf)

    //设置日志输出级别
    sc.setLogLevel("WARN")

    //3、读取HDFS数据文件
    val data: RDD[String] = sc.textFile(args(0))

    //4、切分每一行,获取所有的单词
    val words: RDD[String] = data.flatMap(_.split(" "))

    //5、每个单词计为1 （单词，1）(hadoop,1) (hadoop,1)(spark,1)(hive,1)
    val wordAndOne: RDD[(String, Int)] = words.map(x=>(x,1))

    //6、相同单词出现的所有1进行累加  (hadoop,List(1,1,1,1))(hadoop,1+1+1+1=4)
    val result: RDD[(String, Int)] = wordAndOne.reduceByKey((x,y)=>x+y)

    //7、保存结果数据到hdfs上
    result.saveAsTextFile(args(1))

    //8、关闭sc
    sc.stop()
  }

}
