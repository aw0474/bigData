package com.aw.demo;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * //todo:利用java语言开发一个spark的wordcount程序
 */
public class WordCount_java {

    public static void main(String[] args) {
        //1、创建sparkConf
        SparkConf sparkConf = new SparkConf().setAppName("WordCount_Java").setMaster("local[2]");

        //2、创建JavaSparkContext
        JavaSparkContext jsc = new JavaSparkContext(sparkConf);

        //3、读取数据文件
        JavaRDD<String> dataJavaRDD = jsc.textFile("E:\\words.txt");

        //4、切分每一行，获取所有的单词
        JavaRDD<String> wordsJavaRDD = dataJavaRDD.flatMap(new FlatMapFunction<String, String>() {
            public Iterator<String> call(String line) throws Exception {
                //切分每一行
                String[] split = line.split(" ");
                return Arrays.asList(split).iterator();
            }
        });

        //5、每个单词计为1  （单词，1）
        JavaPairRDD<String, Integer> wordAndOneJavaPairRDD = wordsJavaRDD.mapToPair(new PairFunction<String, String, Integer>() {
            public Tuple2<String, Integer> call(String word) throws Exception {
                return new Tuple2<String, Integer>(word, 1);
            }
        });

        //6、相同单词出现所有的1累加
        JavaPairRDD<String, Integer> resultJavaPairRDD = wordAndOneJavaPairRDD.reduceByKey(new Function2<Integer, Integer, Integer>() {
            //(x,y)=>x+y
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });

        //按照单词出现的次数降序排列   (单词，次数)------->(次数，单词).sortByKey()--------->(单词，次数)
        JavaPairRDD<Integer, String> reverseJavaPairRDD = resultJavaPairRDD.mapToPair(new PairFunction<Tuple2<String, Integer>, Integer, String>() {
            public Tuple2<Integer, String> call(Tuple2<String, Integer> tuple) throws Exception {
                return new Tuple2<Integer, String>(tuple._2, tuple._1);
            }
        }).sortByKey(false);

        // 降序后(次数,单词) ------->(单词，次数)
        JavaPairRDD<String, Integer> sortedJavaPairRDD = reverseJavaPairRDD.mapToPair(new PairFunction<Tuple2<Integer, String>, String, Integer>() {
            public Tuple2<String, Integer> call(Tuple2<Integer, String> tuple) throws Exception {
                return new Tuple2<String, Integer>(tuple._2, tuple._1);
            }
        });

        //7、收集打印结果数据
        List<Tuple2<String, Integer>> finalResult = sortedJavaPairRDD.collect();
        //遍历
        for (Tuple2<String, Integer> tuple : finalResult) {
            System.out.println("单词："+tuple._1+" 次数："+tuple._2);
        }

        //8、关闭jsc
        jsc.stop();
    }
}
