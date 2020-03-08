package com.study

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.util.parsing.json.JSON


object HelloWorld {
  def main(args: Array[String]): Unit = {
    println("hello world")
    val conf = new SparkConf().setMaster("local[2]").setAppName("NetworkWordCount")
    val ssc = new StreamingContext(conf, Seconds(10))

    val lines:DStream[String]  = ssc.socketTextStream("127.0.0.1", 9000)//.textFileStream("E:/study/data/scala_data.txt").map(_.toUpperCase())

    val words:DStream[String]=lines.flatMap(_.split(" "))

//


    val wordCounts=words.map((_,1)).reduceByKey(_+_)
    wordCounts.print()
    ssc.start() //开始计算
    ssc.awaitTermination()  //等待计算结束

    /*val result =input
                    .flatMap(line => line.split("[,]"))
                    .map(word => {
                      println("word" +word)
                      (word, 1)
                    })
                    .reduceByKey((count1, count2) => count1 + count2)
     .saveAsTextFiles("bc","txt")
    ssc.start()
    ssc.awaitTermination()*/
//    result.foreachRDD(rdd=>rdd.foreach(e=>println("v: "+e)))

  }
}

//object ScalaSparkContext {
//  def main(args: Array[String]) {
//    val sparkContext = new ScalaSparkContext()
//    val sc = sparkContext.sc
//    val input = sc.textFile("E:\\study\\data\\scala_data.txt").map(_.toLowerCase)
//    input
//      .flatMap(line => line.split("[,]"))
//      .map(word => (word, 1))
//      .reduceByKey((count1, count2) => count1 + count2)
//      .saveAsTextFile("aaaa")
//    sparkContext.stop()
//  }
//}