package com.study

import java.util.Scanner

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
class ScalaSparkContext(appName:String="Spark"){
  val sc:SparkContext = getSparkContext()
  def getSparkContext() = {
    val conf = new SparkConf().setAppName(appName)
    conf.setMaster("local")
    val sc = new SparkContext(conf)
    sc
  }

  def stop() = {
    println("按回车键结束：")
    val in = new Scanner(System.in)
    in.nextLine()
    sc.stop()
  }
}


object ScalaSparkContext {
  def main(args: Array[String]) {
    val sparkContext = new ScalaSparkContext()
    val sc = sparkContext.sc
    val input = sc.textFile("E:\\study\\data\\scala_data.txt").map(_.toLowerCase)
    input
      .flatMap(line => line.split("[,]"))
      .map(word => (word, 1))
      .reduceByKey((count1, count2) => count1 + count2)
      .saveAsTextFile("aaaa")
    sparkContext.stop()
  }
}
