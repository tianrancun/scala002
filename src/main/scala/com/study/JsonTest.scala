package com.study

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.DStream

import scala.util.parsing.json.JSON

object JsonTest {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[2]").setAppName("NetworkWordCount")
    val ssc = new StreamingContext(conf, Seconds(5))

    val lines:DStream[String]  = ssc.socketTextStream("127.0.0.1", 9000)//.textFileStream("E:/study/data/scala_data.txt").map(_.toUpperCase())

    val words:DStream[String]=lines.flatMap(_.split(" "))
   /* val f1 =json.filter(x =>{
      try {
        val jsonParser = new JSONParser()
        val parse: AnyRef = jsonParser.parse(x.toString)
         
        if(parse.toString.contains("_corrupt_record")){
          false
        }else{
          true
        }
      }catch {
        case _ =>false
      }
    })*/

    words.foreachRDD(rdd=>rdd.foreach(v=>{
      println(v)
      println("=======")
      val json= JSON.parseFull(v)
      val first = regJson(json)
      if(!first.get("mertic").isEmpty){
        val sec = regJson(first.get("mertic"))
        val traceId:String =sec.get("traceId").get.asInstanceOf[String];
        val time :Int = sec.get("rsp").asInstanceOf[Int];
         println("traceId: "+traceId)
         println("time: "+time)

      }else if(!first.get("traceId").isEmpty){
        println(first.get("traceId"))
      }else{
        println("no mathc")
      }

    }))
    //    val json = JSON.parseFull(l)
//    words.print()
    ssc.start() //开始计算
    ssc.awaitTermination()  //等待计算结束

    def regJson(json:Option[Any]) = json match {
      case Some(map: Map[String, Any]) => map
      //      case None => "erro"
      //      case other => "Unknow data structure : " + other
    }

//    val json = JSON.parseFull("{\"mertic\":{\"traceId\":\"123\",\"rsp\":10}}") match {
//      case Some(map:Map[String,Any])=>map
//    }
//
//    var sourceStream :DStream[(String,String)] = lines.map(rdd=>{
//
//      ("aa","aa")
//    })
  }
}
