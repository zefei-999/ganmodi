package day01

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark04 {

  val conf: SparkConf = new SparkConf().setAppName("Spark03").setMaster("local[*]")
  val sc = new SparkContext(conf)

  def main(args: Array[String]): Unit = {


    val rdd: RDD[String] = sc.textFile("")
    val rdd1: RDD[(String, Int,Int)] = rdd.map(x => {
      val arr: Array[String] = x.split(" ")
      //手机号
      var phone = arr(1)
      //上行流量
      var upFlow = arr(arr.length - 4)
      //下行流量
      var downFlow = arr(arr.length - 3)
      (phone, upFlow.toInt,downFlow.toInt)
    })
   rdd1.groupBy(x => x)





  }

}
