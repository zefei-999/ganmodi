package day02

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object SparkDemo07 {

  var conf = new SparkConf().setMaster("local[*]").setAppName("ScalaDemo01")
  var sc = new SparkContext(conf)

  def main(args: Array[String]): Unit = {
    val rdd: RDD[(String, Int)] = sc.makeRDD(Array(("a",90),("a",80),("b",46),("b",58),("b",29),("c",58),("c",90),("d",91),("a",76)))

    val rdd2: RDD[(String, (Int, Int))] = rdd.combineByKey(
      v => (v, 1),
      (c: (Int, Int), v) => (c._1 + v, c._2 + 1),
      (c1: (Int, Int), c2: (Int, Int)) => (c1._1 + c2._1, c1._2 + c2._2)
    )
    println(rdd2.collect().toBuffer)




  }



}
