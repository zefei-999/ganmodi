package day02

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object SparkDemo05 {

  var conf = new SparkConf().setAppName("SparkDemo01").setMaster("local[*]")

  var sc = new SparkContext(conf)

  def main(args: Array[String]): Unit = {
    val idName: RDD[(Int, String)] = sc.parallelize(Array((1,"zhangsan"),(2,"lisi")))
    val idScore: RDD[(Int, Int)] = sc.parallelize(Array((1,100),(2,90)))
    val rdd: RDD[(Int, (Iterable[String], Iterable[Int]))] = idName.cogroup(idScore)
    rdd.foreach(x =>{
      println(x._1+"\t"+x._2._1.toBuffer+"\t"+x._2._2.toBuffer)
    })

    //zip拉链
    val zip: RDD[((Int, String), (Int, Int))] = idName.zip(idScore)
    println(zip.collect().toBuffer)
  }
}
