package day02

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object SparkDemo01 {

  var conf = new SparkConf().setAppName("SparkDemo01").setMaster("local[*]")

  var sc = new SparkContext(conf)

  def main(args: Array[String]): Unit = {
    val rdd: RDD[Int] = sc.parallelize(1 to 20,4)
    //缩减分区
//    val rdd1: RDD[Int] = rdd.coalesce(3)

    //重新进行分区 ，会进行shuffle操作
    val rdd1: RDD[Int] = rdd.repartition(3)
    println(rdd.partitions.size)

    //查看每个分区的内部数据
    val rdd3: RDD[String] = rdd1.mapPartitionsWithIndex((x, iter) => {
      var list = List[String]()
      list.::=(x + "|" + iter.toList)
      list.iterator
    })
    println(rdd3.collect.toBuffer)



  }







}
