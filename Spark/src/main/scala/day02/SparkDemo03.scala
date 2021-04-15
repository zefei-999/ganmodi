package day02

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object SparkDemo03 {

  var conf = new SparkConf().setAppName("SparkDemo01").setMaster("local[*]")

  var sc = new SparkContext(conf)

  def main(args: Array[String]): Unit = {
    val rdd: RDD[Int] = sc.makeRDD(1 to 5 ,1)
    val rdd1: RDD[Int] = sc.makeRDD(5 to 10,1)

    //并集
    val rdd2: RDD[Int] = rdd.union(rdd1)
    println("并集："+rdd2.collect().toBuffer)

    //左RDD的差集
    val rdd3: RDD[Int] = rdd.subtract(rdd1)
    println("左RDD差集："+rdd3.collect().toBuffer)

    //交集
    val rdd4: RDD[Int] = rdd.intersection(rdd1)
    println("交集："+rdd4.collect().toBuffer)

    //rdd每个值都和rdd1的值进行组合
    val rdd5: RDD[(Int, Int)] = rdd.cartesian(rdd1)
    println(rdd5.collect().toBuffer)
  }

}
