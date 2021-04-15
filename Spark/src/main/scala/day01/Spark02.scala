package day01

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark02 {

  def main(args: Array[String]): Unit = {
    //三种方式获取到RDD（弹性分布式数据集）
    val conf: SparkConf = new SparkConf().setAppName("Spark02").setMaster("local[*]")
    val sc = new SparkContext(conf)

    //第一种 通过读取文件获取RDD
    val rdd1: RDD[String] = sc.textFile("")

    //第二种 通过读取数组、集合得到RDD
    val rdd2: RDD[Int] = sc.makeRDD(Array(1,5,38,19))

    //第三种方式 通过转换 获取到RDD4
    val rdd3: RDD[Int] = rdd2.map(_*20)

    //第四种  通过SparakSql获取RDD


  }
}
