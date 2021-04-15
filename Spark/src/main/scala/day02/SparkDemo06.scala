package day02

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object SparkDemo06 {

  var conf = new SparkConf().setAppName("SparkDemo01").setMaster("local[*]")

  var sc = new SparkContext(conf)

  def main(args: Array[String]): Unit = {
    val rdd = sc.parallelize(List(("jack",1),("tom",5),("jack",5),("tom",6),("jack",7)))

    //相同key的值的聚合
    val rdd1: RDD[(String, Int)] = rdd.reduceByKey((x,y) => x+y)
    println(rdd1.collect().toBuffer)

    //groupByKey 对相同key进行分组
    val rdd2: RDD[(String, Iterable[Int])] = rdd.groupByKey()
    println(rdd2.collect().toBuffer)

    //计算jack出现的次数
    val rdd3: RDD[(String, Iterable[(String, Int)])] = rdd.groupBy(_._1)
    println(rdd3.collect().toBuffer)

    //计算jack出现的次数
    val rdd4: RDD[(String, Int, Int)] = rdd2.map {
      case (x, y) => {
        var count = 0
        var sum = 0
        for (ele <- y) {
          count += 1
          sum += ele
        }
        (x, sum, count)
      }
    }
    println(rdd4.collect().toBuffer)


  }
}
