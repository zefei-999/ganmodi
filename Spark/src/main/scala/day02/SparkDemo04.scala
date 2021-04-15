package day02

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object SparkDemo04 {

  var conf = new SparkConf().setAppName("SparkDemo01").setMaster("local[*]")

  var sc = new SparkContext(conf)

  def main(args: Array[String]): Unit = {
    val studentList = Array(
      Tuple2(1,"leo"),
      Tuple2(2,"jack"),
      Tuple2(3,"tom")
    )

    val scoreList = Array(
      Tuple2(1,100),
      Tuple2(2,90),
      Tuple2(3,80),
      Tuple2(3,81)
    )

    val rdd: RDD[(Int, String)] = sc.parallelize(studentList)
    val rdd1: RDD[(Int, Int)] = sc.parallelize(scoreList)

    //内连接
    val rdd2: RDD[(Int, (String, Int))] = rdd.join(rdd1)
    println("内连接："+rdd2.collect().toBuffer)

    //左外连接  右表多余的不要
    val rdd3: RDD[(Int, (String, Option[Int]))] = rdd.leftOuterJoin(rdd1)
    println("左外连接："+rdd3.collect().toBuffer)

    //右连接
    val rdd4: RDD[(Int, (Option[String], Int))] = rdd.rightOuterJoin(rdd1)
    println("右外连接："+rdd4.collect().toBuffer)

    //全连接
    val rdd5: RDD[(Int, (Option[String], Option[Int]))] = rdd.fullOuterJoin(rdd1)
    rdd2.collect().foreach{
      case (x,(y,z)) =>{
        println(x+"\t"+(y+"|"+z))
      }
    }
  }
}
