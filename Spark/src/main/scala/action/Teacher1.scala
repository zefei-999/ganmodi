package action


import java.net.URL

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object Teacher1 {

  /**
   *
   *
   * 每个学科 最受欢迎额老师 前三名
   * */

  def main(args: Array[String]): Unit = {
//      if(args.length<3){println("缺少参数：inputPate、num、out".stripMargin) }
      val conf: SparkConf = new SparkConf().setAppName("Teacher").setMaster("yarn")
      val sc = new SparkContext(conf)
      val subTea: RDD[((String, String), Int)] = sc.textFile(args(0)).map(line => {
      val index: Int = line.lastIndexOf("/")
      val teacher: String = line.substring(index + 1)
      val http: String = line.substring(0, index)
      val subject: String = new URL(http).getHost.split("[.]")(0)
      ((subject, teacher), 1)
    })
    val rdd1: RDD[((String, String), Int)] = subTea.reduceByKey(_+_)
//    println(rdd1.collect().toBuffefer)
    //降维
    val sbuTeaSum: RDD[(String, (String, Int))] = rdd1.map {
      case ((subject, teacher), sum) => (subject, (teacher, sum))
    }
    val groupByKey: RDD[(String, Iterable[(String, Int)])] = sbuTeaSum.groupByKey()
    val res: RDD[(String, List[(String, Int)])] = groupByKey.mapValues(_.toList.sortBy(_._2).reverse.take(args(1).toInt))

    res.saveAsTextFile(args(2))
    sc.stop()
  }
}
