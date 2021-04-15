package action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Teacher {

  def main(args: Array[String]): Unit = {
    if(args.length<3){println("缺少参数：inputPate、num、out".stripMargin) }
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Teacher")
    val sc = new SparkContext(conf)
    val rdd: RDD[String] = sc.textFile(args(0))
    rdd.foreach(println(_))
    val rdd1: RDD[(String, Int)] = rdd.map(line => {
      val index: Int = line.lastIndexOf("/")
      val teacher: String = line.substring(index + 1)
      (teacher, 1)
    })
    val res: Array[(String, Int)] = rdd1.reduceByKey(_+_).sortBy(_._2,false).take(args(1).toInt)
    sc.makeRDD(res,1).saveAsTextFile(args(2))
    sc.stop()

  }
}
