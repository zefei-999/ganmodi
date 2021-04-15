package action

import java.net.URL

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object Teacher2 {

  def main(args: Array[String]): Unit = {
    if(args.length<3){println("缺少参数：inputPate、num、out".stripMargin) }
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Teacher")
    val sc = new SparkContext(conf)
    val subTea: RDD[((String, String), Int)] = sc.textFile(args(0)).map(line => {
      val index: Int = line.lastIndexOf("/")
      val teacher: String = line.substring(index + 1)
      val http: String = line.substring(0, index)
      val subject: String = new URL(http).getHost.split("[.]")(0)
      ((subject, teacher), 1)
    })
    //对数据进行聚合
    val reduceByKey: RDD[((String, String), Int)] = subTea.reduceByKey(_+_)
    //对数据进行分组
    val groupBy: RDD[(String, Iterable[((String, String), Int)])] = reduceByKey.groupBy(_._1._1)
    val map: RDD[(String, Iterator[(String, Int)])] = groupBy.map(x => {
      var list = List[(String, Int)]()
      for (ele <- x._2) {
        list.::=(ele._1._2, ele._2)
      }
      (x._1, list.iterator)
    })
    val res: RDD[(String, List[(String, Int)])] = map.mapValues(x=> x.toList.sortBy(_._2).reverse.take(args(1).toInt))
    res.saveAsTextFile(args(2))
    sc.stop()
  }

}
