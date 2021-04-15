package action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object ScalaDemo01 {

  var conf = new SparkConf().setMaster("local[*]").setAppName("ScalaDemo01")
  var sc = new SparkContext(conf)
  def main(args: Array[String]): Unit = {
    val rdd: RDD[(String, Int)] = sc.makeRDD(Array(("a", 12), ("b", 34), ("c", 53), ("d", 5), ("hell", 22)))

    //reduce方法 keyvalue全聚合
    val rdd2: (String, Int) = rdd.reduce((x, y) => {
      (x._1 + y._1, x._2 + y._2)
    })
    println(rdd2._1+"\t"+rdd2._2)

    //获取rdd个数
    println(rdd.count())

    //获取第一个元素
    println(rdd.first())

    //对数据进行排序
    val rdd3: RDD[(String, Int)] = rdd.sortBy(_._2,false,1)
    rdd3.foreach(x => println(x._1+"\t"+x._2))

    //获取返回rdd的前N个元素 ，相当于sql中limit
    println(rdd3.take(3).toBuffer)

//    println(rdd3.takeOrdered(2).toBuffer)

    val rdd5: RDD[Int] = sc.makeRDD(1 to 10,2)
    val i: Int = rdd5.aggregate(1)({(x:Int,y:Int) => x+y},{(a:Int,b:Int) => a+b})
    println(i)

    //saveAsTextFile saveAsSequenceFile saveAsObjectFile

//    rdd5.saveAsTextFile("D:\\王泽菲1 java笔记\\Spark\\sparkday01core\\src\\main\\scala\\action")
//    rdd5.saveAsObjectFile("D:\\王泽菲1 java笔记\\Spark\\sparkday01core\\src\\main\\scala\\action\\out1")
    rdd3.saveAsSequenceFile("hdfs://192.168.137.61:8020/squfile")
    sc.stop()
  }
}
