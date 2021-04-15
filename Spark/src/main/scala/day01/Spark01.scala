package day01
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark01 {
  def main(args: Array[String]): Unit = {
    // 创建SparkConf
    var conf = new SparkConf().setAppName("Spark01").setMaster("local[*]")
    var sc = new SparkContext(conf)

    // 读取数据源。
    val rdd1: RDD[String] = sc.textFile("hdfs://192.168.137.62/pd.txt",10)
    // 1001    Apple   Red

    //对数据进行切分
    val rdd2: RDD[String] = rdd1.flatMap(x=>x.split("\t"))

//    for(ele <- rdd2){
//      println(ele)
//    }

    //对切分后的数据进行拼接
    val rdd3: RDD[(String, Int)] = rdd2.map(x => (x,10))
//    rdd3.foreach(x => println(x._1+"\t"+x._2))
    val rdd4: RDD[(String, Int)] = rdd3.reduceByKey((x,y) => x+y)

  //加入缓存
//    rdd4.checkpoint()
     val rdd5: RDD[(String, Int)] = rdd4.sortByKey()
    rdd5.foreach{
      case (x,y) => println(x+"\t"+y)
    }

    sc.stop()



  }

}
