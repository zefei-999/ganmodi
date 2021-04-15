package day01

import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, Partitioner, SparkConf, SparkContext, TaskContext}

object Spark03 {

  val conf: SparkConf = new SparkConf().setAppName("Spark03").setMaster("local[*]")
   val sc = new SparkContext(conf)

  def main(args: Array[String]): Unit = {
    //map 方法
//    mapMethod()

    //mapPartitions
//    mapPartitionsMethod()

    //调用glom
//    glomMethod()

    //mapwi
//    mapPartitionsMethod()

    //调用去重复
//    disMethod

//    partitionByMethod() 分区
    partitionByMethod()
  }

  //map方法
  def mapMethod()={
    val rdd: RDD[Int] = sc.makeRDD(List(1,2,3,4,5,6,7,8))
    val rdd1: RDD[Int] = rdd.map(_*10)
    rdd1.foreach(x=> println(x))
    //返回元组类型
    val rdd2: RDD[(String, Int)] = rdd.map(x => ("返回值：",x*10))
  }

  //mapPartitions
  def mapPartitionsMethod(): Unit ={
    //1到9 3个分区
    val rdd: RDD[Int] = sc.makeRDD(1 to 9,3)

    //迭代函数
    def doubleMethod(iterator: Iterator[Int]):Iterator[Int]={

      var list = List[Int]()
      while(iterator.hasNext){
        val cur = iterator.next()
        list.::=(cur*2)
      }
      list.iterator
    }
    val result: RDD[Int] = rdd.mapPartitions(doubleMethod)
    result.foreach(println(_))

  }

  //glom 方法
  def glomMethod(): Unit ={
    val rdd: RDD[Int] = sc.makeRDD(1 to 20,4)
    val rdd1: RDD[Array[Int]] = rdd.glom()
    rdd1.foreach(x => println(x.toBuffer))
  }

  //mapwi
  def mapwiMethod(): Unit ={
    val rdd: RDD[Int] = sc.makeRDD(1 to 20,4)
    val rdd1: RDD[String] = rdd.mapPartitionsWithIndex((x, iter) => {
      var res = List[String]()
      res.::=(x + "|" + iter.toList)
      res.iterator
    })
    rdd1.foreach(println(_))
  }

  ///去除重复
  def disMethod(): Unit ={
    val rdd: RDD[Int] = sc.makeRDD(List(1,45,78,47,25,24,25,24))
    val rdd1: RDD[Int] = rdd.distinct(3)

    rdd1.foreach(println(_))
  }

  //分区
  def partitionByMethod(): Unit ={
    val rdd: RDD[String] = sc.parallelize(List("hello", "jason", "what", "are", "you", "doing", "hi",
      "jason", "do", "you", "eat", "dinner", "hello", "jason", "do",
      "you", "have", "some", "time", "hello", "jason", "time", "do", "you", "jason", "jason"), 4)

    //第一种
    val rdd1: RDD[String] = rdd.repartition(2)

    rdd1.foreachPartition(ter => {
//      println("分区Id"+TaskContext.get.partitionId()+ter.toList)
    })

    //第二种 重设分区
    val rdd2: RDD[(String, Int)] = rdd.map(x => (x,1)).partitionBy(new HashPartitioner((4)))

    rdd2.foreachPartition(ter =>{
//      println("分区ID"+TaskContext.get.partitionId()+ter.toList)
    })

    //自定义分区
    rdd.map(x => (x,1)).partitionBy(new MyPartition(2)).foreach(ter => println("分区ID"+TaskContext.get.partitionId+ter.toString()))


  }

  //自定义分区
  class MyPartition(var num:Int) extends Partitioner{
    override def numPartitions: Int = num

    override def getPartition(key: Any): Int = {
      if(key.toString.length <= 2){
        0
      }else{
        1
      }
    }
  }
}
