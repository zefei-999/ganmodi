package day02

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object SparkDemo02 {
  var conf = new SparkConf().setAppName("SparkDemo01").setMaster("local[*]")

  var sc = new SparkContext(conf)

  def main(args: Array[String]): Unit = {
    val rdd: RDD[Int] = sc.makeRDD(Array(23,56,2,47,84),1)

    //排序
//    rdd.sortBy(x => x).foreach(println(_))

    //调用排序方法
//    sortByKeyMethod

    //调用自定义排序方法
    sortByMethod
  }

  def sortByKeyMethod ={
    val rdd1: RDD[(String, Int)] = sc.parallelize(List(("baihuaqiang", 6), ("hello", 12), ("tom", 2), ("jack", 5),("alibaba",90)))
    //使用sortByKey进行排序
    val rdd2: RDD[(String, Int)] = rdd1.sortByKey(true,1)

    println(rdd2.collect().toBuffer)

  }

  //自定义排序 ordered ordering
  def sortByMethod:Unit={
    val rdd1: RDD[(String, Int)] = sc.parallelize(List(("baihuaqiang", 6), ("hello", 12), ("tom", 2), ("jack", 5)))
//    rdd1.sortBy(x=>x._2,true,1).foreach(println(_))


    //内部类继承ordered 实现 serializable接口
//    val rdd2: RDD[(String, Int)] = rdd1.sortBy(x => new Student(x._1,x._2))
//    println(rdd2.collect().toBuffer)

    //内部类使用隐式转换实现ordering父类
    val rdd2: RDD[(String, Int)] = rdd1.sortBy(x => new Student(x._1,x._2))
    println(rdd2.collect().toBuffer)

  }

  //内部类
  class Student(var name:String,var age:Int) extends Ordered[Student] with Serializable{
    override def compare(that: Student): Int = {
      that.age -this.age
    }
  }

  //内部类隐式转换实现排序
  class Stu(var name:String,var age:Int) extends  Serializable{

  }

  //隐式转换类
  implicit  object MyStu2Order extends Ordering[Stu]{
    override def compare(x: Stu, y: Stu): Int = {
      x.age - y.age
    }
  }

  //隐式转换函数 优先级比隐式转换类高
  implicit def stu2Order = new Ordering[Stu]{

    override def compare(x: Stu, y: Stu): Int = {
      y.age - x.age
    }
  }



}
