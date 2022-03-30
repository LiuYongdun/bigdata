import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object SparkTest {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setAppName("SparkTest")
    conf.setMaster("local[*]")
    val sc = new SparkContext(conf)

    val value: RDD[(String, Int, Int)] = sc.parallelize(List(("a", 1, 2), ("b", 2, 3), ("c", 2, 0)), 1)
    val value1: RDD[(String, Int, Int)] = value.sortBy(each => (-each._2, each._3))

    value1.foreach(each => print(each))

  }

}


case class Student(name:String,age:Int)
