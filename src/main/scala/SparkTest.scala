import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object SparkTest {

  def main(args: Array[String]): Unit = {
    val conf=new SparkConf()
    conf.setAppName("SparkTest")
    conf.setMaster("local[*]")
    val sc = new SparkContext(conf)

    val value: RDD[(String, String)] = sc.parallelize(List(("a", "1 2"), ("b", "3 4")))

  }

}
