import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object WordCount {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setAppName("word count").setMaster("local[*]")
    val sc = SparkContext.getOrCreate(conf)

    val lines: RDD[String] = sc.textFile("F:\\Repository\\bigdata-demo\\src\\main\\resources\\wcdata.txt")
    val tuples: Array[(String, Int)] = lines.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_-_).sortBy(_._2).collect()
    for (elem <- tuples) {
      println(elem)
    }
    sc.stop()

  }

}
