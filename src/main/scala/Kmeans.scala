import java.io.FileWriter
import java.nio.file.{Files, Paths}
import java.util
import org.apache.spark.{SparkConf, SparkContext}
import scala.io.{BufferedSource, Source}

object Kmeans {

  /*
  配置
   */
  val conf = new SparkConf()
  conf.setAppName("k-means test")
  conf.setMaster("local[*]")
  conf.set("means.num","10")
  conf.set("defaultFS","local")
  val sc = new SparkContext(conf)

  val dataPath="src/main/resources/kmeans/user_location.txt"
  val oldMeansPath="src/main/resources/kmeans/oldmeans.txt"
  val newMeansPath="src/main/resources/kmeans/newmeans.txt"
  val trainThreshold=0.001
  var iterLoop=10

  /*
    初始化k个质心: 随机选取 `means.num`条数据作为初始质心, 并将userid替换为clusterid
  */
  def initMeans(): Unit ={
    var k=0
    val means: Array[String] = sc.textFile(this.dataPath)
      .takeSample(withReplacement = true, conf.get("means.num").toInt)
      .map(str => {
        val newStr: String = str.replaceFirst("^[0-9]*,", s"$k,")
        k += 1
        newStr
      })

    Files.deleteIfExists(Paths.get(this.oldMeansPath))
    val writer = new FileWriter(this.oldMeansPath, true)
    writer.write(String.join("\n",means:_*))
    writer.close()
  }

  /*
  一次训练迭代
   */
  def train():Unit={
    val source: BufferedSource = Source.fromFile(this.oldMeansPath)
    val means: Map[Double, Array[Double]] = source.getLines().map(_.split(",").map(_.toDouble)).map(arr => (arr(0), arr.drop(1))).toMap
    source.close()

    //计算新的质心
    val newMeans: Array[(String, util.ArrayList[Double])] = this.sc.textFile(this.dataPath)
      .map(location_str => {
        var clusterId = ""
        var minDistance: Double = Double.MaxValue
        val locations: Array[Double] = location_str.split(",").map(_.toDouble)
        for (cid <- means.keys) {
          var sum: Double = 0
          for (i <- means(cid).indices) sum += math.pow(means(cid)(i) - locations(i + 1), 2)
          if (sum < minDistance) {
            clusterId = cid.toString.split("\\.")(0)
            minDistance = sum
          }
        }
        (clusterId, locations)
      })
      .groupByKey().mapValues(iter => {
      val newMean = new util.ArrayList[Double]()
      var count = 0
      for (elem <- iter) {
        for (i <- 0 until elem.length - 1) {
          if (newMean.size() <= i)
            newMean.add(0)
          newMean.set(i, newMean.get(i) + elem(i + 1))
        }
        count += 1
      }

      for (i <- 0 until newMean.size())
        newMean.set(i, newMean.get(i) / count)
      newMean
    }).collect()

    //保存新质心
    val meanStrs: Array[String] = newMeans.map(mean => {
      val builder: StringBuilder = StringBuilder.newBuilder
      builder.append(mean._1)
      for (i <- 0.until(mean._2.size())) {
        builder.append(",")
        builder.append(mean._2.get(i))
      }
      builder.toString()
    }).sorted

    Files.deleteIfExists(Paths.get(this.newMeansPath))
    val writer = new FileWriter(this.newMeansPath, true)
    writer.write(String.join("\n",meanStrs:_*))
    writer.close()
  }

  /*
  计算新旧质心间的距离以确定是否需要继续迭代
   */
  def continueTrain():Boolean={
    val oldMeansSource: BufferedSource = Source.fromFile(this.oldMeansPath)
    val oldMeans: Map[Double, Array[Double]] = oldMeansSource.getLines().map(_.split(",").map(_.toDouble)).map(arr => (arr(0), arr.drop(1))).toMap
    oldMeansSource.close()

    val newMeansSource: BufferedSource = Source.fromFile(this.newMeansPath)
    val newMeans: Map[Double, Array[Double]] = newMeansSource.getLines().map(_.split(",").map(_.toDouble)).map(arr => (arr(0), arr.drop(1))).toMap
    newMeansSource.close()

    var distanceDelta=0.0
    for (clusterId <- oldMeans.keys) {
      val oldMean: Array[Double] = oldMeans(clusterId)
      val newMean: Array[Double] = newMeans(clusterId)
      distanceDelta+=math.sqrt(oldMean.zip(newMean).map(each => math.pow(each._1 - each._2, 2)).sum)
    }

    Files.deleteIfExists(Paths.get(this.oldMeansPath))
    Files.move(Paths.get(this.newMeansPath),Paths.get(this.oldMeansPath))

    this.iterLoop-=1
    this.iterLoop>0 && distanceDelta/oldMeans.keys.size>this.trainThreshold
  }

  def main(args: Array[String]): Unit = {
    initMeans()
    val loop: Int = this.iterLoop
    do {
      println(s"开始第 ${loop-this.iterLoop+1} 轮的训练")
      train()
    } while (
      continueTrain()
    )
    this.sc.stop()
  }
}
