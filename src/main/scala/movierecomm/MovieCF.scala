package movierecomm

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SQLContext, SparkSession, functions}

import scala.collection.immutable.HashMap

/**
 * 基于物品的协同过滤
 */
object MovieCF {

  private val dataDir = "C:\\Users\\fs1424\\Downloads\\movieRating"

  private val topK = 10

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("movieCF").setMaster("local[*]")
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()
    val sc: SparkContext = spark.sparkContext

    //读取数据
    val sourceRdd: RDD[Row] = sc
      .textFile(String.format("%s\\Netflix_Dataset_Rating.csv", this.dataDir))
      .map(line => {
        val tokens: Array[String] = line.split(",")
        if (tokens.length == 3 && !tokens(0).equals("User_ID"))
          //userId, movieId, rating
          Row(tokens(0).toInt, tokens(2).toInt, tokens(1).toInt)
        else
          Row(0, 0, 0)
    }).filter(each => each.getInt(0) != 0)
    sourceRdd.cache()

    val movieVector: RDD[Row] = sourceRdd.groupBy(each => each.getInt(1)).map(each => {
      val movieId: Int = each._1
      val iter: Iterable[Row] = each._2
      var sum: Double = 0
      var userMap: Map[Int, Int] = HashMap()

      iter.foreach(row => {
        val userId: Int = row.getInt(0)
        val rating: Int = row.getInt(2)
        userMap += (userId -> rating)
        //计算movie向量的模长
        sum += Math.pow(rating, 2)
      })
      //movieId, movie向量模长, 用户评分map
      Row(movieId, Math.sqrt(sum), userMap)
    })

    //计算电影相似度矩阵
    val sqlContext: SQLContext = spark.sqlContext
    val movieDF: DataFrame = spark.createDataFrame(movieVector,
      StructType(List(StructField("movieId", IntegerType),
        StructField("movieMag", DoubleType),
        StructField("userMap", MapType(IntegerType, IntegerType)))))
    movieDF.createTempView("t_movie")
    sqlContext.udf.register("getSimilarity",getSimilarity(_:Map[Int,Int],_:Map[Int,Int],_:Double,_:Double))
    val similarityDF: DataFrame = spark.sql(
      """
        |select
        |   t1.movieId AS movieI,
        |   t2.movieId AS movieJ,
        |   getSimilarity(t1.userMap,t2.userMap,t1.movieMag,t2.movieMag) AS sim
        |from
        |   t_movie t1 join t_movie t2 on t1.movieId!=t2.movieId
        |""".stripMargin)
    similarityDF.cache()

    //获取每个电影的相似度topK数据
    similarityDF.createTempView("t_sim")
    val mostSimilarityDF: DataFrame = spark.sql(
      """
        |select
        |   movieI,
        |   movieJ,
        |   row_number() over(partition by movieI order by sim desc) rn
        |from
        |   t_sim
        |where
        |   rn<=%s
        |""".stripMargin.format(this.topK))
    mostSimilarityDF.cache()

    //推荐
    val sourceDF: DataFrame = spark.createDataFrame(sourceRdd, StructType(List(StructField("userId", IntegerType),
      StructField("movieId", IntegerType),
      StructField("rating", IntegerType))))

    sc.stop()

  }

  def getSimilarity(userMap1:Map[Int,Int],userMap2:Map[Int,Int],magnitude1:Double,magnitude2:Double):Double={
    var sum=0
    for (elem <- userMap1)
      sum+=elem._2 * userMap2.getOrElse(elem._1, 0)
    1-sum/(magnitude1*magnitude2)
  }
}
