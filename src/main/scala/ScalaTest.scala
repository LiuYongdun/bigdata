object ScalaTest {

  def main(args: Array[String]): Unit = {
    val ints: Array[Int] = Array(1, 2, 3)
    val ints1: Array[Int] = Array(10, 20, 30)

    val sum: Double = ints.zip(ints1).map(each => math.pow(each._1-each._2,2)).sum
    print(math.sqrt(sum))
  }

}
