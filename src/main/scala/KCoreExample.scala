
import org.apache.spark.graphx.{GraphLoader, VertexId}
import org.apache.spark.sql.SparkSession

object KCoreExample {
  def main(args: Array[String]): Unit = {
    // Creates a SparkSession.
    val spark = SparkSession
      .builder
      .appName(s"${this.getClass.getSimpleName}")
      .config("spark.executor.memory", "8g")
      .config("spark.driver.memory", "4g")
      .master("local[*]")
      .getOrCreate()
    val sc = spark.sparkContext

    val friendsGraph = GraphLoader.edgeListFile(sc, "data/followers.txt")

    val degreeMax = friendsGraph.degrees.reduce(max)._2
    println(s"max degree: $degreeMax")
    // get all vertices coreness
    val kGraph = KCore.run(friendsGraph)
    val kmax: Int = kGraph.vertices.reduce(max)._2
    println(s"max kcore ${kmax}")

    println(s"v1 $kmax-core 中包含的顶点")
    // 正确结果 [('7', 3), ('6', 3), ('3', 3), ('2', 2), ('1', 2), ('4', 1), ('15', 1), ('14', 1), ('13', 1), ('12', 1), ('11', 1)]
    kGraph.vertices.collect().sortBy(_._2).reverse.foreach(println)

    println(s"kCore fast")
    KCoreFast.run(friendsGraph, kmax=kmax).vertices
      .filter(_._2 >= kmax)
      .foreach(println(_))

    println(s"outerJoinVertices version")
    KCoreV2.computeCurrentKCore(friendsGraph, k = kmax)
      .vertices.foreach(println)
  }

  // Define a reduce operation to compute the highest degree vertex
  def max(a: (VertexId, Int), b: (VertexId, Int)): (VertexId, Int) = {
    if (a._2 > b._2) a else b
  }

}