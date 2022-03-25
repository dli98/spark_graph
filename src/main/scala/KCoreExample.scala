
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
    var kGraph = KCore.run(friendsGraph)
    val kmax: Int = kGraph.vertices.reduce(max)._2
    println(s"max kcore ${kmax}")

    // get kCore vertices
    val subGraph = kGraph.subgraph(vpred = (vid, k) => k >= kmax)
    println(s"v1 $kmax-core 中包含的顶点")
    subGraph.vertices.foreach(println)

    kGraph = KCoreV2.computeCurrentKCore(friendsGraph, k = kmax)
    println(s"v2 vesion $kmax-core 中包含的顶点")
    kGraph.vertices.foreach(println)
  }

  // Define a reduce operation to compute the highest degree vertex
  def max(a: (VertexId, Int), b: (VertexId, Int)): (VertexId, Int) = {
    if (a._2 > b._2) a else b
  }

}