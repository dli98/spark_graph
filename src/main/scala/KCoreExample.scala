
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

    val kmax = friendsGraph.degrees.reduce(max)
    val kGraph = KCore.run(friendsGraph, kmax = kmax._2)

    kGraph.vertices.foreach(println)
  }

  // Define a reduce operation to compute the highest degree vertex
  def max(a: (VertexId, Int), b: (VertexId, Int)): (VertexId, Int) = {
    if (a._2 > b._2) a else b
  }

}