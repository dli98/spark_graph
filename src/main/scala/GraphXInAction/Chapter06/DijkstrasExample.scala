package GraphXInAction.Chapter06

import lib.{Dijkstras, DijkstrasTrace}
import org.apache.spark.SparkContext
import org.apache.spark.graphx.{Edge, Graph}
import utils.{SparkUtils, Utils}

object DijkstrasExample {
  val sc: SparkContext = SparkUtils.getSpark.sparkContext

  def main(args: Array[String]): Unit = {

    val myVertices = sc.makeRDD(Array((1L, "A"), (2L, "B"), (3L, "C"),
      (4L, "D"), (5L, "E"), (6L, "F"), (7L, "G")))
    val myEdges = sc.makeRDD(Array(Edge(1L, 2L, 7.0), Edge(1L, 4L, 5.0),
      Edge(2L, 3L, 8.0), Edge(2L, 4L, 9.0), Edge(2L, 5L, 7.0),
      Edge(3L, 5L, 5.0), Edge(4L, 5L, 15.0), Edge(4L, 6L, 6.0),
      Edge(5L, 6L, 8.0), Edge(5L, 7L, 9.0), Edge(6L, 7L, 11.0)))
    val myGraph = Graph(myVertices, myEdges).cache()

    val g = Utils.time(Dijkstras.run(myGraph, 1L).vertices.map(_._2).collect)
    g.foreach(println(_))
    println("------------------")
    val g2 = Utils.time(Dijkstras.pregelRun(myGraph, 1L).vertices.map(_._2).collect)
    g2.foreach(println(_))
    println("------------------")
    DijkstrasTrace.run(myGraph, 1L).vertices.map(_._2).collect.foreach(println(_))
    println("------------------")
    DijkstrasTrace.pregelRun(myGraph, 1L).vertices.map(_._2).collect.foreach(println(_))

  }

}
