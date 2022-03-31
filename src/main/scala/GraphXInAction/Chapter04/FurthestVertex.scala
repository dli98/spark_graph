package GraphXInAction.Chapter04

import org.apache.spark.graphx.{Edge, Graph}
import utils.SparkUtils

object FurthestVertex {
  def main(args: Array[String]): Unit = {
    val sc = SparkUtils.getSpark.sparkContext

    val myVertices = sc.makeRDD(Array((1L, "Ann"), (2L, "Bill"),
      (3L, "Charles"), (4L, "Diane"), (5L, "Went to gym this morning")))

    val myEdges = sc.makeRDD(Array(Edge(1L, 2L, "is-friends-with"),
      Edge(2L, 3L, "is-friends-with"), Edge(3L, 4L, "is-friends-with"),
      Edge(4L, 5L, "Likes-status"), Edge(3L, 5L, "Wrote-status")))

    val myGraph = Graph(myVertices, myEdges)
    val g = myGraph.mapVertices((_, _) => 0)
    println("furthest hops", propagateEdgeCount(g).vertices.reduce((a, b) => if (a._2 > b._2) a else b)._2)

    //    val maxDepth = myGraph.mapVertices((vid, attr) => (attr, 0))
    //      .aggregateMessages[Int](et => et.sendToDst(et.srcAttr._2 + 1), max)
    //      .join(myGraph.vertices).reduce((a, b) => {
    //      if (a._2._1 > b._2._1) a else b
    //    })
    //    println(s"maxDepth: $maxDepth")

  }


  def propagateEdgeCount(g: Graph[Int, String]): Graph[Int, String] = {
    val vertices = g.aggregateMessages[Int](triplet => triplet.sendToDst(triplet.srcAttr + 1), math.max)
    val g2 = Graph(vertices, g.edges)
    val check = g2.vertices.join(g.vertices).map(x => x._2._1 - x._2._2).reduce(_ + _)
    if (check > 0)
      propagateEdgeCount(g2)
    else
      g
  }

}
