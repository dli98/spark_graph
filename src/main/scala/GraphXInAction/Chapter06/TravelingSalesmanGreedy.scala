package GraphXInAction.Chapter06

import GraphXInAction.Chapter06.DijkstrasExample.sc
import org.apache.spark.graphx.{Edge, EdgeTriplet, Graph, VertexId}


object TravelingSalesmanGreedy {

  def main(args: Array[String]): Unit = {
    val myVertices = sc.makeRDD(Array((1L, "A"), (2L, "B"), (3L, "C"),
      (4L, "D"), (5L, "E"), (6L, "F"), (7L, "G")))
    val myEdges = sc.makeRDD(Array(Edge(1L, 2L, 7.0), Edge(1L, 4L, 5.0),
      Edge(2L, 3L, 8.0), Edge(2L, 4L, 9.0), Edge(2L, 5L, 7.0),
      Edge(3L, 5L, 5.0), Edge(4L, 5L, 15.0), Edge(4L, 6L, 6.0),
      Edge(5L, 6L, 8.0), Edge(5L, 7L, 9.0), Edge(6L, 7L, 11.0)))
    val myGraph = Graph(myVertices, myEdges)

    // A -> D -> F -> E -> C -> B
    greedy(myGraph, 1L).outerJoinVertices(myGraph.vertices)(
      (vid, vd, data) => (data.getOrElse("None"))
    ).triplets.filter(_.attr._2).map(et => (et.srcAttr, et.dstAttr))
      .collect.foreach(println(_))
  }

  def greedy[VD](graph: Graph[VD, Double], origin: VertexId): Graph[Boolean, (Double, Boolean)] = {

    // 设置每个顶点的遍历状态
    var g = graph.mapVertices((vid, _) => vid == origin)
      // 记录旅行路径
      .mapTriplets(triplet => (triplet.attr, false))

    var nextVertexId = origin
    var edgesAreAvailable = true
    type tripletType = EdgeTriplet[Boolean, (Double, Boolean)]

    while (edgesAreAvailable) {
      // 获取当前顶点所有可用边
      val availableEdges = g.triplets.filter((triplet) => {
        !triplet.attr._2 && (triplet.srcId == nextVertexId && !triplet.dstAttr) || (triplet.dstId == nextVertexId && !triplet.srcAttr)
      })

      edgesAreAvailable = availableEdges.count() > 0
      if (edgesAreAvailable) {
        // 获取最小权重的边
        val smallestEdge = availableEdges.min()(new Ordering[tripletType] {
          override def compare(a: tripletType, b: tripletType): Int = {
            Ordering[Double].compare(a.attr._1, b.attr._1)
          }
        })

        nextVertexId = Seq(smallestEdge.srcId, smallestEdge.dstId)
          .filter(_ != nextVertexId).head

        // 记录最新状态
        g = g.mapVertices((vid, vd) => (vd || vid == nextVertexId))
          .mapTriplets(et => (et.attr._1, et.attr._2 || (et.srcId == smallestEdge.srcId && et.dstId == smallestEdge.dstId)))
      }
    }
    g
  }

}
