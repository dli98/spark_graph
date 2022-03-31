package lib

import org.apache.spark.graphx.{Graph, VertexId}

import scala.reflect.ClassTag

object DijkstrasTrace {

  def run[VD: ClassTag](graph: Graph[VD, Double], origin: VertexId): Graph[(VD, Double, List[VertexId]), Double] = {
    var g = graph.mapVertices((vid, vd) => (false, if (origin == vid) 0 else Double.MaxValue, List[VertexId]()))

    (0L until g.vertices.count()).foreach(i => {
      val currentVertexId = g.vertices.filter(!_._2._1)
        .reduce((e1, e2) => if (e1._2._2 < e2._2._2) e1 else e2)._1

      val newDistances = g.aggregateMessages[(Double, List[VertexId])](
        (etc => if (etc.srcId == currentVertexId) etc.sendToDst((etc.srcAttr._2 + etc.attr, etc.srcAttr._3 :+ etc.srcId))),
        (a, b) => if (a._1 < b._1) a else b
      )

      g = g.outerJoinVertices(newDistances)((vid, vd, newSum) => {
        val newSumVal = newSum.getOrElse((Double.MaxValue, List[VertexId]()))
        val trace = if (vd._2 < newSumVal._1) vd._3 else newSumVal._2
        (vd._1 || vid == currentVertexId, math.min(vd._2, newSumVal._1), trace)
      })
    })

    graph.outerJoinVertices(g.vertices)((vid, vd, update) => {
      val updateVal = update.getOrElse(false, Double.MaxValue, List[VertexId]())
      (vd, updateVal._2, updateVal._3)
    })
  }
}
