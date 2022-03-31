package lib

import org.apache.spark.graphx.{Graph, VertexId}

import scala.reflect.ClassTag

object Dijkstras extends Serializable {

  def run[VD: ClassTag](graph: Graph[VD, Double], origin: VertexId): Graph[(VD, Double), Double] = {

    // 初始化
    var g = graph.mapVertices((vid, _) => (false, if (vid == origin) 0 else Double.MaxValue))

    // 遍历所有顶点
    (0L until g.vertices.count).foreach(i => {
      // 获取最短路段作为顶点
      // filter 过滤已经遍历过的顶点
      val currentVertexId = g.vertices.filter(!_._2._1)
        .reduce((e1, e2) => if (e1._2._2 < e2._2._2) e1 else e2)._1 // 未求出最短路径的顶点的最小顶点

      val newDistances = g.aggregateMessages[Double](
        (ctx) => {
          if (ctx.srcId == currentVertexId) ctx.sendToDst(ctx.srcAttr._2 + ctx.attr)
        },
        (a, b) => math.min(a, b)
      )

      // 将 currentVertexId 遍历状态设为 true, 表示已遍历, 并更新节点的最短路径
      g = g.outerJoinVertices(newDistances)((vid, oldData, newSum) => {
        (oldData._1 || vid == currentVertexId, math.min(oldData._2, newSum.getOrElse(Double.MaxValue)))
      })
    })

    graph.outerJoinVertices(g.vertices)((vid, vd, dist) =>
      (vd, dist.getOrElse((false, Double.MaxValue))._2))
  }
}
