package lib

import org.apache.spark.graphx.{EdgeDirection, Graph, VertexId}

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

  def pregelRun[VD: ClassTag](graph: Graph[VD, Double], origin: VertexId): Graph[(VD, Double), Double] = {

    // 初始化
    var g = graph.mapVertices((vid, _) => (if (vid == origin) 0 else Double.PositiveInfinity))

    // 当图很大的时候，pregel 每次 superstep 会迭代很多无用的节点。
    // A --5---> B --2---> D --->
    //     --        --->
    //        A指向D,9
    // eg: 第一次迭代 A -> D 的距离为9， A -> B 的距离为 5
    //     第二次迭代会迭代  B, D 假设 B->D 为 2， 那么A-D的最短距离更新，但是D为9的距离还是会继续向后迭代
    //     第三次迭代会覆盖 第二次 D 迭代的最短路径结果
    g = g.pregel[Double](initialMsg = Double.PositiveInfinity, activeDirection = EdgeDirection.Out)(
      (vid, vd, a) => math.min(vd, a),
      (et) => {
        val candidate = et.srcAttr + et.attr
        if (candidate < et.dstAttr) Iterator((et.dstId, candidate)) else Iterator.empty
      },
      (a, b) => math.min(a, b)
    )

    graph.outerJoinVertices(g.vertices)((vid, vd, dist) => (vd, dist.getOrElse(0)))
  }
}
