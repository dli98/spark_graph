package lib

import org.apache.spark.graphx.{EdgeTriplet, Graph}

import scala.reflect.ClassTag

object Kruskal extends Serializable {

  def run[VD: ClassTag](graph: Graph[VD, Double]): Graph[(Int, VD), (Int, Double)] = {
    // 最小的边肯定在最小生成树里面
    val smallestEdge = graph.triplets.min()(
      new Ordering[EdgeTriplet[VD, Double]] {
        override def compare(x: EdgeTriplet[VD, Double], y: EdgeTriplet[VD, Double]): Int = {
          Ordering[Double].compare(x.attr, y.attr)
        }
      }
    )

    // 使用点记录来去重，避免生成环
    // 使用边记录是否在生成数
    var g = graph.mapVertices((vid, vd) => if (vid == smallestEdge.srcId || vid == smallestEdge.dstId) (1, vd) else (0, vd))
      .mapTriplets((et) => if (et.srcId == smallestEdge.srcId && et.dstId == smallestEdge.dstId) (1, et.attr) else (0, et.attr))

    type edgeType = EdgeTriplet[(Int, VD), (Int, Double)]
    (2L until g.vertices.count()).foreach(i => {

      // 依次访问已访问顶点的最小的边
      val smallestEdge = g.triplets.filter((et) => (et.srcAttr._1 + et.dstAttr._1) == 1).min()(
        new Ordering[edgeType] {
          override def compare(x: edgeType, y: edgeType): Int = {
            Ordering[Double].compare(x.attr._2, y.attr._2)
          }
        }
      )

      g = g.mapVertices((vid, vd) => (if (vid != smallestEdge.srcId && vid != smallestEdge.dstId) vd else (1, vd._2)))
        .mapTriplets(et => if (et.srcId == smallestEdge.srcId && et.dstId == smallestEdge.dstId) (1, et.attr._2) else et.attr)
    })

    g.subgraph((et) => et.attr._1 == 1)
  }

}
