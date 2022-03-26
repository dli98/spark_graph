
import org.apache.spark.graphx._

import scala.reflect.ClassTag

object HopNebr {

  def run[VD: ClassTag, ED: ClassTag](graph: Graph[VD, ED], hop: Int) = {
    // 节点编号，跳数
    type VMap = Map[VertexId, Int]

    def sendMsg(e: EdgeTriplet[VMap, _]) = {
      // 取两个集合的差集  然后将生命值减1
      val srcMap = (e.dstAttr.keySet -- e.srcAttr.keySet).map { k => k -> (e.dstAttr(k) - 1) }.toMap
      val dstMap = (e.srcAttr.keySet -- e.dstAttr.keySet).map { k => k -> (e.srcAttr(k) - 1) }.toMap
      if (srcMap.isEmpty && dstMap.isEmpty)
        Iterator.empty
      else
        Iterator((e.dstId, dstMap), (e.srcId, srcMap))
    }

    def vProg(vid: VertexId, data: VMap, update: VMap): Map[VertexId, Int] = mergeMsg(data, update)

    def mergeMsg(m1: VMap, m2: VMap): VMap = {
      //合并两个map, 求并集, 对于交集的点的处理，取m2和m2中最小的值, 取最短路径
      (m1.keySet ++ m2.keySet).map {
        k => k -> math.min(m1.getOrElse(k, Int.MaxValue), m2.getOrElse(k, Int.MaxValue))
      }.toMap
    }

    val g = graph.mapVertices((vid, _) => Map[VertexId, Int](vid -> hop))
    Pregel(g, Map[VertexId, Int](), hop, EdgeDirection.Out)(vProg, sendMsg, mergeMsg)
  }

}
