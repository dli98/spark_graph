package lib

import org.apache.spark.graphx.Graph
import org.apache.spark.internal.Logging

import scala.reflect.ClassTag

object KCoreV2 extends Logging {

  /*
  仅仅是计算了 coreness 等于大于k的顶点，不知道每个点的 coreness
   */
  def computeCurrentKCore[VD: ClassTag, ED: ClassTag](graph: Graph[VD, ED], k: Int) = {
    println(s"Computing kcore for k=$k")

    var g = graph.outerJoinVertices(graph.degrees) {
      (vid, vd, degree) => degree.getOrElse(0)
    }.cache()

    var lastVerticesNum: Long = g.numVertices
    var thisVerticesNum: Long = -1
    var isConverged = false
    var i = 0

    // todo：查看spark ui 整体耗时每次单调递增, 每次的task数目都递增，每次增量在哪里发生的？
    while (!isConverged) {
      // 获取 g 中 degree 大于 k 的子图
      val subGraph = g.subgraph(vpred = (vid, degree) => degree >= k).cache()

      g = subGraph.outerJoinVertices(subGraph.degrees) { (vid, vd, degree) => degree.getOrElse(0) }.cache()

      // 当前顶点数和上次迭代前的顶点数相等，证明图稳定
      thisVerticesNum = g.numVertices
      if (lastVerticesNum == thisVerticesNum) {
        isConverged = true
        logWarning("thisVerticesNum num is " + thisVerticesNum + ", iteration is " + i + ", k-core >= " + k)
      } else {
        logWarning("lastVerticesNum is " + lastVerticesNum + ", thisVerticesNum is " + thisVerticesNum + ", iteration is " + i + ", not converge")
        lastVerticesNum = thisVerticesNum
      }
      i += 1
    }
    g
  }

  //  def getCoreness[VD: ClassTag, ED: ClassTag]
  //  (graph: Graph[VD, ED],
  //   kmax: Int,
  //   kmin: Int = 1,
  //   maxIterations: Int = Int.MaxValue)
  //  : Graph[Int, ED] = {
  //    graph
  //  }
}
