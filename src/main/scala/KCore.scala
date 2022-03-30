// https://github.com/ankurdave/spark/blob/vldb/graphx/src/main/scala/org/apache/spark/graphx/lib/KCore.scala

import org.apache.spark.graphx._
import org.apache.spark.internal.Logging

import scala.reflect.ClassTag

object KCore extends Logging {

  def run[VD: ClassTag, ED: ClassTag](graph: Graph[VD, ED], kmax: Int = Int.MaxValue, kmin: Int = 1): Graph[Int, ED] = {

    // Graph[(Int, Boolean), ED] - boolean indicates whether it is active or not
    var g = graph.outerJoinVertices(graph.degrees)((vid, oldData, newData) => (newData.getOrElse(0), true)).cache
    var curK = kmin
    var vCount: Long = 1
    while (curK <= kmax && vCount > 0) {
      g = computeCurrentKCore(g, curK).cache
      vCount = g.vertices.filter { case (vid, (vd, _)) => vd >= curK }.count()
      //      val eCount = g.triplets.filter { t => t.srcAttr._1 >= curK && t.dstAttr._1 >= curK }.count()
      //      println(s"K=$curK, V=$vCount, E=$eCount")
      //      g.vertices.foreach(println(_))
      println(s"K=$curK, V=$vCount")
      curK += 1
    }
    g.mapVertices({ case (_, (k, _)) => k })
  }

  def time[R](block: => R): R = {
    val t0 = System.nanoTime()
    val result = block // call-by-name
    val t1 = System.nanoTime()
    println("Elapsed time: " + (t1 - t0) / 1000000000.0 + " seconds")
    result
  }

  def computeCurrentKCore[ED: ClassTag](graph: Graph[(Int, Boolean), ED], k: Int) = {
    logWarning(s"Computing kcore for k=$k")

    def sendMsg(et: EdgeTriplet[(Int, Boolean), ED]): Iterator[(VertexId, (Int, Boolean))] = {
      if (!et.srcAttr._2 || !et.dstAttr._2) {
        // if either vertex has already been turned off we do nothing
        Iterator.empty
      } else if (et.srcAttr._1 < k && et.dstAttr._1 < k) {
        // tell both vertices to turn off but don't need change count value
        Iterator((et.srcId, (0, false)), (et.dstId, (0, false)))
      } else if (et.srcAttr._1 < k) {
        // if src is being pruned, tell dst to subtract from vertex count but don't turn off
        Iterator((et.srcId, (0, false)), (et.dstId, (1, true)))
      } else if (et.dstAttr._1 < k) {
        // if dst is being pruned, tell src to subtract from vertex count but don't turn off
        Iterator((et.dstId, (0, false)), (et.srcId, (1, true)))
      } else {
        // no-op but keep these vertices active?
        // Iterator((et.srcId, (0, true)), (et.dstId, (0, true)))
        Iterator.empty
      }
    }

    // subtracts removed neighbors from neighbor count and tells vertex whether it was turned off or not
    def mergeMsg(m1: (Int, Boolean), m2: (Int, Boolean)): (Int, Boolean) = {
      (m1._1 + m2._1, m1._2 && m2._2)
    }

    def vProg(vid: VertexId, data: (Int, Boolean), update: (Int, Boolean)): (Int, Boolean) = {
      var newCount = data._1
      var on = data._2
      if (on) {
        on = update._2
        newCount = data._1 - update._1
        // newCount 小于0 证明 vid 已经没有边相连了
        if (newCount <= 0) {
          on = false
          newCount = k - 1
        }
      }
      (newCount, on)
    }

    // Note that initial message should have no effect
    Pregel(graph, (0, true))(vProg, sendMsg, mergeMsg)
  }
}