package lib

import org.apache.spark.graphx.{EdgeTriplet, Graph, Pregel, VertexId}
import org.apache.spark.internal.Logging

import scala.math.max
import scala.reflect.ClassTag

object KCoreFast extends Logging {


  def run[VD: ClassTag, ED: ClassTag](
                                       graph: Graph[VD, ED],
                                       kmax: Int,
                                       kmin: Int = 1)
  : Graph[Int, ED] = {

    // Graph[(Int, Boolean), ED] - boolean indicates whether it is active or not
    var g = graph.outerJoinVertices(graph.degrees)((vid, oldData, newData) => newData.getOrElse(0)).cache

    var curK = kmin
    while (curK <= kmax) {
      g = computeCurrentKCore(g, curK).cache
      val testK = curK
      val vCount = g.vertices.filter { case (vid, vd) => vd >= curK }.count()
      val eCount = g.triplets.filter { t => t.srcAttr >= testK && t.dstAttr >= testK }.count()
      logWarning(s"K=$curK, V=$vCount, E=$eCount")
      curK += 1
    }
    g.mapVertices({ case (_, k) => k })
  }

  def computeCurrentKCore[ED: ClassTag](graph: Graph[Int, ED], k: Int) = {
    logWarning(s"Computing kcore for k=$k")

    def sendMsg(et: EdgeTriplet[Int, ED]): Iterator[(VertexId, Int)] = {
      if (et.srcAttr < 0 || et.dstAttr < 0) {
        // if either vertex has already been turned off we do nothing
        Iterator.empty
      } else if (et.srcAttr < k && et.dstAttr < k) {
        // tell both vertices to turn off but don't need change count value
        Iterator((et.srcId, -1), (et.dstId, -1))

      } else if (et.srcAttr < k) {
        // if src is being pruned, tell dst to subtract from vertex count
        Iterator((et.srcId, -1), (et.dstId, 1))

      } else if (et.dstAttr < k) {
        // if dst is being pruned, tell src to subtract from vertex count
        Iterator((et.dstId, -1), (et.srcId, 1))

      } else {
        Iterator.empty
      }
    }

    // subtracts removed neighbors from neighbor count and tells vertex whether it was turned off or not
    def mergeMsg(m1: Int, m2: Int): Int = {
      if (m1 < 0 || m2 < 0) {
        -1
      } else {
        m1 + m2
      }
    }

    def vProg(vid: VertexId, data: Int, update: Int): Int = {
      if (update < 0) {
        // if the vertex has turned off, keep it turned off
        -1
      } else {
        // subtract the number of neighbors that have turned off this round from
        // the count of active vertices
        // TODO(crankshaw) can we ever have the case data < update?
        max(data - update, 0)
      }
    }

    // Note that initial message should have no effect
    Pregel(graph, 0)(vProg, sendMsg, mergeMsg)
  }
}
