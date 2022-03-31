package GraphXInAction.Chapter04

import org.apache.spark.graphx.Graph
import org.apache.spark.graphx.util.GraphGenerators
import utils.SparkUtils

object GraphGeneratorExample {

  def main(args: Array[String]): Unit = {
    val sc = SparkUtils.getSpark.sparkContext

    val pw = new java.io.PrintWriter("gridGraph.gexf")
    pw.write(toGexf(GraphGenerators.gridGraph(sc, 4, 4)))
    pw.close

    // 图的出度对数正态分布
    val logNormalGraph = GraphGenerators.logNormalGraph(sc, 15)

    logNormalGraph.joinVertices(logNormalGraph.outDegrees)((vid, attr, degrees) => degrees)
      .vertices.sortBy(_._2).foreach(x => print(x._2 + ", "))

    // 社交网络, vertices 数目是最近的一个 2 的幂次方
    val rmatGraph = GraphGenerators.rmatGraph(sc, 32, 60)

  }

  def toGexf[VD, ED](g: Graph[VD, ED]) =
    "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" +
      "<gexf xmlns=\"http://www.gexf.net/1.2draft\" version=\"1.2\">\n" +
      "  <graph mode=\"static\" defaultedgetype=\"directed\">\n" +
      "    <nodes>\n" +
      g.vertices.map(v => "      <node id=\"" + v._1 + "\" label=\"" +
        v._2 + "\" />\n").collect.mkString +
      "    </nodes>\n" +
      "    <edges>\n" +
      g.edges.map(e => "      <edge source=\"" + e.srcId +
        "\" target=\"" + e.dstId + "\" label=\"" + e.attr +
        "\" />\n").collect.mkString +
      "    </edges>\n" +
      "  </graph>\n" +
      "</gexf>"


}
