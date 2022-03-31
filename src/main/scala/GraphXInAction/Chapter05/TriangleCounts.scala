package GraphXInAction.Chapter05

import org.apache.spark.graphx.lib.ShortestPaths
import org.apache.spark.graphx.{Edge, Graph, GraphLoader, PartitionStrategy}
import utils.SparkUtils

object TriangleCounts {
  def main(args: Array[String]): Unit = {
    val sc = SparkUtils.getSpark.sparkContext
    // https://snap.stanford.edu/data/soc-Slashdot0811.html
    val g = GraphLoader.edgeListFile(sc, "data/soc-Slashdot0811.txt").cache

    //计算不同子图中的三角形个数
    (0 to 6).map(i => g.subgraph(vpred = (vid, _) => vid >= i * 10000 && vid < (i + 1) * 10000)
      .triangleCount.vertices.map(_._2).reduce(_ + _))
      .foreach(println(_))

    // 每个顶点到3L最短路径
    ShortestPaths.run(g ,Array(3L)).vertices.foreach(println(_))
  }
}
