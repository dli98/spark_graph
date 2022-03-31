package GraphXInAction.Chapter04

import com.fasterxml.jackson.core.`type`.TypeReference
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.spark.graphx.{Edge, Graph}
import utils.SparkUtils

object SaveAsJson {
  def main(args: Array[String]): Unit = {
    val sc = SparkUtils.getSpark.sparkContext

    val myVertices = sc.makeRDD(Array((1L, "Ann"), (2L, "Bill"),
      (3L, "Charles"), (4L, "Diane"), (5L, "Went to gym this morning")))

    val myEdges = sc.makeRDD(Array(Edge(1L, 2L, "is-friends-with"),
      Edge(2L, 3L, "is-friends-with"), Edge(3L, 4L, "is-friends-with"),
      Edge(4L, 5L, "Likes-status"), Edge(3L, 5L, "Wrote-status")))

    val myGraph = Graph(myVertices, myEdges)

//    myGraph.vertices.map(x => {
//      val mapper = new ObjectMapper()
//      mapper.registerModule(DefaultScalaModule)
//      val writer = new java.io.StringWriter()
//      mapper.writeValue(writer, x)
//      writer.toString
//    }).coalesce(1, true).saveAsTextFile("myGraphVertices")

    // 输出文件本身不是 JSON 可以解析的，该文件的每一行内容都是有效的 JSON 格式
    myGraph.vertices.mapPartitions(vertices => {
      val mapper = new com.fasterxml.jackson.databind.ObjectMapper()
      mapper.registerModule(DefaultScalaModule)
      val writer = new java.io.StringWriter()
      vertices.map(v => {
        writer.getBuffer.setLength(0)
        mapper.writeValue(writer, v)
        writer.toString
      })
    }).coalesce(1, true).saveAsTextFile("myGraphVertices")

    myGraph.edges.mapPartitions(edges => {
      val mapper = new com.fasterxml.jackson.databind.ObjectMapper();
      mapper.registerModule(DefaultScalaModule)
      val writer = new java.io.StringWriter()
      edges.map(e => {
        writer.getBuffer.setLength(0)
        mapper.writeValue(writer, e)
        writer.toString
      })
    }).coalesce(1, true).saveAsTextFile("myGraphEdges")

    val myGraph2 = Graph(
      sc.textFile("myGraphVertices").mapPartitions(vertices => {
        val mapper = new com.fasterxml.jackson.databind.ObjectMapper()
        mapper.registerModule(DefaultScalaModule)
        vertices.map(v => {
          val r = mapper.readValue[Tuple2[Integer, String]](v,
            new TypeReference[Tuple2[Integer, String]] {})
          (r._1.toLong, r._2)
        })
      }),
      sc.textFile("myGraphEdges").mapPartitions(edges => {
        val mapper = new com.fasterxml.jackson.databind.ObjectMapper()
        mapper.registerModule(DefaultScalaModule)
        edges.map(e => mapper.readValue[Edge[String]](e,
          new TypeReference[Edge[String]] {}))
      })
    )

  }
}
