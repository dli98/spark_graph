
import lib.HopNebr
import org.apache.spark.graphx._
import org.apache.spark.sql.SparkSession


object TwoHopNebrExample {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .appName(s"${this.getClass.getSimpleName}")
      .config("spark.executor.memory", "8g")
      .config("spark.driver.memory", "4g")
      .master("local[*]")
      .getOrCreate()
    val sc = spark.sparkContext
    val graph = GraphLoader.edgeListFile(sc, "data/followers.txt")
    println("\n~~~~~~~~~ Confirm Vertices of friends ")
    //过滤得到二跳邻居 就是 value=0 的顶点
    val friends = HopNebr.run(graph, hop = 2)
    val twoHopFriends = friends.vertices
      .mapValues(_.filter(_._2 == 0).keys) //由于在第二轮迭代，源节点会将自己的邻居（非目标节点）推荐给目标节点——各个邻居就是目标节点的二跳邻居，并将邻居对应的值减为0，

    twoHopFriends.filter(x => x._2 != Set()).foreach(println(_)) //把二跳邻居集合非空的（点，{二跳邻居集合}）打印出来
    sc.stop

  }
}
