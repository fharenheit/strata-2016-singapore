package org.strata.workshop

import org.apache.log4j.{Level, Logger}
import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.graphx.util.GraphGenerators
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object GraphXExample {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val spark = SparkSession
      .builder
      .appName("KMeans")
      .master("local")
      .getOrCreate()


    // Assume the SparkContext has already been constructed
    val sc: SparkContext = spark.sparkContext

    // Create an RDD for the vertices
    val users: RDD[(VertexId, (String, String))] =
      sc.parallelize(Array((3L, ("rxin", "student")), (7L, ("jgonzal", "postdoc")),
        (5L, ("franklin", "prof")), (2L, ("istoica", "prof"))))

    // Create an RDD for edges
    val relationships: RDD[Edge[String]] =
      sc.parallelize(Array(Edge(3L, 7L, "collab"), Edge(5L, 3L, "advisor"),
        Edge(2L, 5L, "colleague"), Edge(5L, 7L, "pi")))

    // Define a default user in case there are relationship with missing user
    val defaultUser = ("John Doe", "Missing")

    // Build the initial Graph
    val graph = Graph(users, relationships, defaultUser)

    // Count all users which are postdocs
    var count = graph.vertices.filter { case (id, (name, pos)) => pos == "postdoc"}.count
    println("count of postdoc vertics " + count)

    // Count all the edges where src > dst
    count = graph.edges.filter(e => e.srcId > e.dstId).count
    println("count is " + count)

    // An edge triplet represents an edge along with the vertex attributes of its neighboring vertices.
    graph.triplets.map(
      triplet => triplet.srcAttr._1 + " is the " + triplet.attr + " of " + triplet.dstAttr._1
    ).collect.foreach(println(_))

    // Remove franklin
    println("Subgraph")
    val subgraph = graph.subgraph(vpred = (id, attr) => attr._1 != "franklin")
    println(subgraph.vertices.collect.mkString("\n"))

    // connected components. The connected components algorithm labels each connected component of the graph with the ID of its lowest-numbered vertex.
    // For example, in a social network, connected components can approximate clusters.
    println("Connected Components")
    val cc = subgraph.connectedComponents()
    cc.vertices.collect().foreach(println(_))

    // triangle counting
    println("Triangle Counts")
    val triCounts = graph.triangleCount()
    triCounts.vertices.collect().foreach(println(_))

    // Compute the PageRank
    println("PageRank")
    val pagerankGraph = graph.pageRank(0.001)
    pagerankGraph.vertices.collect().foreach(println(_))


    // The reverse operator returns a new graph with all the edge directions reversed.
    println("Reverse")
    val reverse = graph.reverse
    reverse.triplets.map(
      triplet => triplet.toString()
    ).collect.foreach(println(_))

    // pregel
    pregel(sc)
  }

  def pregel(sc : SparkContext) {

    // Create an RDD for the vertices
    val users: RDD[(VertexId, (String, Double))] =
    sc.parallelize(Array((3L, ("rxin", 0.0)), (7L, ("jgonzal", 0.0)),
      (5L, ("franklin", 0.0)), (2L, ("istoica", 0.0))))

    // Create an RDD for edges
    val relationships: RDD[Edge[Double]] =
    sc.parallelize(Array(Edge(3L, 7L, 5.0), Edge(5L, 3L, 3.0),
      Edge(2L, 5L, 2.0), Edge(5L, 7L, 1.0)))

    // Define a default user in case there are relationship with missing user
    val defaultUser = ("John Doe", 0.0)

    // Build the initial Graph
    val graph = Graph(users, relationships, defaultUser)


    graph.triplets.map(
      triplet => "SrcId : " + triplet.srcAttr + " Distance : " + triplet.attr + " DstId : " + triplet.dstAttr
    ).collect.foreach(println(_))

    val sourceId: VertexId = 2 // The ultimate source

    // Initialize the graph such that all vertices except the root have distance infinity.
    val initialGraph = graph.mapVertices((id, _) =>
        if (id == sourceId) 0.0 else Double.PositiveInfinity)

    // Pregel
    val sssp = initialGraph.pregel(Double.PositiveInfinity)(

      (id, dist, newDist) => math.min(dist, newDist), // Vertex Program

      triplet => {  // Send Message
        if (triplet.srcAttr + triplet.attr < triplet.dstAttr) {
          Iterator((triplet.dstId, triplet.srcAttr + triplet.attr))
        } else {
          Iterator.empty
        }
      },

      (a, b) => math.min(a, b) // Merge Message
    )

    println("Single Source Shortest Path")
    println(sssp.vertices.collect.mkString("\n"))

  }

}
