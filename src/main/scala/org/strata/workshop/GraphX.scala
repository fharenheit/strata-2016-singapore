package org.strata.workshop

import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.graphx.util.GraphGenerators
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/**
 * Created by jayantshekhar on 9/16/16.
 */
object GraphX {

  def main(args: Array[String]): Unit = {

    val session = SparkSession
      .builder
      .appName("KMeans")
      .master("local")
      .getOrCreate()


    // Assume the SparkContext has already been constructed
    val sc: SparkContext = session.sparkContext

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
    println("count is " + count)

    // Count all the edges where src > dst
    count = graph.edges.filter(e => e.srcId > e.dstId).count
    println("count is " + count)

    // An edge triplet represents an edge along with the vertex attributes of its neighboring vertices.
    graph.triplets.map(
      triplet => triplet.srcAttr._1 + " is the " + triplet.attr + " of " + triplet.dstAttr._1
    ).collect.foreach(println(_))

    // Remove franklin
    val subgraph = graph.subgraph(vpred = (id, attr) => attr._1 != "franklin")
    println(subgraph.vertices.collect.mkString("\n"))

    // connected components
    val cc = subgraph.connectedComponents()
    cc.vertices.collect().foreach(println(_))

    // triangle counting
    val triCounts = graph.triangleCount()
    triCounts.vertices.collect().foreach(println(_))

    // Compute the PageRank
    val pagerankGraph = graph.pageRank(0.001)
    pagerankGraph.vertices.collect().foreach(println(_))


    // The reverse operator returns a new graph with all the edge directions reversed.
    val reverse = graph.reverse
    reverse.triplets.map(
      triplet => triplet.toString()
    ).collect.foreach(println(_))

    // pregel
    pregel(sc)
  }

  def pregel(sc : SparkContext) {
    val graph: Graph[Long, Double] =
      GraphGenerators.logNormalGraph(sc, numVertices = 100).mapEdges(e => e.attr.toDouble)

    val sourceId: VertexId = 42 // The ultimate source
    // Initialize the graph such that all vertices except the root have distance infinity.
    val initialGraph = graph.mapVertices((id, _) =>
        if (id == sourceId) 0.0 else Double.PositiveInfinity)
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
    println(sssp.vertices.collect.mkString("\n"))

  }

}
