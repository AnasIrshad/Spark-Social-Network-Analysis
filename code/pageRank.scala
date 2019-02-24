import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import java.util.Calendar
import java.io._

val t0 = System.nanoTime()
val pw = new PrintWriter(new File("/home/anas/Hadoop/Spark/network-analysis/results/pageRank_result.txt"))



// val graph = GraphLoader.edgeListFile(sc, "/home/anas/Hadoop/Spark/network-analysis/dataset/page-rank-yt-data.txt")
val graph = GraphLoader.edgeListFile(sc, "/home/anas/Hadoop/Spark/network-analysis/dataset/triangle-count-fb-data.txt")

val vertexCount = graph.numVertices

val vertices = graph.vertices

pw.write("\nvertices: "+vertices.count().toString)
////scala.tools.nsc.io.File("/home/anas/Hadoop/Spark/network-analysis/pageRank_result.txt").appendAll(c.toString)

val edgeCount = graph.numEdges

val edges = graph.edges
pw.write("\nedges: "+edges.count().toString)


val triplets = graph.triplets
pw.write("\nno of triplets: "+triplets.count().toString)
triplets.take(5)

val inDegrees = graph.inDegrees
inDegrees.collect()

val outDegrees = graph.outDegrees
outDegrees.collect()

val degrees = graph.degrees
degrees.collect()


val staticPageRank = graph.staticPageRank(10)
staticPageRank.vertices.collect()
pw.write("\n\nStatic Page Rank: \n"+staticPageRank.vertices.top(10).mkString("\n"))

Calendar.getInstance().getTime()
val pageRank = graph.pageRank(0.001).vertices
Calendar.getInstance().getTime()

// Print top 20 items from the results
println(pageRank.top(20).mkString("\n"))
pw.write("\n\nDynamic Page Rank: \n"+pageRank.top(10).mkString("\n"))


val t1 = System.nanoTime()

println("Elapsed time: " + (t1 - t0) + "ns")
pw.write("\n\nElapsed time: \n"+(t1 - t0).toString + "ns")

pw.close
