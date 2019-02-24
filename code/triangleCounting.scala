import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import java.io._

val t0 = System.nanoTime()
val pw = new PrintWriter(new File("/home/anas/Hadoop/Spark/network-analysis/results/triangleCounting_result.txt"))


val graph = GraphLoader.edgeListFile(sc,"/home/anas/Hadoop/Spark/network-analysis/dataset/triangle-count-fb-data.txt")

println("Number of vertices : " + graph.vertices.count())
pw.write("\nvertices: "+graph.vertices.count().toString)

println("Number of edges : " + graph.edges.count())
pw.write("\nedges: "+graph.edges.count().toString)

graph.vertices.foreach(v => println(v))

val tc = graph.triangleCount()

tc.vertices.collect()
//pw.write("\nvertices: \n"+tc.vertices.collect().mkString("\n"))

// top 20 items from the results
println("tc: " + tc.vertices.take(20).mkString("\n"));
pw.write("\n\nvertices: \n"+tc.vertices.take(20).mkString("\n"))


println("Triangle counts: " + graph.connectedComponents.triangleCount().vertices.top(20).mkString("\n"));
pw.write("\n\nTriangle counts: \n"+graph.connectedComponents.triangleCount().vertices.top(20).mkString("\n"))

//val sum = tc.vertices.map(a => a._2).reduce((a, b) => a + b)


val t1 = System.nanoTime()

println("Elapsed time: " + (t1 - t0) + "ns")
pw.write("\n\nElapsed time: \n"+(t1 - t0).toString + "ns")

pw.close
