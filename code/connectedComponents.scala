import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import java.util.Calendar
import java.io._

val t0 = System.nanoTime()

val pw = new PrintWriter(new File("/home/anas/Hadoop/Spark/network-analysis/results/connectComponents_result.txt"))


// val graph = GraphLoader.edgeListFile(sc, "/home/anas/Hadoop/Spark/network-analysis/dataset/connected-components-lj-data.txt")
val graph = GraphLoader.edgeListFile(sc, "/home/anas/Hadoop/Spark/network-analysis/dataset/triangle-count-fb-data.txt")

Calendar.getInstance().getTime()
val cc = graph.connectedComponents()
Calendar.getInstance().getTime()

// pw.write("\nvertices: "+cc.vertices.collect().mkString("\n"))

// Print top 20 items from the results
println(cc.vertices.take(5).mkString("\n"))
pw.write("\nvertices: \n"+cc.vertices.take(20).mkString("\n"))


val t1 = System.nanoTime()

println("Elapsed time: " + (t1 - t0) + "ns")
pw.write("\n\nElapsed time: \n"+(t1 - t0).toString + "ns")

pw.close
