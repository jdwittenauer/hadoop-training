import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.util.IntParam
import org.apache.spark.graphx._
import org.apache.spark.graphx.util.GraphGenerators

case class Flight(dofM:String, dofW:String, carrier:String, tailnum:String, flnum:Int, org_id:Long, origin:String, dest_id:Long, dest:String, crsdeptime:Double, deptime:Double, depdelaymins:Double, crsarrtime:Double, arrtime:Double, arrdelay:Double, crselapsedtime:Double, dist:Int)
 
def parseFlight(str: String): Flight = {
  val line = str.split(",")
  Flight(line(0), line(1), line(2), line(3), line(4).toInt, line(5).toLong, line(6), line(7).toLong, line(8), line(9).toDouble, line(10).toDouble, line(11).toDouble, line(12).toDouble, line(13).toDouble, line(14).toDouble, line(15).toDouble, line(16).toInt)
}

val textRDD = sc.textFile("/user/user01/data/rita2014jan.csv")
val flightsRDD = textRDD.map(parseFlight).cache()
val airports = flightsRDD.map(flight => (flight.org_id, flight.origin)).distinct()
airports.take(1)
// Array((14057,PDX))

val nowhere = "nowhere"
val routes = flightsRDD.map(flight => ((flight.org_id, flight.dest_id), flight.dist)).distinct()
routes.cache()
routes.take(1)
// Array[((Long, Long), Int)] = Array(((10299,10926),160))

val airportMap = airports.map { case ((org_id), name) => (org_id -> name) }.collect().toList().toMap()
val edges = routes.map { case ((org_id, dest_id), distance) => Edge(org_id.toLong, dest_id.toLong, distance) }
edges.take(1)
// Array(Edge(10299,10926,160))

val graph = Graph(airports, edges, nowhere) 
val numairports = graph.numVertices

// Routes > 1000 miles distance
graph.edges.filter { case ( Edge(org_id, dest_id,distance)) => distance > 1000}.take(3)
// Array(Edge(10140,10397,1269), Edge(10140,10821,1670), Edge(10140,12264,1628))

// Number of routes
val numroutes = graph.numEdges()
// numroutes: Long = 4090

// The EdgeTriplet class extends the Edge class by adding the srcAttr and dstAttr members which contain the source and destination properties respectively
graph.triplets.take(3).foreach(println)
// ((10135,ABE),(10397,ATL),692)
// ((10135,ABE),(13930,ORD),654)
// ((10140,ABQ),(10397,ATL),1269)

// Sort and print out the longest distance routes
graph.triplets.sortBy(_.attr, ascending=false).map(triplet => "Distance " + triplet.attr.toString + " from " + triplet.srcAttr + " to " + triplet.dstAttr).take(10).foreach(println)
// Distance 4983 from JFK to HNL
// Distance 4983 from HNL to JFK
// Distance 4963 from EWR to HNL
// Distance 4963 from HNL to EWR
// Distance 4817 from HNL to IAD
// Distance 4817 from IAD to HNL
// Distance 4502 from ATL to HNL
// Distance 4502 from HNL to ATL
// Distance 4243 from HNL to ORD
// Distance 4243 from ORD to HNL

// Define a reduce operation to compute the highest degree vertex
def max(a: (VertexId, Int), b: (VertexId, Int)): (VertexId, Int) = {
  if (a._2 > b._2) a else b
}

// Compute the max degrees
val maxInDegree: (VertexId, Int) = graph.inDegrees.reduce(max)
val maxOutDegree: (VertexId, Int) = graph.outDegrees.reduce(max)
val maxDegrees: (VertexId, Int) = graph.degrees.reduce(max)
airportMap(10397)
// String = ATL

// We can compute the in-degree of each vertex (defined in GraphOps)
graph.inDegrees.collect.sortWith(_._2 > _._2).map(x => (airportMap(x._1), x._2))
val maxIncoming = graph.inDegrees.collect.sortWith(_._2 > _._2).map(x => (airportMap(x._1), x._2)).take(3)
maxIncoming.foreach(println)
// (ATL,152)
// (ORD,145)
// (DFW,143)

// Which airport has the most outgoing flights?
val maxout = graph.outDegrees.join(airports).sortBy(_._2._1, ascending=false).take(3)
maxout.foreach(println)
// (10397,(153,ATL))
// (13930,(146,ORD))
// (11298,(143,DFW))

// What are the most important airports according to PageRank?
val ranks = graph.pageRank(0.1).vertices()
val impAirports = ranks.join(airports).sortBy(_._2._1, false).map(_._2._2)
impAirports.take(4)
// Array(ATL, ORD, DFW, DEN)
