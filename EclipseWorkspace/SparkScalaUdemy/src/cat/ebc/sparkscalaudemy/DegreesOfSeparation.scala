package cat.ebc.sparkscalaudemy

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.rdd._
import org.apache.spark.util.LongAccumulator
import org.apache.log4j._
import scala.collection.mutable.ArrayBuffer

/** Finds the degrees of separation between two Marvel comic book characters, based
 *  on co-appearances in a comic.
 */
object DegreesOfSeparation {
  
  // The characters we want to find the separation between.
  val startCharacterID = 620 //5306 SpiderMan - 620 BLOWTORCH/
  val targetCharacterID = 3031 //ADAM 3,031 (who?) - 3518 MASTER OF VENGEANCE
  
  // We make our accumulator a "global" Option so we can reference it in a mapper later.
  var hitCounter:Option[LongAccumulator] = None
  
  // Some custom data types 
  // BFSData contains an array of hero ID connections, the distance, and color.
  type BFSData = (Array[Int], Int, String)
  // A BFSNode has a heroID and the BFSData associated with it.
  type BFSNode = (Int, BFSData)
    
  /** Converts a line of raw input into a BFSNode */
  def prepareForBFS(line: String): BFSNode = {
    
    // Split up the line into fields
    val fields = line.split("\\s+")
    
    // Extract this hero ID from the first field
    val heroID = fields(0).toInt
    
    // Extract subsequent hero ID's into the connections array
    var connections: ArrayBuffer[Int] = ArrayBuffer()
    for ( connection <- 1 to (fields.length - 1)) {
      connections += fields(connection).toInt
    }
    
    // Default distance and state is 9999 and NOT_EXPLORED
    var state:String = "NOT_EXPLORED"
    var distance:Int = 9999
    
    // Unless this is the character we're starting from
    if (heroID == startCharacterID) {
      state = "NEXT"
      distance = 0
    }
    
    return (heroID, (connections.toArray, distance, state))
  }
  
  /** Create "iteration 0" of our RDD of BFSNodes */
  def createStartingRdd(sc:SparkContext): RDD[BFSNode] = {
    
    // read list of co-occurrencies from file
    val inputFile = sc.textFile("../../SparkScala/Marvel-graph.txt")

    // return am RDD with prepared data for BFS
    return inputFile.map(prepareForBFS)
  }
  
  /** Expands a BFSNode into this node and its children */
  def bfsMap(node:BFSNode): Array[BFSNode] = {
    
    // Extract data from the BFSNode
    val characterID:Int = node._1
    val data:BFSData = node._2
    
    val connections:Array[Int] = data._1
    val distance:Int = data._2
    var state:String = data._3
    
    // This is called from flatMap, so we return an array
    // of potentially many BFSNodes to add to our new RDD
    var results:ArrayBuffer[BFSNode] = ArrayBuffer()
    
    // NEXT nodes are flagged for expansion, and create new
    // NEXT nodes for each connection
    if (state == "NEXT") {
      for (connection <- connections) {
        val newCharacterID = connection
        val newDistance = distance + 1
        val newState = "NEXT"
        
        // Have we stumbled across the character we're looking for?
        // If so increment our accumulator so the driver script knows.
        if (targetCharacterID == connection) {
          if (hitCounter.isDefined) {
            hitCounter.get.add(1)
          }
        }
        
        // Create our new Next node for this connection and add it to the results
        val newEntry:BFSNode = (newCharacterID, (Array(), newDistance, newState))
        results += newEntry
      }
      
      // Mark this node as explored, indicating it has been processed already.
      state = "EXPLORED"
    }
    
    // Add the original node back in, so its connections can get merged with 
    // the Next nodes in the reducer.
    val thisEntry:BFSNode = (characterID, (connections, distance, state))
    results += thisEntry
    
    return results.toArray
  }
  
  /** Combine nodes for the same heroID, preserving the shortest length and furthest state. */
  def bfsReduce(data1:BFSData, data2:BFSData): BFSData = {
    
    // Extract data that we are combining
    val connections1:Array[Int] = data1._1
    val connections2:Array[Int] = data2._1
    val distance1:Int = data1._2
    val distance2:Int = data2._2
    val state1:String = data1._3
    val state2:String = data2._3
    
    // Default node values
    var distance:Int = 9999
    var state:String = "NOT_EXPLORED"
    var connections:ArrayBuffer[Int] = ArrayBuffer()
    
    // Preserve the connections if one of the nodes to reduce is the original one that includes connections
    if (connections1.length > 0) {
      connections ++= connections1
    }
    if (connections2.length > 0) {
      connections ++= connections2
    }
    
    // Preserve minimum distance
    if (distance1 < distance) {
      distance = distance1
    }
    if (distance2 < distance) {
      distance = distance2
    }
    
    // Preserve furthest state
    if (state1 == "NOT_EXPLORED" && (state2 == "NEXT" || state2 == "EXPLORED")) {
      state = state2
    }
    if (state1 == "NEXT" && state2 == "EXPLORED") {
      state = state2
    }
    if (state2 == "NOT_EXPLORED" && (state1 == "NEXT" || state1 == "EXPLORED")) {
      state = state1
    }
    if (state2 == "NEXT" && state1 == "EXPLORED") {
      state = state1
    }
    
    return (connections.toArray, distance, state)
  }
    
  /** Our main function where the action happens */
  def main(args: Array[String]) {
   
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)
    
     // Create a SparkContext using every core of the local machine
    val sc = new SparkContext("local[*]", "DegreesOfSeparation") 
    
    // Our accumulator, used to signal when we find the target 
    // character in our BFS traversal.
    hitCounter = Some(sc.longAccumulator("Hit Counter"))
    
    // create the inital rdd (read from file, prepare for bfs)
    var iterationRdd = createStartingRdd(sc)
    
    //iterationRdd.take(5).map(println)
    
    // Run BFS iterations
    var iteration:Int = 0
    for (iteration <- 1 to 10) {
      println("Running BFS Iteration# " + iteration)
   
      // Create new vertices as needed to process in the reduce stage.
      // If we encounter the node we're looking for as a NEXT
      // node, increment our accumulator to signal that we're done.
      val mapped = iterationRdd.flatMap(bfsMap)
      
      // Note that mapped.count() action here forces the RDD to be evaluated, and
      // that's the only reason our accumulator is actually updated.  
      println("Processing " + mapped.count() + " values.")
      
      // check hitCounter and end iterations if the target is found
      if (hitCounter.isDefined) {
        val hitCount = hitCounter.get.value
        if (hitCount > 0) {
          println("Hit the target character! From " + hitCount + " different direction(s).")
          return
        }
      }
      
      // Reducer combines data for each character ID, preserving the further
      // state and shortest path.      
      iterationRdd = mapped.reduceByKey(bfsReduce)
    }
  }
}


/*
EXAMPLE

Initial
1,(2,3),0,NEXT
2,(3,4),99999,NOT_EXPLORED
3,(4),99999,NOT_EXPLORED
4,(5),99999,NOT_EXPLORED
5,(4),99999,NOT_EXPLORED

After map of iteration 1
1,(2,3),0,EXPLORED <= from flatmap
2,(),1,NEXT <= from flatmap
3,(),1,NEXT <= from flatmap
2,(3,4),99999,NOT_EXPLORED
3,(4),99999,NOT_EXPLORED
4,(5),99999,NOT_EXPLORED
5,(4),99999,NOT_EXPLORED

After reduce of iteration 1
1,(2,3),0,EXPLORED
2,(3,4),1,NEXT <= reduced from 2,(),1,NEXT & 2,(3,4),99999,NOT_EXPLORED
3,(4),1,NEXT <= reduced from 3,(),1,NEXT & 3,(4),99999,NOT_EXPLORED
4,(5),99999,NOT_EXPLORED
5,(4),99999,NOT_EXPLORED
 */