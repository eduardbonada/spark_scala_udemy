package cat.ebc.sparkscalaudemy

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._

/** Count up how many of each word occurs in a book, using regular expressions and sorting the final results */
object WordCountBetterSorted {
 
  /** Our main function where the action happens */
  def main(args: Array[String]) {
   
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)
    
     // Create a SparkContext using the local machine
    val sc = new SparkContext("local", "WordCountBetterSorted")   
    
    // Load each line of my book into an RDD
    val input = sc.textFile("../../SparkScala/book.txt")
    
    // Split using a regular expression that extracts words
    val words = input.flatMap(x => x.split("\\W+"))
    
    // Normalize everything to lowercase
    val lowercaseWords = words.map(x => x.toLowerCase())

    // filter out stop-words
    val stopWords = List("to", "the", "a", "of")        
    val filteredWords = lowercaseWords.filter(!stopWords.contains(_))
    
    // slect final words to be counted and sorted
    //val finalWords = lowercaseWords
    val finalWords = filteredWords
      
    // Count of the occurrences of each word
    // countByValue does not return an RDD, but a map. When sorting a map we are not using the cluster, it is done locally. 
    // We do it the hard way to keep the RDD in the cluster and use the cluster to sort.
    val wordCounts = finalWords.map(x => (x, 1)).reduceByKey( (x,y) => x + y )
    

    // Flip (word, count) tuples to (count, word) and then sort by key (the counts)
    val wordCountsSorted = wordCounts.map( x => (x._2, x._1) ).sortByKey(ascending = false)
    
    //wordCountsSorted.foreach(println)
    
    // Print the results, flipping the (count, word) results to word: count as we go.
    for (result <- wordCountsSorted) {
      val count = result._1
      val word = result._2
      println(s"$word: $count")
    }
    
  }
  
}

