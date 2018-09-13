package cat.ebc.sparkscalaudemy

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._

object TotalSpentByCustomer {

  def parseLine(line:String) = {
    val fields = line.split(",")
    val customer = fields(0).toInt
    val expense = fields(2).toFloat
    (customer, expense)
  }

  def main(args: Array[String]) {
      
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)
    
     // Create a SparkContext using the local machine
    val sc = new SparkContext("local", "TotalSpentByCustomer")   
    
    // Load data and parse it
    val expenses = sc.textFile("../../SparkScala/customer-orders.csv").map(parseLine)
    
    // Reduce by key (customer), adding expenses
    val expensesByCustomer = expenses.reduceByKey((x,y) => x+y)
    
    // order by expense: Swap key<=>value and sortByKey
    val sortedExpensesByCustomer = expensesByCustomer.map(x=>(x._2, x._1)).sortByKey(ascending=false)
    
    //expensesByCustomer.take(5).foreach(println)

    // Print the results
    val results = sortedExpensesByCustomer.collect()
    for (r <- results) {
      val customer = r._2
      val expense = r._1
      println(s"$customer: $expense")
    }
      
  }

}