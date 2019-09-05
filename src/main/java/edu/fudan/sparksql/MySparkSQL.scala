//package edu.fudan.sparksql
//
//import org.apache.spark.sql.SparkSession
//
//object MySparkSQL {
//  def main(args: Array[String]): Unit = {
//    val spark = SparkSession
//      .builder()
//      .appName("Spark SQL basic example")
//      .master("local")
//      .getOrCreate()
//
//    // For implicit conversions from RDDs to DataFrames
//    import spark.implicits._
//
//    // 表1
//    // Create an RDD of Person objects from a text file, convert it to a Dataframe
//    val peopleDF = spark.sparkContext
//      .textFile("people.txt")
//      .map(_.split(","))
//      .map(attributes => Person(attributes(0), attributes(1), attributes(2).trim.toInt))
//      .toDF()
//    // Register the DataFrame as a temporary view
//    peopleDF.createOrReplaceTempView("people")
//
//    // 表2
//    val ordersDF = spark.sparkContext
//      .textFile("orders.txt")
//      .map(_.split(","))
//      .map(attributes => Orders(attributes(0), attributes(1)))
//      .toDF()
//    ordersDF.createOrReplaceTempView("orders")
//
//    // 表1 join 表2
//    val df = spark.sql("SELECT * FROM people t1 JOIN orders t2 ON t1.id = t2.id")
//    df.show
//  }
//}
