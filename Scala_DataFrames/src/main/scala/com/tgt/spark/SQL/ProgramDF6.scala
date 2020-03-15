package com.tgt.spark.SQL

import org.apache.spark.sql._
import org.apache.spark.sql.SQLImplicits
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.col

object ProgramDF6 {
  
  def main(args: Array[String]){
    
  val spark = SparkSession
              .builder()
              .appName("ProgramDF5")
              .config("spark.master", "local")
              .config("spark.sql.warehouse.dir", "file:///C:/Users/CSC/git/SparkSQL/Scala_DataFrames/spark-warehouse")
              .getOrCreate()
  
  val sc = spark.sparkContext
  import spark.implicits._
  
  //Creating data Frames from tuples
  val donuts = Seq(("plain donut", 1.50), ("vanilla donut", 2.0), ("glazed donut", 2.50))
  val donutsDF = spark.createDataFrame(donuts).toDF("Donut Name", "Price")
  donutsDF.show()
  
  //Get DataFrame column names
  val columnNames = donutsDF.columns
  columnNames.foreach(name => println("Column name : " + name))
  println()
  
  //DataFrame column names and types
  val (columnNames1, columnDataTypes) = donutsDF.dtypes.unzip
  println("DataFrame column names 	: " + columnNames1.mkString(","))
  println("DataFrame column data types : " + columnDataTypes.mkString(",")) 
    
  }
  
}