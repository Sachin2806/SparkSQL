package com.tgt.spark.SQL

import org.apache.spark.sql._
import org.apache.spark.sql.SQLImplicits
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.col
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.DataFrame 
import org.apache.spark.sql.functions._ 
import org.apache.spark.sql.types._

object ProgramDF7 {
  
  def main(args: Array[String]){
    
  val spark = SparkSession
              .builder()
              .appName("ProgramDF5")
              .config("spark.master", "local")
              .config("spark.sql.warehouse.dir", "file:///C:/Users/CSC/git/SparkSQL/Scala_DataFrames/spark-warehouse")
              .getOrCreate()
  
  val sc = spark.sparkContext
  import spark.implicits._
  
  // Create a DataFrame from reading a sample json file
  
  val tagsDF = spark
                .read
                .load("C:/Users/CSC/git/SparkSQL/Scala_DataFrames/Files/employee.json")
//                .load("C:/Users/CSC/git/SparkSQL/Scala_DataFrames/Files/tags_sample.json")
               
               
  //val tagsDF1 = spark.read.json("C:/Users/CSC/git/SparkSQL/Scala_DataFrames/Files/tags_sample.json")
  tagsDF.show()

  val df = tagsDF.select(explode($"stackoverflow") as "stackoverflow_tags")
  df.printSchema()
  }
  
}