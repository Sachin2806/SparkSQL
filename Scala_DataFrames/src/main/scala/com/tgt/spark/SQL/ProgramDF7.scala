package com.tgt.spark.SQL

import org.apache.spark.sql._
import org.apache.spark.sql.SQLImplicits
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.col

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
               .option("multiLine","true")
               .option("inferSchema", "true")
               .load("C:/Users/CSC/git/SparkSQL/Scala_DataFrames/Files/tags_sample.json")

  val df = tagsDF.select(explode($"stackoverflow") as "stackoverflow_tags")
  df.printSchema()
  }
  
}