package com.tgt.spark.SQL

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLImplicits
import org.apache.spark.sql.execution.datasources.csv.CSVFileFormat

object ProgramSQL7 {
  
  def main(args: Array[String]){
    
  val spark = SparkSession
               .builder()
               .appName("Spark SQL basic example")
               .config("spark.master", "local")
               .config("spark.sql.warehouse.dir", "file:///C:/Users/CSC/git/SparkSQL/Scala_DataFrames/spark-warehouse")
               .getOrCreate()
              
   val sc = spark.sparkContext
   import spark.implicits._
  
   // Create a DataFrame from reading a CSV file
   val dfFile = spark
               .read
               .format("com.databricks.spark.csv")
               .option("inferSchema", "true")
               .option("header","true")
               .load("C:/Users/CSC/git/SparkSQL/Scala_DataFrames/Files/question_tags_10K.csv")
               .toDF("id", "tag")
               
   //val dfs = sqlcontext.read.json("C:/Users/CSC/workspace/Scala_DataFrames/Files/employee.json")
               
   dfFile.show(10)
   
   
  }
  
}