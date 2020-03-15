package com.tgt.spark.SQL

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLImplicits
import org.apache.spark.sql.execution.datasources.csv.CSVFileFormat
import org.apache.spark.rdd.RDD.rddToOrderedRDDFunctions
import org.apache.spark.rdd._

object ProgramSQL11 {
  
  def main(args: Array[String]){
    
  val spark = SparkSession
              .builder()
              .appName("ProgramSQL8")
              .config("spark.master", "local")
              .config("spark.sql.warehouse.dir", "file:///C:/Users/CSC/git/SparkSQL/Scala_DataFrames/spark-warehouse")
              .getOrCreate()
  
  val sc = spark.sparkContext
  import spark.implicits._
  
  // Create a DataFrame from reading a CSV file
  val dfTags = spark
               .read
               .option("header","true")
               .format("com.databricks.spark.csv")
               .option("inferSchema", "true")
               .load("C:/Users/CSC/git/SparkSQL/Scala_DataFrames/Files/question_tags_10K.csv")
               .toDF("id", "tag")
               
  dfTags.createOrReplaceTempView("so_tags")
  
  //Program to Demo UDF - User Defined Functions
  def prefixStackoverflow(s: String): String = s"so_$s"
  
  //Register User Defined Function (UDF)
  spark.udf.register("prefix_so", prefixStackoverflow _)
  
  //Use UDF prefix_so to augment each tag value with so_
  spark.sql("select id, prefix_so(tag) from so_tags").show()
  
  
  
  }  
}