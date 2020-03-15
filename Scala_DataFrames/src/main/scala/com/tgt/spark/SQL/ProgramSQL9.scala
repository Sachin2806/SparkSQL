package com.tgt.spark.SQL

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLImplicits
import org.apache.spark.sql.execution.datasources.csv.CSVFileFormat
import org.apache.spark.rdd.RDD.rddToOrderedRDDFunctions
import org.apache.spark.rdd._

object ProgramSQL9 {
  
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
  
  // List all tables in Spark's catalog
  spark.catalog.listTables().show()
  
  // List all tables in Spark's catalog using Spark SQL
  spark.sql("show tables").show()
  
  //Select columns using SQL
  spark.sql("select id, tag from so_tags").show()
  
  // Filter by column value
  spark.sql("select * from so_tags where id = 1").show()
  
  // Count number of rows
  spark.sql("select count(*) as id_count from so_tags where id = 4").show()
  
  // SQL like
  spark.sql("select * from so_tags where tag like 'c%'").show()
  
  // SQL where with and clause
  spark.sql("select * from so_tags where tag like 'c%' and (id = 6)").show()
  
  // SQL IN clause
  spark.sql("select * from so_tags where id IN(4,6)").show()
  
  // SQL Group By
  spark.sql("select id, count(*) as count from so_tags group by id").show()
  
  // SQL Group By with having clause
  spark.sql("select id, count(*) as count from so_tags group by id having count = 4 ").show()
  
   // SQL Order by
   spark.sql("select tag, count(*) as count from so_tags group by tag order by tag").show()
  
  }  
}