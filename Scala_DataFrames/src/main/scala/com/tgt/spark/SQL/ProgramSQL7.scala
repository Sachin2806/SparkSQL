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
               .option("header","true")
               .format("com.databricks.spark.csv")
               .option("inferSchema", "true")
               .load("C:/Users/CSC/git/SparkSQL/Scala_DataFrames/Files/question_tags_10K.csv")
               .toDF("id", "tag")
               
     dfFile.show(10)
     //To show the dataframe schema 
     dfFile.printSchema()
     
     //select columns from a dataframe
     dfFile.select("id", "tag").show(10)
   
     //filter by column value of a dataframe
     dfFile.filter("id == 4").show()
     
     //Count rows of a dataframe
     println("No of IDs with 4 : " + dfFile.filter("id == 4").count())
     
     //SQL "like" query - Single Filter
     dfFile.filter("tag like 'd%'").show()
     
     //SQL "like" query - Multiple Filter
     dfFile.filter("tag like 'c%'").filter(" id == 4 or id == 6 ").show()
     
     //SQL "IN" clause
     dfFile.filter("id IN(1,4)").show()
     
     //SQL Group By
     println("Group by tag value")
     dfFile.groupBy("tag").count().show()
     
     //SQL Group By with filter
     dfFile.groupBy("tag").count().filter(" count == 1")show()
     
     //SQL order By with filter
     dfFile.groupBy("tag").count().filter(" count == 1").orderBy("tag")show()
     
  }
  
}