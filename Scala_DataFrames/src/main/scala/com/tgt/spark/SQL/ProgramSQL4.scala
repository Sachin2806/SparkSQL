package com.tgt.spark.SQL

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext._
import org.apache.spark.sql.SQLContext            
//import org.apache.spark.sql._
//import org.apache.spark.sql.types._


object ProgramSQL4 {
  
  def main(args: Array[String])
  {
    
   val spark = SparkSession
                .builder()
                .appName("Spark SQL basic example")
                .config("spark.master", "local")
                .config("spark.sql.warehouse.dir", "file:///C:/Users/CSC/git/SparkSQL/Scala_DataFrames/spark-warehouse")
                .getOrCreate()
              
   val sc = spark.sparkContext
   import spark.implicits._
   
   
   val df = Seq("I am an old grumpy DataFrame!").toDF
   df.show()
  
  }
  
}