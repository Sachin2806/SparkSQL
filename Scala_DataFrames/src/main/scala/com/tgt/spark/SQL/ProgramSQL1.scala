package com.tgt.spark.SQL

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object ProgramSQL1 {
  
  def main(args: Array[String]){
    
     
   val conf = new SparkConf()
                  .setAppName("ProgramJson2")
                  .setMaster("local")
                  
    val spark = SparkSession
               .builder()
               .appName("ProgramJson2")
               .config(conf)
               .config("spark.master", "local")
               .config("spark.sql.warehouse.dir", "file:///C:/Users/CSC/git/SparkSQL/Scala_DataFrames/spark-warehouse")
               .getOrCreate()
               
    val sc = spark.sparkContext
    import spark.implicits._
    
    val dfs = spark.read.json("C:/Users/CSC/git/SparkSQL/Scala_DataFrames/Files/employee.json")    
   
    //To see the data in the DataFrame  
    dfs.show()
    
    //To see the Structure (Schema) of the DataFrame
    dfs.printSchema()
    
    //To fetch only the required column
    dfs.select("name").show()
    
    //Finding the employees whose age is greater than 23 (age > 23).
    dfs.filter(dfs("age") > 23).show()
    
    //counting the number of employees who are of the same age.
    dfs.groupBy("age").count().show()
    
    dfs.select("age").distinct().show()
    
  }
}